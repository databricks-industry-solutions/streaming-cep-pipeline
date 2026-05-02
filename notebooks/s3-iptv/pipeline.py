# Databricks notebook source
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# 룰 파일 경로
RULE_PATH_1 = "/Volumes/cep_demo/network/rules/3-1.json"  # TODO: 수정 필요
RULE_PATH_2 = "/Volumes/cep_demo/network/rules/3-2.json"  # TODO: 수정 필요
TARGET_TABLE = "cep_demo.network.s3_results"  # TODO: 수정 필요

spark = SparkSession.builder.getOrCreate()

def get_step1_query(interval):
    return f"""
    SELECT
      a.eventhub_received_at,
      a.rule_id,
      a.alarm_status,
      a.facility_code,
      a.olt_ip,
      COALESCE(d2.local_device_name, d1.local_device_name)         AS ser_equip_nm,
      COALESCE(d2.local_device_ip,   d1.local_device_ip)           AS ser_equip_ip,
      COALESCE(d2.local_phys_if,     d1.local_phys_if)             AS ser_phys_if_nm
    FROM cep_demo.network.s3_olt_alarm_events a
    -- ① OLT → 1차 상위
    LEFT JOIN (
      SELECT
        link_type,
        local_device_name,
        local_device_ip,
        remote_device_code,
        remote_device_ip,
        local_phys_if
      FROM cep_demo.network.s3_device_link_topology  -- TODO: 수정 필요
      WHERE link_type IN ('EDGE-OLT', 'AGG-OLT')
    ) d1 ON a.olt_ip = d1.remote_device_ip
    -- ② AGG → 최종 SER
    LEFT JOIN (
      SELECT
        local_device_name,
        local_device_ip,
        remote_device_ip,
        local_phys_if
      FROM cep_demo.network.s3_device_link_topology
      WHERE link_type = 'EDGE-AGG'
    ) d2 ON d1.link_type = 'AGG-OLT'
        AND d1.local_device_ip = d2.remote_device_ip
    -- ✅ SER 장비 IP가 조직 라우터 목록에 존재해야 함
    INNER JOIN (
      SELECT DISTINCT router_ip
      FROM cep_demo.network.s3_router_inventory
    ) org ON org.router_ip = COALESCE(d2.local_device_ip, d1.local_device_ip)
    -- [조건 필터링]
    WHERE d1.remote_device_ip IS NOT NULL
      AND a.rule_id IN ('R001', 'R822')
      AND a.alarm_status IN ('1', '2')
      AND a.eventhub_received_at >= DATE_TRUNC('minute', current_timestamp()) - INTERVAL {interval + 1} MINUTE
      AND a.eventhub_received_at < DATE_TRUNC('minute', current_timestamp()) - INTERVAL {interval} MINUTE

      AND NOT EXISTS (
        SELECT 1
        FROM cep_demo.network.s3_olt_alarm_events sub
        WHERE sub.olt_ip = a.olt_ip
          AND sub.rule_id = a.rule_id
          AND sub.alarm_status = '0'
          AND sub.eventhub_received_at >= DATE_TRUNC('minute', current_timestamp()) - INTERVAL {interval} MINUTE
          AND sub.eventhub_received_at < DATE_TRUNC('minute', current_timestamp())
      )
    """

def get_step2_query(interval):
    return f"""
    WITH raw_targets AS (
      SELECT from_json(
               :targets_json,
               'array<struct<
                  ser_equip_ip:string,
                  ser_phys_if_nm:string,
                  olt_ip:string,
                  rule_id:string
                >>'
             ) AS arr
    ),
    params AS (
      SELECT
        t.ser_equip_ip,
        t.ser_phys_if_nm,
        t.olt_ip,
        t.rule_id
      FROM raw_targets
      LATERAL VIEW explode(arr) AS t
    ),
    -- 1) t - (n*4) ~ t - n (3n분간) 트래픽 평균
    avg_3nm AS (
      SELECT
        p.ser_equip_ip,
        p.ser_phys_if_nm,
        AVG(total_tx_multicast_pkts) AS avg_3nm_tx_multicast_pkts
      FROM params p
      JOIN (
        SELECT
          router_ip,
          if_name,
          SUM(tx_multicast_pkts) AS total_tx_multicast_pkts
        FROM cep_demo.network.s3_snmp_interface_traffic  -- TODO: 수정 필요
        WHERE
          -- t - (n*4) ~ t - n
          collected_at >= DATE_TRUNC('minute', current_timestamp()) - INTERVAL {interval*4} MINUTE
          AND collected_at <  DATE_TRUNC('minute', current_timestamp()) - INTERVAL {interval} MINUTE
        GROUP BY router_ip, if_name, DATE_TRUNC('minute', collected_at)
      ) t ON t.router_ip = p.ser_equip_ip
         AND t.if_name = p.ser_phys_if_nm
      GROUP BY p.ser_equip_ip, p.ser_phys_if_nm
    ),
    -- 2) t - n ~ t - (n-1) (1분간) 트래픽 평균
    last_1m AS (
      SELECT
        p.ser_equip_ip,
        p.ser_phys_if_nm,
        MAX(collected_at)                AS last_collected_at,
        AVG(total_tx_multicast_pkts)     AS last_1m_tx_multicast_pkts
      FROM params p
      JOIN (
        SELECT
          router_ip,
          if_name,
          collected_at,
          SUM(tx_multicast_pkts) AS total_tx_multicast_pkts
        FROM cep_demo.network.s3_snmp_interface_traffic  -- TODO: 수정 필요
        WHERE
          -- t - n ~ t - (n-1)
          collected_at >= DATE_TRUNC('minute', current_timestamp()) - INTERVAL {interval} MINUTE
          AND collected_at <  DATE_TRUNC('minute', current_timestamp()) - INTERVAL {interval - 1} MINUTE
        GROUP BY router_ip, if_name, collected_at
      ) t ON t.router_ip = p.ser_equip_ip
         AND t.if_name = p.ser_phys_if_nm
      GROUP BY p.ser_equip_ip, p.ser_phys_if_nm
    )
    SELECT
      l.ser_equip_ip,
      l.ser_phys_if_nm,
      l.last_collected_at,
      l.last_1m_tx_multicast_pkts,
      a.avg_3nm_tx_multicast_pkts,
      -- 증감율 계산
      (l.last_1m_tx_multicast_pkts - a.avg_3nm_tx_multicast_pkts)
      / NULLIF(a.avg_3nm_tx_multicast_pkts, 0)      AS diff_ratio
    FROM last_1m l
    JOIN avg_3nm a
      ON l.ser_equip_ip = a.ser_equip_ip
     AND l.ser_phys_if_nm = a.ser_phys_if_nm
    """

def run_batch(df, batch_id):
    import zen
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import json

    spark_local = df.sparkSession

    print(f"\n[{datetime.now(ZoneInfo('Asia/Seoul'))}] 마이크로배치 #{batch_id} 실행 중...")

    # 룰 변경 감지 함수
    def check_rule_modified(rule_path):
        import os
        from datetime import timedelta
        mod_time = os.path.getmtime(rule_path)
        mod_datetime = datetime.fromtimestamp(mod_time, ZoneInfo("Asia/Seoul"))
        now = datetime.now(ZoneInfo("Asia/Seoul"))
        if now - mod_datetime < timedelta(minutes=1):
            print(f"룰 변경이 감지되었습니다 : {rule_path} \nlast modified : {mod_datetime.strftime('%Y-%m-%d %H:%M')}")

    # 1. 룰 1 로드 (Interval 계산)
    check_rule_modified(RULE_PATH_1)
    with open(RULE_PATH_1, 'r') as f:
        content_1 = f.read()
    decision_1 = zen.ZenEngine().create_decision(content_1)
    result_1 = decision_1.evaluate({})
    interval = result_1['result']['interval']

    # 2. Step 1 실행
    step1_df = spark_local.sql(get_step1_query(interval))
    print(f"Rule 3-1 (interval {interval}분) 에 의한 알람 대상 장비 조회 완료. 발생한 데이터: {step1_df.count()}건")
    step1_df.show(truncate=False, n=10)

    if step1_df.count() == 0:
        print("✅ 지속 알람 없음")
        return

    # 3. Step 2 준비
    target_list = [row.asDict() for row in step1_df.collect()]
    for row in target_list:
        for k, v in row.items():
            if hasattr(v, 'isoformat'):
                row[k] = v.isoformat()
            elif isinstance(v, datetime):
                row[k] = v.strftime('%Y-%m-%d %H:%M:%S')

    targets_json = json.dumps(target_list)

    # 4. Step 2 실행
    step2_df = spark_local.sql(get_step2_query(interval), args={"targets_json": targets_json})
    print(f"평균 트래픽 분석 완료. 발생한 데이터: {step2_df.count()}건")
    step2_df.show(truncate=False, n=10)

    # 5. Step 3 (룰 2 평가 및 저장)
    check_rule_modified(RULE_PATH_2)
    with open(RULE_PATH_2, 'r') as f:
        content_2 = f.read()
    decision_2 = zen.ZenEngine().create_decision(content_2)

    rows = [row.asDict() for row in step2_df.collect()]
    s3_alarms = []

    for row in rows:
        for k, v in row.items():
            if hasattr(v, 'isoformat'):
                row[k] = v.isoformat()

        result = decision_2.evaluate(row)
        is_alarm = result.get("result", {}).get("is_alarm", False)

        if is_alarm:
            alarm_record = {
                "created_at": datetime.now(),
                "ser_equip_ip": row['ser_equip_ip'],
                "ser_phys_if_nm": row['ser_phys_if_nm'],
                "diff_ratio": float(row['diff_ratio']) if row['diff_ratio'] else 0.0,
                "last_1m_val": int(row['last_1m_tx_multicast_pkts']) if row['last_1m_tx_multicast_pkts'] else 0,
                "avg_3nm_val": float(row['avg_3nm_tx_multicast_pkts']) if row['avg_3nm_tx_multicast_pkts'] else 0.0
            }
            s3_alarms.append(alarm_record)

    print(f"Rule 3-2 에 의한 diff ratio 평가 완료. 발생한 알람: {len(s3_alarms)}건")

    if s3_alarms:
        print(f"💾 알람 결과를 {TARGET_TABLE} 테이블에 저장 중...")
        alarm_df = spark_local.createDataFrame(s3_alarms)
        alarm_df.write.mode("append").saveAsTable(TARGET_TABLE)
        print("✅ 저장 완료")
        alarm_df.show(truncate=False)
    else:
        print("ℹ️ 저장할 알람이 없습니다.")
    print(f"[{datetime.now(ZoneInfo('Asia/Seoul'))}] 마이크로배치 #{batch_id} 실행 완료.\n")


# 스트리밍 드라이버
driver = (
    spark.readStream.format("rate").option("rowsPerSecond", 1).load()
)

stream_query = (
    driver.writeStream
    .foreachBatch(run_batch)
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", "/Volumes/cep_demo/network/checkpoints/s3_results_driver")  # TODO: 수정 필요
    .start()
)
