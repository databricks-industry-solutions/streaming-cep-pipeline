# Databricks notebook source
# zen-engine is installed at the job level (databricks.yml libraries: pypi: zen-engine).
# For interactive notebook runs, install with: %pip install zen-engine
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# 룰 파일 경로
RULE_PATH = "/Volumes/cep_demo/network/rules/2.json"  # TODO: 수정 필요
TARGET_TABLE = "cep_demo.network.s2_results"  # TODO: 수정 필요

spark = SparkSession.builder.getOrCreate()

# Step 1: Linkdown 이벤트 조회 쿼리 (마이크로배치 시점 기준)
def get_step1_query():
    return """
    WITH clock AS (
      SELECT date_trunc('minute', current_timestamp()) - INTERVAL 5 MINUTE AS target_minute
    )
    SELECT DISTINCT
      e.router_ip AS router_ip,
      e.link_type AS link_type,
      date_format(clock.target_minute, 'yyyy-MM-dd HH:mm:ss') AS target_minute
    FROM clock
    JOIN cep_demo.network.s2_snmp_linkdown_events e  -- TODO: 수정 필요
      ON date_trunc('minute', e.event_at) = clock.target_minute
    WHERE e.link_type IN ('CORE-METRO','CORE-EDGE');
    """

# Step 2: 트래픽 분석 쿼리
def get_step2_query():
    return """
    WITH raw AS (
      SELECT from_json(
               :rows_json,
               'array<struct<
                  router_ip:string,
                  link_type:string,
                  target_minute:string
                >>'
             ) AS arr
    ),
    params AS (
      SELECT
        p.router_ip      AS filter_router_ip,
        p.link_type      AS filter_link_type,
        CAST(p.target_minute AS TIMESTAMP) + INTERVAL 4 MINUTE AS now_kst,
        CAST(p.target_minute AS TIMESTAMP) + INTERVAL 2 MINUTE AS start_4m_kst
      FROM raw
      LATERAL VIEW explode(arr) AS p
    ),
    -- 장비 / 링크 매핑
    d_norm AS (
      SELECT
        d.local_device_ip,
        d.link_type,
        d.local_device_name,
        CASE
          WHEN d.link_type = 'CORE-METRO'
            THEN REGEXP_REPLACE(d.local_phys_if, '\\.0$', '')
          ELSE d.local_phys_if
        END AS if_name_norm
      FROM cep_demo.network.s2_device_link_topology d  -- TODO: 수정 필요
      JOIN params p
        ON d.link_type = p.filter_link_type
        AND d.local_device_ip = p.filter_router_ip
      WHERE d.link_type IN ('CORE-EDGE', 'CORE-METRO')
    ),
    -- SNMP 트래픽 (최근 4분)
    base AS (
      SELECT
        t.collected_at,
        d.local_device_name,
        t.router_ip,
        t.if_name,
        COALESCE(t.rx_bytes, 0) AS rx_bytes,
        d.link_type
      FROM cep_demo.network.s2_snmp_interface_traffic t  -- TODO: 수정 필요
      JOIN d_norm d
        ON t.router_ip = d.local_device_ip
       AND (
            (d.link_type = 'CORE-METRO' AND REGEXP_REPLACE(t.if_name, '\\.0$', '') = d.if_name_norm)
         OR (d.link_type <> 'CORE-METRO' AND t.if_name = d.if_name_norm)
       )
      JOIN params p
        ON t.router_ip = p.filter_router_ip
        AND d.link_type = p.filter_link_type
      WHERE t.collected_at >= p.start_4m_kst AND t.collected_at < p.now_kst
    ),
    -- 분 단위 집계
    snmp_min AS (
      SELECT
        DATE_TRUNC('minute', collected_at) AS minute_kst,
        local_device_name,
        router_ip,
        link_type,
        SUM(rx_bytes) AS bytes_sum
      FROM base
      GROUP BY 1,2,3,4
    ),
    -- Gbps 변환
    snmp_gbps AS (
      SELECT
        minute_kst,
        local_device_name,
        router_ip,
        link_type,
        (bytes_sum * 8) / (1e9 * 60.0) AS gbps
      FROM snmp_min
    ),
    -- Forecast
    fcst_raw AS (
      SELECT
        DATE_TRUNC('minute', CAST(f.ds AS TIMESTAMP)) AS minute_kst,
        f.router_ip,
        f.link_type,
        CAST(f.yhat_lower AS DOUBLE) AS yhat_lower_gbps,
        CAST(f.yhat_upper AS DOUBLE) AS yhat_upper_gbps,
        f.phase
      FROM cep_demo.network.s2_traffic_forecast f  -- TODO: 수정 필요
      JOIN params p
        ON f.router_ip = p.filter_router_ip
        AND f.link_type = p.filter_link_type
      WHERE CAST(f.ds AS TIMESTAMP) >= p.start_4m_kst AND CAST(f.ds AS TIMESTAMP) < p.now_kst
    ),
    fcst AS (
      SELECT
        r.minute_kst,
        r.router_ip,
        r.link_type,
        d.local_device_name,
        AVG(r.yhat_lower_gbps) AS yhat_lower_gbps,
        AVG(r.yhat_upper_gbps) AS yhat_upper_gbps,
        MAX(r.phase) AS phase
      FROM fcst_raw r
      LEFT JOIN d_norm d
        ON r.router_ip = d.local_device_ip
       AND r.link_type = d.link_type
      GROUP BY 1,2,3,4
    )
    -- Anomaly 판정
    SELECT
      COALESCE(s.minute_kst, f.minute_kst) AS minute_kst,
      COALESCE(s.router_ip, f.router_ip) AS router_ip,
      COALESCE(s.local_device_name, f.local_device_name) AS local_device_name,
      COALESCE(s.link_type, f.link_type) AS link_type,
      ROUND(COALESCE(s.gbps, 0), 2) AS actual_gbps,
      COALESCE(f.yhat_lower_gbps, 0) AS yhat_lower_gbps,
      COALESCE(f.yhat_upper_gbps, 0) AS yhat_upper_gbps,
      CASE
        WHEN COALESCE(s.gbps, 0) > COALESCE(f.yhat_upper_gbps, 999999) THEN 'high_anomaly'
        WHEN COALESCE(s.gbps, 0) < COALESCE(f.yhat_lower_gbps, -999999) THEN 'low_anomaly'
        ELSE 'normal'
      END AS anomaly_flag,
      COALESCE(f.phase, 'unknown') AS phase
    FROM snmp_gbps s
    FULL OUTER JOIN fcst f
      ON s.router_ip = f.router_ip
      AND s.link_type = f.link_type
      AND s.minute_kst = f.minute_kst
      AND s.local_device_name = f.local_device_name
    ORDER BY minute_kst DESC
    LIMIT 1000
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
            print(f"룰 변경이 감지되었습니다 {rule_path}.\nlast modified : {mod_datetime.strftime('%Y-%m-%d %H:%M')}")

    # 1. 룰 로드 (Hot Reload)
    check_rule_modified(RULE_PATH)
    with open(RULE_PATH, 'r') as f:
        rule_content = f.read()
    decision = zen.ZenEngine().create_decision(rule_content)

    # 2. Step 1: Linkdown 이벤트 조회
    step1_df = spark_local.sql(get_step1_query())

    if step1_df.count() == 0:
        print("✅ Linkdown 이벤트 없음")
        return

    # 3. Step 2 준비 (파라미터 변환)
    rows_list = [row.asDict() for row in step1_df.collect()]
    rows_json = json.dumps(rows_list)
    print(f"📦 이상 탐지 대상 장비 파라미터 전달: {rows_json}")

    # 4. Step 2: 트래픽 분석 쿼리 실행
    step2_df = spark_local.sql(get_step2_query(), args={"rows_json": rows_json})
    print(f"[장비별 트래픽 집계 및 예측치 비교, Anomaly 판정] 완료. 발생한 데이터: {step2_df.count()}건")
    step2_df.show(truncate=False, n=10)

    # 5. Step 3: GoRules 평가 및 알람 수집
    anomaly_list = []
    for row in step2_df.collect():
        row_dict = row.asDict()
        # timestamp를 문자열로 변환
        for k, v in row_dict.items():
            if hasattr(v, 'isoformat'):
                row_dict[k] = v.isoformat()
        anomaly_list.append(row_dict)

    input_data = {
        "anomaly_results": anomaly_list
    }

    gorules_result = decision.evaluate(input_data)
    result_data = gorules_result.get("result", {}) if gorules_result else {}
    alarms = result_data.get("alarms", [])

    print(f"Rule 2 에 의한 평가 완료. 발생한 알람: {len(alarms)}건")

    # 6. 알람 저장
    if alarms:
        print(f"💾 알람 결과를 {TARGET_TABLE} 테이블에 저장 중...")
        alarm_df = spark_local.createDataFrame(alarms)
        final_alarm_df = alarm_df.selectExpr(
            "cast(created_at as timestamp) as created_at",
            "router_ip",
            "local_device_name",
            "cast(high_count as int) as high_count",
            "cast(low_count as int) as low_count"
        )
        final_alarm_df.write.mode("append").saveAsTable(TARGET_TABLE)
        print("✅ 저장 완료")
        final_alarm_df.show(truncate=False)
    else:
        print("ℹ️ 저장할 알람이 없습니다.")
    print(f"[{datetime.now(ZoneInfo('Asia/Seoul'))}] 마이크로배치 #{batch_id} 실행 완료.\n")


# 스트리밍 드라이버 (더미 스트림 - 1분 간격 트리거용)
driver = (
    spark.readStream.format("rate").option("rowsPerSecond", 1).load()
)

stream_query = (
    driver.writeStream
    .foreachBatch(run_batch)
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", "/Volumes/cep_demo/network/checkpoints/s2_results_driver")  # TODO: 수정 필요
    .start()
)

# Block so the Databricks Job task does not exit immediately after .start().
# Manually cancel the run to stop the stream.
stream_query.awaitTermination()
