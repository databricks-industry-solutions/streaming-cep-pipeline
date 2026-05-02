# Databricks notebook source
import random
from datetime import datetime, timedelta
import pytz
from pyspark.sql.types import *

# 설정
START_DATE = datetime(2026, 1, 1, 0, 0, 0)
END_DATE = datetime(2026, 1, 31, 23, 59, 0)

# 테이블 명칭
TABLE_ALARM = "cep_demo.network.s3_olt_alarm_events"
TABLE_EQUIP = "cep_demo.network.s3_device_link_topology"
TABLE_TRAFFIC = "cep_demo.network.s3_snmp_interface_traffic"
TABLE_INVENTORY = "cep_demo.network.s3_router_inventory"

# KST 시간대
kst = pytz.timezone('Asia/Seoul')

def clear_existing_data():
    """기존 데이터 삭제"""
    tables = [TABLE_ALARM, TABLE_EQUIP, TABLE_TRAFFIC, TABLE_INVENTORY]
    for tbl in tables:
        try:
            # 테이블 스키마 충돌 방지를 위해 DROP TABLE 사용 (재생성)
            spark.sql(f"DROP TABLE IF EXISTS {tbl}")
            print(f"🗑️ 기존 테이블 삭제 완료: {tbl}")
        except Exception as e:
            print(f"⚠️ 데이터 삭제 중 오류 발생 ({tbl}): {e}")

def generate_static_data():
    """2. s3_device_link_topology 데이터 생성 (일회성)"""
    print("📦 장비 연결 정보(Static) 생성 중...")

    # 목표: OLT(192.0.2.23) -> SER(198.51.100.212, 198.51.100.211) 매핑
    # 3-1.sql 로직:
    # 1. OLT -> d1 (link_type IN ('EDGE-OLT', 'AGG-OLT'))
    # 2. d1(AGG-OLT) -> d2 (EDGE-AGG) : 여기서는 단순하게 EDGE-OLT로 1차 매핑만으로 해결되도록 구성

    static_data = [
        # link_type, local_device_name, local_device_ip, remote_device_ip (OLT), local_phys_if
        ("EDGE-OLT", "Edge-RouterB-212", "198.51.100.212", "192.0.2.23", "9/1/1"),
        ("EDGE-OLT", "Edge-RouterB-211", "198.51.100.211", "192.0.2.23", "9/2/1")
    ]

    rows = []
    for link_type, lfb_nm, lfb_ip, ofb_ip, lfb_if in static_data:
        # 필요한 컬럼 외에는 NULL
        # 스키마 순서:
        # link_type, local_device_name, local_device_code, local_device_ip, ...
        # local_phys_if (7번째), remote_device_ip (10번째)

        # Row 생성 (22개 컬럼)
        row = [None] * 22
        row[0] = link_type
        row[1] = lfb_nm
        row[3] = lfb_ip
        row[6] = lfb_if
        row[9] = ofb_ip
        rows.append(tuple(row))

    schema = """
        link_type STRING, local_device_name STRING, local_device_code STRING, local_device_ip STRING,
        local_srnl_if_ip STRING, local_logic_if STRING, local_phys_if STRING,
        remote_device_name STRING, remote_device_code STRING, remote_device_ip STRING, remote_srnl_if_ip STRING,
        remote_phys_if STRING, remote_logic_if STRING, topology_base_device STRING,
        local_device_unit STRING, local_if_status STRING, remote_device_unit STRING, remote_if_status STRING,
        etl_loaded_at TIMESTAMP, sftp_received_date STRING, etl_user_id STRING, etl_loaded_at_v2 TIMESTAMP
    """

    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("append").saveAsTable(TABLE_EQUIP)
    print("✅ 장비 연결 정보 생성 완료")

    # SER router inventory — pipeline.py only emits alarms for routers in this table.
    inventory_rows = [("198.51.100.212",), ("198.51.100.211",)]
    spark.createDataFrame(inventory_rows, schema="router_ip STRING") \
        .write.mode("append").saveAsTable(TABLE_INVENTORY)
    print("✅ Router inventory 생성 완료")

def generate_history_data():
    current_time = START_DATE

    alarm_buffer = []
    traffic_buffer = []

    print("🔄 히스토리 데이터 생성 시작...")

    # diff_ratio = (last_1m - avg_3nm) / avg_3nm 가 0.2 이상이 되도록
    # 트래픽 패턴: [1000]*9 + [1200] 반복하여 10분 주기로 spike 발생
    # (모든 분에 대해 spike 만들면 평균이 같이 올라가서 diff_ratio가 0 됨)
    traffic_pattern = [1000] * 9 + [1200]

    # 타겟 장비
    targets = [
        ("198.51.100.212", "9/1/1"),
        ("198.51.100.211", "9/2/1")
    ]

    i = 0
    base_val = 1000
    current_val = base_val

    while current_time <= END_DATE:
        ts_kst = kst.localize(current_time)
        ts_utc = ts_kst.astimezone(pytz.utc).replace(tzinfo=None)

        # 1. Alarms (매분 1건, Status 1)
        # OLT_IP: 192.0.2.23, RULE: R822 (csv 샘플), STATUS: 1
        # Step 1 조건: rule_id IN ('R001', 'R822') AND alarm_status IN ('1', '2')
        # 그리고 해제(0) 로그는 없어야 함 -> 0은 생성 안 함.
        alarm_buffer.append((ts_utc, "R822", "1", "FCLT_STD", "192.0.2.23"))

        # 2. Traffic (매분 증가 패턴)
        # 10분 주기 spike: 9분 base + 1분 spike
        # diff_ratio = (current - avg) / avg >= 0.2 이 되도록 spike 폭 설정
        current_val = 1000 * (10 ** (i % 10))

        for ip, if_nm in targets:
            # tx_multicast_pkts (20번째 컬럼, 인덱스 19)
            traffic_buffer.append((ts_utc, ip, if_nm, current_val))

        current_time += timedelta(minutes=1)
        i += 1

        # Larger buffer (~10 days) keeps the generator well under driver
        # memory while cutting saveAsTable count from ~90 to ~3.
        if len(alarm_buffer) >= 14400:
            save_buffers(alarm_buffer, traffic_buffer)
            alarm_buffer = []
            traffic_buffer = []
            print(f"  Processed up to {current_time}")

    if alarm_buffer:
        save_buffers(alarm_buffer, traffic_buffer)
    print("✅ 데이터 생성 완료")

def save_buffers(alarms, traffic):
    # Alarm 저장
    # eventhub_received_at, rule_id, alarm_status, facility_code, olt_ip
    spark.createDataFrame(alarms, schema="eventhub_received_at TIMESTAMP, rule_id STRING, alarm_status STRING, facility_code STRING, olt_ip STRING") \
        .write.mode("append").saveAsTable(TABLE_ALARM)

    # Traffic 저장
    # collected_at, router_ip, if_name, tx_multicast_pkts (나머지 null)
    rows = []
    for t, ip, if_nm, val in traffic:
        row = [None] * 25
        row[0] = t   # collected_at
        row[1] = ip  # router_ip
        row[19] = val  # tx_multicast_pkts
        row[23] = if_nm  # if_name
        rows.append(tuple(row))

    schema_tf = """
        collected_at TIMESTAMP, router_ip STRING, if_index STRING, if_admin_status STRING,
        if_description STRING, rx_discard_pkts BIGINT, rx_error_pkts BIGINT, if_oper_status STRING,
        tx_discard_pkts BIGINT, tx_error_pkts BIGINT, eventhub_name STRING, eventhub_received_at STRING,
        etl_completed_at STRING, eventhub_received_date STRING, rx_broadcast_pkts BIGINT,
        rx_multicast_pkts BIGINT, rx_bytes BIGINT, rx_unicast_pkts BIGINT,
        tx_broadcast_pkts BIGINT, tx_multicast_pkts BIGINT, tx_bytes BIGINT,
        tx_unicast_pkts BIGINT, if_speed BIGINT, if_name STRING, created_at STRING
    """
    spark.createDataFrame(rows, schema=schema_tf).write.mode("append").saveAsTable(TABLE_TRAFFIC)

# 실행
clear_existing_data()
generate_static_data()
generate_history_data()


# COMMAND ----------
