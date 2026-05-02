# Databricks notebook source
import random
from datetime import datetime, timedelta
import pytz
from pyspark.sql.types import *

# 설정
START_DATE = datetime(2026, 2, 1, 0, 0, 0)
END_DATE = datetime(2026, 3, 1, 23, 59, 0)

# 테이블 명칭
TABLE_LINKDOWN = "cep_demo.network.s2_snmp_linkdown_events"
TABLE_EQUIP = "cep_demo.network.s2_device_link_topology"
TABLE_TRAFFIC = "cep_demo.network.s2_snmp_interface_traffic"
TABLE_FORECAST = "cep_demo.network.s2_traffic_forecast"

# KST 시간대
kst = pytz.timezone('Asia/Seoul')

def clear_existing_data():
    """기존 데이터 삭제"""
    tables = [TABLE_LINKDOWN, TABLE_EQUIP, TABLE_TRAFFIC, TABLE_FORECAST]
    for tbl in tables:
        try:
            spark.sql(f"DELETE FROM {tbl}")
            print(f"🗑️ 기존 데이터 삭제 완료: {tbl}")
        except Exception as e:
            print(f"⚠️ 데이터 삭제 중 오류 발생 ({tbl}): {e}")

def generate_static_data():
    """2. s2_device_link_topology 데이터 생성 (일회성)"""
    print("📦 장비 연결 정보(Static) 생성 중...")

    # 8개 결과(인터페이스별 분리)를 위해 장비명을 다르게 설정
    # 1) CORE-METRO, 203.0.113.90, 2/1/1 -> Router-A-1
    # 2) CORE-METRO, 203.0.113.90, 2/1/2 -> Router-A-2
    # 3) CORE-EDGE,  203.0.113.90, 2/2/1 -> Router-A-3
    # 4) CORE-EDGE,  203.0.113.90, 2/2/2 -> Router-A-4

    static_data = [
        ("CORE-METRO", "Router-A-1", "CODE1", "203.0.113.90", "2/1/1"),
        ("CORE-METRO", "Router-A-2", "CODE2", "203.0.113.90", "2/1/2"),
        ("CORE-EDGE", "Router-A-3", "CODE3", "203.0.113.90", "2/2/1"),
        ("CORE-EDGE", "Router-A-4", "CODE4", "203.0.113.90", "2/2/2")
    ]

    rows = []
    for link_type, equip_nm, equip_cd, ip, if_nm in static_data:
        # 나머지 컬럼은 더미 값 혹은 NULL
        rows.append((link_type, equip_nm, equip_cd, ip, None, None, if_nm, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None))

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

def gbps_to_bytes(gbps):
    return int((gbps * 1_000_000_000 * 60) / 8)

def generate_history_data():
    current_time = START_DATE

    linkdown_buffer = []
    traffic_buffer = []
    forecast_buffer = []

    print("🔄 히스토리 데이터 생성 시작...")

    while current_time <= END_DATE:
        ts_kst = kst.localize(current_time)
        ts_utc = ts_kst.astimezone(pytz.utc).replace(tzinfo=None)

        # 1. Linkdown Event (매분)
        # 1) 203.0.113.90, CORE-METRO, 000
        # 2) 203.0.113.90, CORE-EDGE, 000
        linkdown_buffer.append((ts_utc, "Router-A", "203.0.113.90", "SNMP_LINK_DOWN", "000", "CORE-METRO", "OFB-A", "CD1", ts_utc, ts_utc))
        linkdown_buffer.append((ts_utc, "Router-A", "203.0.113.90", "SNMP_LINK_DOWN", "000", "CORE-EDGE", "OFB-A", "CD1", ts_utc, ts_utc))

        # 3. Traffic Delta (매분)
        traffic_buffer.append((ts_utc, "203.0.113.90", "2/1/1", gbps_to_bytes(70)))
        traffic_buffer.append((ts_utc, "203.0.113.90", "2/1/2", gbps_to_bytes(50)))
        traffic_buffer.append((ts_utc, "203.0.113.90", "2/2/1", gbps_to_bytes(30)))
        traffic_buffer.append((ts_utc, "203.0.113.90", "2/2/2", gbps_to_bytes(50)))

        # 4. Forecast (매분)
        # Forecast는 LinkType별로 생성되므로, 4개 인터페이스에 대응하기 위해
        # CORE-METRO, CORE-EDGE 각각 생성하면 결과적으로 조인 시 1:N으로 증폭됨.
        # (쿼리가 LinkType으로 조인하므로)
        forecast_buffer.append(("203.0.113.90", "CORE-METRO", ts_utc, 50.0, 50.0, 40.0, 60.0, "realtime"))
        forecast_buffer.append(("203.0.113.90", "CORE-EDGE", ts_utc, 50.0, 50.0, 40.0, 60.0, "realtime"))

        current_time += timedelta(minutes=1)

        # Larger buffer (10 days) -> 3 saveAsTable calls instead of ~360.
        # Earlier 120-row threshold caused gen_s2 to take 100+ minutes.
        if len(linkdown_buffer) >= 28800:
            save_buffers(linkdown_buffer, traffic_buffer, forecast_buffer)
            linkdown_buffer = []
            traffic_buffer = []
            forecast_buffer = []
            print(f"  Processed up to {current_time}")

    if linkdown_buffer:
        save_buffers(linkdown_buffer, traffic_buffer, forecast_buffer)

    print("✅ 모든 데이터 생성 완료")

def save_buffers(linkdown, traffic, forecast):
    schema_ld = "event_at TIMESTAMP, router_host STRING, router_ip STRING, category_2 STRING, if_name STRING, link_type STRING, remote_device_name STRING, remote_device_code STRING, first_detect_ts TIMESTAMP, last_update_ts TIMESTAMP"
    spark.createDataFrame(linkdown, schema=schema_ld).write.mode("append").saveAsTable(TABLE_LINKDOWN)

    traffic_rows = []
    for t, ip, if_nm, bytes_val in traffic:
        traffic_rows.append((t, ip, None, None, None, None, None, None, None, None, None, None, None, None, None, None, bytes_val, None, None, None, None, None, None, if_nm, None))

    schema_tf = """
        collected_at TIMESTAMP, router_ip STRING, if_index STRING, if_admin_status STRING,
        if_description STRING, rx_discard_pkts BIGINT, rx_error_pkts BIGINT, if_oper_status STRING,
        tx_discard_pkts BIGINT, tx_error_pkts BIGINT, eventhub_name STRING, eventhub_received_at STRING,
        etl_completed_at STRING, eventhub_received_date STRING, rx_broadcast_pkts BIGINT,
        rx_multicast_pkts BIGINT, rx_bytes BIGINT, rx_unicast_pkts BIGINT,
        tx_broadcast_pkts BIGINT, tx_multicast_pkts BIGINT, tx_bytes BIGINT,
        tx_unicast_pkts BIGINT, if_speed BIGINT, if_name STRING, created_at STRING
    """
    spark.createDataFrame(traffic_rows, schema=schema_tf).write.mode("append").saveAsTable(TABLE_TRAFFIC)

    schema_fc_full = "router_ip STRING, link_type STRING, ds TIMESTAMP, y DOUBLE, yhat DOUBLE, yhat_lower DOUBLE, yhat_upper DOUBLE, phase STRING, ds_date STRING, run_ts TIMESTAMP"
    fc_rows = []
    for r_ip, l_type, ds, y, yh, y_l, y_u, ph in forecast:
        fc_rows.append((r_ip, l_type, ds, y, yh, y_l, y_u, ph, None, None))
    spark.createDataFrame(fc_rows, schema=schema_fc_full).write.mode("append").saveAsTable(TABLE_FORECAST)

clear_existing_data()
generate_static_data()
generate_history_data()


# COMMAND ----------
