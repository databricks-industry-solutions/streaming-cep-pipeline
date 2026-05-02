# Databricks notebook source
import random
from datetime import datetime, timedelta
import pytz

# 설정
START_DATE = datetime(2026, 2, 1, 0, 0, 0)
END_DATE = datetime(2026, 3, 1, 23, 59, 0)
TARGET_TABLE = "cep_demo.network.s1_router_syslog_events"

# 생성 규칙
RULES = [
    {"ip": "203.0.113.34", "host": "Edge-RouterA-034", "count": 50},
    {"ip": "203.0.113.33", "host": "Edge-RouterA-033", "count": 10},
    {"ip": "203.0.113.36", "host": "Edge-RouterA-036", "count": 2}
]

# KST 시간대
kst = pytz.timezone('Asia/Seoul')

def clear_existing_data():
    """기존 데이터 삭제"""
    try:
        spark.sql(f"DELETE FROM {TARGET_TABLE}")
        print(f"🗑️ 기존 데이터 삭제 완료: {TARGET_TABLE}")
    except Exception as e:
        print(f"⚠️ 데이터 삭제 중 오류 발생 (테이블이 없을 수 있음): {e}")

def generate_log_msg(host, ip):
    # 샘플 로그 메시지 포맷 (Nokia SR OS 표준)
    # 5288 Base DHCP-WARNING-sapDHCPLseStatePopulateErr-2005 [Lease State Population Error]:  Lease state table population error on SAP lag-13 in service 100 - Conflict with lease 198.51.100.107 on SAP lag-12
    # 랜덤값: msg_id(5288), conflict_ip, lag_id(13, 12)
    msg_id = random.randint(1000, 9999)
    conflict_ip = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
    lag_1 = random.randint(1, 100)
    lag_2 = random.randint(1, 100)

    return f"{msg_id} Base DHCP-WARNING-sapDHCPLseStatePopulateErr-2005 [Lease State Population Error]:  Lease state table population error on SAP lag-{lag_1} in service 100 - Conflict with lease {conflict_ip} on SAP lag-{lag_2}"

def generate_data():
    current_time = START_DATE
    all_data = []

    total_minutes = int((END_DATE - START_DATE).total_seconds() / 60) + 1
    print(f"총 {total_minutes}분 동안 데이터를 생성합니다...")

    # 기존 데이터 삭제
    clear_existing_data()

    while current_time <= END_DATE:

        # KST 시간을 UTC로 변환 (서버 시간대가 어디든 KST 기준 시간 사용)
        ts_kst = kst.localize(current_time)
        ts_utc = ts_kst.astimezone(pytz.utc)
        # Spark TIMESTAMP 타입은 UTC 기준이므로 naive datetime(utc) 또는 aware datetime을 넘김

        for rule in RULES:
            for _ in range(rule["count"]):
                log_msg = generate_log_msg(rule["host"], rule["ip"])
                all_data.append((
                    rule["host"],
                    rule["ip"],
                    ts_utc.replace(tzinfo=None),  # Spark 저장을 위해 naive UTC로 변환
                    log_msg
                ))

        current_time += timedelta(minutes=1)

        # 메모리 관리를 위해 1시간치씩 저장 (또는 적절한 배치)
        if len(all_data) > 100000:
            df = spark.createDataFrame(all_data, schema="router_host STRING, router_ip STRING, collected_at TIMESTAMP, syslog_message STRING")
            df.write.mode("append").saveAsTable(TARGET_TABLE)
            all_data = []
            print(f"  Processed up to {current_time}")

    # 남은 데이터 저장
    if all_data:
        df = spark.createDataFrame(all_data, schema="router_host STRING, router_ip STRING, collected_at TIMESTAMP, syslog_message STRING")
        df.write.mode("append").saveAsTable(TARGET_TABLE)
        print("모든 데이터 저장 완료")

# 실행
generate_data()


# COMMAND ----------
