# Databricks notebook source
# Streaming variant of S1 that loads rules from the rules_apps Volume
# (the same Volume the rule editor app writes to). Use this notebook
# to demo the rule hot-reload flow end-to-end: edit a rule in the
# app UI -> next microbatch picks it up automatically.
RULE_PATH_1 = "/Volumes/cep_demo/network/rules_apps/1-1.json"
RULE_PATH_2 = "/Volumes/cep_demo/network/rules_apps/1-2.json"
TARGET_TABLE = "cep_demo.network.s1_results"

def getCheckDataQuery():
    return f"""
    SELECT
      syslog_message,
      router_host,
      router_ip,
      collected_at
    FROM cep_demo.network.s1_router_syslog_events
    WHERE syslog_message LIKE '%sapDHCPLseStatePopulateErr%'
      AND collected_at >= date_trunc('minute', current_timestamp()) - INTERVAL 2 MINUTE
      AND collected_at < date_trunc('minute', current_timestamp())
    ORDER BY
      collected_at DESC
    """

def getLogMonitorQuery(log_pattern):
    return f"""
    WITH base AS (
      SELECT
        router_host,
        router_ip,
        collected_at AS ts_kst
      FROM cep_demo.network.s1_router_syslog_events
      WHERE syslog_message LIKE '{log_pattern}'
        AND collected_at >= date_trunc('minute', current_timestamp()) - INTERVAL 2 MINUTE
        AND collected_at < date_trunc('minute', current_timestamp())
    )
    SELECT
      DATE_TRUNC('minute', ts_kst) AS dt_kst,
      router_host,
      router_ip,
      COUNT(*) AS err_cnt
    FROM base
    GROUP BY
      DATE_TRUNC('minute', ts_kst),
      router_host,
      router_ip
    ORDER BY
      dt_kst DESC,
      err_cnt DESC;
    """

# 3) foreachBatch 내부: 외부 참조 없는 순수 함수로 구성
def run_batch(df, batch_id: int):
    # 모듈 import도 내부에서 (클로저에 모듈 객체가 들어가지 않게)
    import zen
    from datetime import datetime
    from zoneinfo import ZoneInfo

    # Spark 세션은 df.sparkSession로만 접근
    spark_local = df.sparkSession

    print(f"\n[{datetime.now(ZoneInfo('Asia/Seoul'))}] 마이크로배치 #{batch_id} 실행 중...")
    print("Input 데이터 확인 중...")
    df_result = spark_local.sql(getCheckDataQuery())
    df_result.show(truncate=False, n=10)

    # 룰 변경 감지 함수
    def check_rule_modified(rule_path):
        import os
        from datetime import timedelta
        mod_time = os.path.getmtime(rule_path)
        mod_datetime = datetime.fromtimestamp(mod_time, ZoneInfo("Asia/Seoul"))
        now = datetime.now(ZoneInfo("Asia/Seoul"))
        if now - mod_datetime < timedelta(minutes=1):
            print(f"룰 변경이 감지되었습니다 {rule_path}.\nlast modified : {mod_datetime.strftime('%Y-%m-%d %H:%M')}")

    # 룰: 매 배치마다 로딩
    check_rule_modified(RULE_PATH_1)
    with open(RULE_PATH_1, "r") as f:
        content_1 = f.read()
    decision_1 = zen.ZenEngine().create_decision(content_1)
    pattern = decision_1.evaluate({})["result"]["pattern"]

    query = getLogMonitorQuery(pattern)
    df_result = spark_local.sql(query)
    rows = [row.asDict() for row in df_result.collect()]

    print(f"Rule 1-1 (pattern {pattern}) 에 의한 알람 대상 장비 조회 완료. 발생한 데이터 : {len(rows)}건")
    df_result.show(truncate=False)

    check_rule_modified(RULE_PATH_2)
    with open(RULE_PATH_2, "r") as f:
        content_2 = f.read()
    decision_2 = zen.ZenEngine().create_decision(content_2)

    final_alarms = []
    now = datetime.now()

    for r in rows:
        res = decision_2.evaluate({"err_cnt": r["err_cnt"]}).get("result", {})
        if res.get("should_alarm", False):
            final_alarms.append({
                "alarm_time": now,
                "router_name": r["router_host"],
                "router_ip": r["router_ip"],
                "error_count": r["err_cnt"],
                "severity": res.get("severity", "Unknown"),
                "alarm_reason": res.get("alarm_reason", "Threshold exceeded"),
                "processing_time": r["dt_kst"]
            })

    print(f"Rule 1-2 에 의한 평가 완료. 발생한 데이터: {len(final_alarms)}건")

    if final_alarms:
        out_df = spark_local.createDataFrame(final_alarms).select(
            "alarm_time", "router_name", "router_ip", "error_count",
            "severity", "alarm_reason", "processing_time"
        )
        out_df.show(truncate=False)
    else:
        print("ℹ️ 알람이 없습니다.")
    print(f"[{datetime.now(ZoneInfo('Asia/Seoul'))}] 마이크로배치 #{batch_id} 실행 완료.\n")

driver = (
    spark.readStream.format("rate").option("rowsPerSecond", 1).load()
)

stream_query = (
    driver.writeStream
    .foreachBatch(run_batch)
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", "/Volumes/cep_demo/network/checkpoints/apps")
    .start()
)
