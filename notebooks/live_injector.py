# Databricks notebook source
# MAGIC %md
# MAGIC # Live data injector
# MAGIC
# MAGIC Continuously inserts fresh-timestamped rows into the source tables so the
# MAGIC streaming pipelines have something to detect. Without this, the synthetic
# MAGIC data from the generators is historical (2026-02-01 ~ 2026-03-01) and the
# MAGIC `current_timestamp() - INTERVAL N MINUTE` windows in the pipelines never
# MAGIC see it.
# MAGIC
# MAGIC Runs alongside the 3 CEP pipelines as a 4th task in the
# MAGIC `cep_pipelines` job. Cancel the run to stop.
# MAGIC
# MAGIC All rows are inserted with `collected_at`/`event_at`/`eventhub_received_at`
# MAGIC = `current_timestamp()`. The pipelines read sliding past windows
# MAGIC (e.g. S1 [now-2, now), S2 finds events at minute "now-5", S3 at minute
# MAGIC "now-3"), so a steady drumbeat of `now`-stamped rows naturally feeds
# MAGIC every batch.

# COMMAND ----------

import time
from datetime import datetime
from zoneinfo import ZoneInfo


def inject_s1(spark, tick):
    """60 syslog rows -> Critical alarm via 1-2.json (>= 50 threshold)."""
    spark.sql("""
        INSERT INTO cep_demo.network.s1_router_syslog_events
        SELECT
          'Edge-RouterA-034' AS router_host,
          '203.0.113.34'    AS router_ip,
          current_timestamp() AS collected_at,
          '5288 Base DHCP-WARNING-sapDHCPLseStatePopulateErr-2005 [Lease State Population Error]:  Lease state table population error on SAP lag-13 in service 100 - Conflict with lease 198.51.100.107 on SAP lag-12' AS syslog_message
        FROM range(60)
    """)


def inject_s2(spark, tick):
    """linkdown + high traffic + forecast -> high_anomaly via 2.json correlator.

    Linkdown event at NOW becomes "5 minutes ago" by the time the pipeline
    fires (its `target_minute = trunc(now-5)`). Traffic and forecast also
    inserted at NOW; the pipeline reads them in [target+2, target+4] which
    is roughly [now-3, now-1] → matches our most recent injections.
    """
    spark.sql("""
        INSERT INTO cep_demo.network.s2_snmp_linkdown_events
        SELECT
          current_timestamp() AS event_at,
          'Router-A', '203.0.113.90', 'SNMP_LINK_DOWN', '000',
          link_type, 'OFB-A', 'CD1',
          current_timestamp(), current_timestamp()
        FROM (SELECT explode(array('CORE-METRO','CORE-EDGE')) AS link_type)
    """)
    # High traffic (100 Gbps -> exceeds yhat_upper of 60) for the 4 monitored interfaces
    spark.sql("""
        INSERT INTO cep_demo.network.s2_snmp_interface_traffic (collected_at, router_ip, rx_bytes, if_name)
        SELECT
          current_timestamp(),
          '203.0.113.90',
          CAST((100.0 * 1e9 * 60) / 8 AS BIGINT),
          ifn.if_name
        FROM (SELECT explode(array('2/1/1','2/1/2','2/2/1','2/2/2')) AS if_name) ifn
    """)
    # Matching forecast row per link_type at NOW
    spark.sql("""
        INSERT INTO cep_demo.network.s2_traffic_forecast (router_ip, link_type, ds, y, yhat, yhat_lower, yhat_upper, phase)
        SELECT
          '203.0.113.90',
          link_type,
          current_timestamp(),
          50.0, 50.0, 40.0, 60.0, 'realtime'
        FROM (SELECT explode(array('CORE-METRO','CORE-EDGE')) AS link_type)
    """)


def inject_s3(spark, tick):
    """OLT alarm + multicast spike pattern -> diff_ratio >= 0.2 alarm via 3-2.json.

    Alarm at NOW gets caught by the pipeline 4 minutes later when
    eventhub_received_at falls into [now-4, now-3] window.

    Traffic value pattern: spike (1000) every 5th tick; baseline (100)
    otherwise. The pipeline computes diff_ratio = (last_1m - avg_3nm) / avg_3nm.
    With ~20% of recent rows being spikes, last_1m occasionally lands on a
    spike minute → alarm fires.
    """
    spark.sql("""
        INSERT INTO cep_demo.network.s3_olt_alarm_events
        VALUES (current_timestamp(), 'R822', '1', 'FCLT_STD', '192.0.2.23')
    """)
    # Vary traffic to create spike pattern that triggers diff_ratio >= 0.2.
    # Every 5th minute is a 10x spike; other minutes are baseline.
    value = 1000 if (tick % 5 == 0) else 100
    spark.sql(f"""
        INSERT INTO cep_demo.network.s3_snmp_interface_traffic (collected_at, router_ip, tx_multicast_pkts, if_name)
        SELECT
          current_timestamp(),
          r.router_ip,
          {value},
          r.if_name
        FROM (
          SELECT '198.51.100.211' AS router_ip, '9/2/1' AS if_name UNION ALL
          SELECT '198.51.100.212',                '9/1/1'
        ) r
    """)


# Tick once a minute. Each iteration is fast (~1-2s of SQL).
seoul = ZoneInfo("Asia/Seoul")
i = 0
while True:
    try:
        ts = datetime.now(seoul).strftime("%Y-%m-%d %H:%M:%S")
        inject_s1(spark, i)
        inject_s2(spark, i)
        inject_s3(spark, i)
        i += 1
        print(f"[{ts}] tick #{i} — injected fresh rows for S1, S2, S3 (s3 spike: {i % 5 == 0})")
    except Exception as e:
        print(f"inject error at tick {i}: {e}")
    time.sleep(60)
