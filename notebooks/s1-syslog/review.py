# Databricks notebook source
spark.sql("SET TIME ZONE 'Asia/Seoul'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cep_demo.network.s1_router_syslog_events
# MAGIC where collected_at between "2026-01-19 00:39:00" and "2026-01-19 00:40:59"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT router_host, router_ip, collected_at, count(*) FROM cep_demo.network.s1_router_syslog_events
# MAGIC where collected_at between "2026-01-19 00:39:00" and "2026-01-19 00:40:59" and syslog_message like '%sapDHCPLseStatePopulateErr%'
# MAGIC GROUP BY router_host, router_ip, collected_at

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cep_demo.network.s1_results
# MAGIC where alarm_time between "2026-01-19 00:41:00" and "2026-01-19 00:41:59"
