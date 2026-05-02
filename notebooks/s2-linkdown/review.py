# Databricks notebook source
spark.sql("SET TIME ZONE 'Asia/Seoul'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cep_demo.network.s2_snmp_linkdown_events
# MAGIC where event_at between "2026-01-19 00:39:00" and "2026-01-19 00:39:59"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cep_demo.network.s2_device_link_topology

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT collected_at, router_ip, rx_bytes, if_name FROM cep_demo.network.s2_snmp_interface_traffic
# MAGIC where collected_at between "2026-01-19 00:41:00" and "2026-01-19 00:42:59"

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from cep_demo.network.s2_traffic_forecast
# MAGIC where ds between "2026-01-19 00:41:00" and "2026-01-19 00:42:59"

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT router_host, router_ip, collected_at, count(*) FROM cep_demo.network.s1_router_syslog_events
# MAGIC where collected_at between "2026-01-19 00:39:00" and "2026-01-19 00:40:59" and syslog_message like '%sapDHCPLseStatePopulateErr%'
# MAGIC GROUP BY router_host, router_ip, collected_at

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cep_demo.network.s1_results
# MAGIC where alarm_time between "2026-01-19 00:41:00" and "2026-01-19 00:41:59"
