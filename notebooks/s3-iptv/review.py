# Databricks notebook source
spark.sql("SET TIME ZONE 'Asia/Seoul'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cep_demo.network.s3_olt_alarm_events
# MAGIC WHERE eventhub_received_at between "2026-01-19 00:45:00" and "2026-01-19 00:48:59"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT link_type, local_device_name, local_device_ip, local_phys_if, remote_device_ip FROM cep_demo.network.s3_device_link_topology

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cep_demo.network.s3_router_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT collected_at, router_ip, tx_multicast_pkts, if_name FROM cep_demo.network.s3_snmp_interface_traffic
# MAGIC where collected_at between "2026-01-19 00:37:00" and "2026-01-19 00:46:59"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cep_demo.network.s3_results
# MAGIC where created_at between "2026-01-19 00:49:00" and "2026-01-19 00:49:59"

# COMMAND ----------
