# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC
# MAGIC Creates the catalog, schema, volumes, and result tables used by the three CEP scenarios.
# MAGIC Idempotent — safe to re-run.
# MAGIC
# MAGIC Customize via Asset Bundle variables in `databricks.yml`:
# MAGIC - `catalog` (default: `cep_demo`)
# MAGIC - `schema`  (default: `network`)

# COMMAND ----------

dbutils.widgets.text("catalog", "cep_demo")
dbutils.widgets.text("schema", "network")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
print(f"Target: {FQ}")

# COMMAND ----------

# MAGIC %md ## Catalog & schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {FQ}")

# COMMAND ----------

# MAGIC %md ## Volumes
# MAGIC
# MAGIC - **`rules`** — pipeline rule JSON files (one per scenario sub-rule)
# MAGIC - **`rules_apps`** — rule editor app target (operators write here, demo of hot-reload)
# MAGIC - **`checkpoints`** — Spark Structured Streaming checkpoints

# COMMAND ----------

for vol in ("rules", "rules_apps", "checkpoints"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.{vol}")
print(f"Volumes ready under /Volumes/{CATALOG}/{SCHEMA}/")

# COMMAND ----------

# MAGIC %md ## Result tables
# MAGIC
# MAGIC The streaming jobs `saveAsTable(...)` into these. Pre-creating with explicit schema avoids
# MAGIC schema-on-write surprises on first batch.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.s1_results (
  alarm_time      TIMESTAMP,
  router_name     STRING,
  router_ip       STRING,
  error_count     BIGINT,
  severity        STRING,
  alarm_reason    STRING,
  processing_time TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.s2_results (
  created_at        TIMESTAMP,
  router_ip         STRING,
  local_device_name STRING,
  high_count        INT,
  low_count         INT
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.s3_results (
  created_at     TIMESTAMP,
  ser_equip_ip   STRING,
  ser_phys_if_nm STRING,
  diff_ratio     DOUBLE,
  last_1m_val    BIGINT,
  avg_3nm_val    DOUBLE
) USING DELTA
""")

print("Result tables ready.")

# COMMAND ----------

# MAGIC %md ## Done
# MAGIC
# MAGIC Next steps:
# MAGIC 1. Upload rule files: `bash scripts/upload_rules.sh` (from repo root)
# MAGIC 2. Run synthetic data generators in `notebooks/s{1,2,3}-*/generate_*.py`
# MAGIC 3. Start streaming jobs (S1/S2/S3 pipelines)
# MAGIC 4. (Optional) Deploy `cep-rules-editor` app
