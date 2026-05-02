# Real-time CEP Pipeline on Databricks

[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Serverless](https://img.shields.io/badge/Serverless-Compute-00C851?style=for-the-badge)](https://docs.databricks.com/en/compute/serverless.html)
[![GoRules](https://img.shields.io/badge/GoRules-MIT_License-blue?style=for-the-badge)](https://github.com/gorules/zen)

A production-validated reference architecture for **Complex Event Processing (CEP)** on Databricks, combining Spark Structured Streaming with an embedded rule engine ([GoRules](https://gorules.io/)).

## The Problem

Enterprises with real-time event streams need to detect complex patterns — multi-source correlation, threshold-based escalation, time-windowed anomaly detection — with sub-minute latency. Two core challenges exist:

- **CEP pattern complexity:** Real-world fault detection demands multi-step rules spanning multiple data sources. These patterns are extremely difficult to implement at the code level and embed into streaming pipelines.
- **Rule agility:** Detection rules are typically hardcoded in pipeline code. Any change requires developer involvement, code redeployment, and pipeline restart.

## The Solution

![CEP Architecture](diagrams/cep-architecture.png)

This solution accelerator provides an end-to-end CEP pipeline with:

| Component | Description |
|-----------|-------------|
| **Streaming Infrastructure** | Spark Structured Streaming with `foreachBatch` (1-min microbatch), multi-source SQL joins, time-windowed aggregations |
| **Embedded Rule Engine** | GoRules (`pip install zen-engine`, MIT license) embedded in the Spark process — Decision Tables for thresholds, Function Nodes for complex logic |
| **Hot-Reload** | Rule JSON files in Unity Catalog Volumes, reloaded every microbatch via file modification detection — zero downtime, zero code changes |
| **Visual Rule Editor** | Databricks Apps (FastAPI + React + GoRules JDM Editor) for operators to edit and deploy rules without code |

### Performance

| Metric | S1 (Simple) | S2 (Multi-source) | S3 (Topology-aware) |
|--------|-------------|-------------------|---------------------|
| Batch time | ~6s | ~7s | ~9s |
| Data sources | 1 | 4+ | 4+ |
| Rule types | Decision Table | Function Node | DT + FN + SQL |
| Compute | Serverless | Serverless | Serverless |

Estimated cost: **~$1.16/hour** on serverless compute.

## Three Scenarios

### S1 — Single-Source Pattern Detection
Syslog pattern matching with configurable thresholds. Rule 1-1 (Function Node) extracts the pattern, Rule 1-2 (Decision Table) evaluates severity based on error count.

### S2 — Multi-Source Correlation
Joins 4+ data sources (syslog, SNMP traffic, equipment topology, ML forecast) within a single microbatch. Uses `from_json` + `LATERAL VIEW explode` for SQL chaining. GoRules Function Node groups anomalies per IP and escalates when thresholds are met.

### S3 — Topology-Aware Detection
Equipment topology traversal (OLT → Aggregation Node → Service Node) with double LEFT JOIN. Calculates diff_ratio between current and previous periods. Includes alarm lifecycle management with deduplication and auto-cancellation.

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog and Serverless compute
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) v0.205+
- (Rule editor app only) Node.js 18+ for the React frontend build

### Quick Start

The whole pipeline (3 streaming scenarios + rule editor app) deploys as a single Databricks Asset Bundle. Defaults: catalog=`cep_demo`, schema=`network`. Override with `--var catalog=...,schema=...` if needed.

```bash
git clone https://github.com/databricks-industry-solutions/streaming-cep-pipeline
cd streaming-cep-pipeline

# 1) Build the rule editor frontend (required before bundle deploy)
( cd apps/rule-editor/frontend && npm install && npm run build )

# 2) Deploy the bundle (creates jobs + app definitions)
databricks bundle deploy --target dev

# 3) One-shot setup — creates catalog, schema, volumes, result tables,
#    and 30 days of synthetic data for all three scenarios
databricks bundle run cep_setup --target dev

# 4) Upload starter rule files to the Volume the pipelines read from
bash scripts/upload_rules.sh

# 5) Start the streaming jobs (this runs forever; cancel the run to stop)
databricks bundle run cep_pipelines --target dev

# 6) (Optional) Deploy the rule editor Databricks App
databricks apps deploy cep-rules-editor \
  --source-code-path /Workspace/Users/$(databricks current-user me | jq -r .userName)/.bundle/streaming-cep-pipeline/dev/files/apps/rule-editor
```

After step 5, alarms will appear in `cep_demo.network.{s1_results, s2_results, s3_results}` within ~1 minute. Hot-reload demo: edit a rule via the app UI in step 6 → next microbatch picks it up automatically.

### Customizing the catalog/schema

Edit `databricks.yml`'s `variables.catalog.default` / `variables.schema.default`, OR pass per-deploy:

```bash
databricks bundle deploy --var catalog=my_catalog,schema=my_schema --target dev
databricks bundle run cep_setup --var catalog=my_catalog,schema=my_schema --target dev
bash scripts/upload_rules.sh my_catalog my_schema
```

> Note: the streaming pipeline files (`notebooks/s{1,2,3}-*/pipeline.py`) currently hard-code `cep_demo.network` in their SQL. If you change the catalog/schema you also need to find-replace those references (or fork and adjust). Future improvement: read from widgets like `00_setup.py` does.

### Cleanup

```bash
bash scripts/cleanup.sh           # destroys the deployed bundle
```

The Volume contents (rules, checkpoints) and Delta tables persist — drop them manually if you want a fully clean slate.

### Project Structure

```
streaming-cep-pipeline/
├── notebooks/
│   ├── s1-syslog/               # S1: single-source pattern detection
│   │   ├── pipeline.py          #   Streaming foreachBatch driver
│   │   ├── microbatch.py        #   Single-batch debug variant
│   │   ├── review.py            #   Data inspection SQL
│   │   └── generate_input.py    #   Synthetic data generator
│   ├── s2-linkdown/             # S2: multi-source correlation (4+ tables)
│   │   ├── pipeline.py
│   │   ├── microbatch.py
│   │   ├── target_timestamp.py  #   Re-run for a fixed timestamp
│   │   ├── review.py
│   │   └── generate_inputs.py
│   └── s3-iptv/                 # S3: topology-aware detection (OLT → AGG → SER)
│       ├── pipeline.py
│       ├── microbatch.py
│       ├── review.py
│       └── generator.py
├── apps/
│   └── rule-editor/             # Databricks App (FastAPI + React + GoRules JDM Editor)
│       ├── app.py
│       ├── app.yaml
│       ├── apps-microbatch.py   # S1 variant that loads rules from rules_apps Volume
│       ├── backend/             # /api/rules CRUD over UC Volume
│       └── frontend/            # React + Vite (build with `npm run build`)
├── dashboards/                  # Lakeview dashboards (optional)
├── scripts/                     # Setup / cleanup helpers
├── databricks.yml               # Asset Bundle configuration
└── README.md
```

## Rule Hot-Reload Demo

One of the key features — operators can change rules while the pipeline is running:

1. Pipeline running with thresholds: `Critical >= 50`, `Warning >= 10`
2. Operator edits rule via visual editor → saves to UC Volume
3. Next microbatch (~1 min) picks up new rule automatically
4. New thresholds take effect immediately — **zero restart, zero code change**

## Contributing

1. `git clone` this project locally
2. Use Databricks CLI to test changes against a workspace
3. Submit PRs with a second-party review

## Third-Party Package Licenses

&copy; 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the [Databricks License](https://databricks.com/db-license-source). All included or referenced third party libraries are subject to the licenses set forth below.

| Package | License | Copyright |
|---------|---------|-----------|
| [zen-engine](https://github.com/gorules/zen) | MIT | GoRules |
| [@gorules/jdm-editor](https://github.com/gorules/jdm-editor) | MIT | GoRules |
| [FastAPI](https://github.com/tiangolo/fastapi) | MIT | Sebastián Ramírez |
| [React](https://github.com/facebook/react) | MIT | Meta Platforms |

## Authors

- **Sangwon Park** ([@freepsw](https://github.com/freepsw)) — Project initiative & core architecture
- **Jeonghwan Lee** ([@bellamy-k](https://github.com/bellamy-k)) — Implementation & packaging

## Catalog & Volume Conventions

The notebooks reference a catalog `cep_demo` and schema `network`. Adjust these to match your workspace:

```sql
CREATE CATALOG IF NOT EXISTS cep_demo;
CREATE SCHEMA  IF NOT EXISTS cep_demo.network;
CREATE VOLUME  IF NOT EXISTS cep_demo.network.rules;        -- pipeline rule files
CREATE VOLUME  IF NOT EXISTS cep_demo.network.rules_apps;   -- rule editor app target
CREATE VOLUME  IF NOT EXISTS cep_demo.network.checkpoints;  -- streaming checkpoints
```

If you use different names, update the `RULE_PATH*`, `TARGET_TABLE`, and `checkpointLocation` constants at the top of each pipeline file (and the `APP_VOLUME_PATH` env var in `apps/rule-editor/app.yaml`).
