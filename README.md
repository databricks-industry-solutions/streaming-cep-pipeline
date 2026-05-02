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
- Databricks workspace with Unity Catalog enabled
- Serverless compute enabled
- Python: `zen-engine` (GoRules)

### Quick Start

1. Clone this repo into your Databricks Workspace
2. Open the Asset Bundle Editor → Deploy
3. Run the setup notebook to create tables and generate synthetic data
4. Run the scenario pipelines (S1 → S2 → S3)
5. (Optional) Deploy the rule editor app

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
