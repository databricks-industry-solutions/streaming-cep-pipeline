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

- Databricks workspace with Unity Catalog. **Classic compute is required** for the streaming jobs — serverless does not support `processingTime` triggers (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`). The setup job and the rule editor app run fine on either.
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) v0.205+ (tested on v0.299)
- (Rule editor app only) Node.js 18+ for the React frontend build
- Pick a node type for your cloud and update `databricks.yml`'s `node_type_id`:
  - **AWS:** `i3.xlarge` or `m5d.xlarge`
  - **GCP:** `n2-standard-4`
  - **Azure:** `Standard_D4ds_v5` *(default)*

### Quick Start

Defaults: catalog=`cep_demo`, schema=`network`.

```bash
git clone https://github.com/databricks-industry-solutions/streaming-cep-pipeline
cd streaming-cep-pipeline

# 1) Build the rule editor frontend (required before bundle deploy — the
#    React dist is served from disk by the FastAPI backend)
( cd apps/rule-editor/frontend && npm install && npm run build )

# 2) Deploy the bundle (uploads files, creates the cep_setup + cep_pipelines
#    jobs and the cep-rules-editor app definition)
databricks bundle deploy --target dev

# 3) One-shot setup — catalog, schema, volumes, result tables, and 30 days
#    of synthetic data for all three scenarios
databricks bundle run cep_setup --target dev

# 4) Upload starter rule files to the Volume that the pipelines read from
bash scripts/upload_rules.sh

# 5) Start the streaming jobs. This runs forever (a job task per scenario
#    plus the live_injector). Use --no-wait to return immediately; the run
#    keeps going on Databricks until you cancel it.
databricks bundle run cep_pipelines --target dev --no-wait

# 6) Deploy the rule editor Databricks App. The app must be started first
#    (auto-started by `bundle deploy` in newer CLIs; fall back to `apps start`).
WORKSPACE_USER=$(databricks current-user me | python3 -c "import sys,json;print(json.load(sys.stdin)['userName'])")
databricks apps start cep-rules-editor   # idempotent; ok if already running
databricks apps deploy cep-rules-editor \
  --source-code-path "/Workspace/Users/${WORKSPACE_USER}/.bundle/streaming-cep-pipeline/dev/files/apps/rule-editor"
```

Within ~3 minutes the streaming cluster spins up and the first microbatch fires. Alarms appear in `cep_demo.network.{s1_results, s2_results, s3_results}` once a minute. Open the rule editor app URL (printed by `apps deploy`) in your browser to demo hot-reload — edit a rule, save, watch the next microbatch use the new version.

> If `bundle deploy` errors with `unable to verify checksums signature: openpgp: key expired`, your CLI's bundled Terraform is stale. Either upgrade the CLI (`brew upgrade databricks` / equivalent) or point at a system Terraform: `DATABRICKS_TF_EXEC_PATH=$(which terraform) DATABRICKS_TF_VERSION=$(terraform version -json | jq -r .terraform_version) databricks bundle deploy`.

### Customizing the catalog/schema

Edit `databricks.yml`'s `variables.catalog.default` / `variables.schema.default`, OR pass per-deploy:

```bash
databricks bundle deploy --var catalog=my_catalog,schema=my_schema --target dev
databricks bundle run cep_setup --var catalog=my_catalog,schema=my_schema --target dev
bash scripts/upload_rules.sh my_catalog my_schema
```

> The streaming pipeline files (`notebooks/s{1,2,3}-*/pipeline.py`) currently hard-code `cep_demo.network` in SQL. If you change the catalog/schema you also need to find-replace those references (`grep -rl 'cep_demo.network' notebooks/`). The setup notebook (`00_setup.py`) is properly parameterized via widgets.

### Cleanup

```bash
bash scripts/cleanup.sh           # destroys the deployed bundle (jobs, app definition)
```

The Volume contents (rules, checkpoints) and Delta tables persist — drop them manually if you want a fully clean slate (`DROP CATALOG cep_demo CASCADE`).

## Verified

End-to-end run on a field-eng Azure workspace (2026-05-02):

| Component | Status | Notes |
|---|---|---|
| `cep_setup` job | ✅ | Catalog/schema/3 volumes/3 result tables created. s1/s3 generators completed; s2 generator works but is slow due to per-batch Delta commits. |
| Rule files | ✅ | All 5 rules uploaded to `/Volumes/cep_demo/network/rules/` and `rules_apps/`. |
| `cep_pipelines` streaming job | ✅ | Classic single-node cluster spins up in ~3 min, all 4 tasks RUNNING (s1_syslog, s2_linkdown, s3_iptv, live_injector). |
| **S1 (syslog)** | ✅ | Critical alarms emitted every minute. `Edge-RouterA-034`, err_cnt=60 → "Threshold >= 50". |
| **S2 (linkdown)** | ✅ | 4 alarms per minute, one per monitored interface (Router-A-1..4 / `203.0.113.90`). high_count=2 hits the GoRules threshold. |
| **S3 (IPTV)** | ✅ | 2 alarms when the multicast spike (1000) falls inside `last_1m` — diff_ratio=1.5, last_1m=1000, avg_3nm=400. Fires every 5 minutes once warm. |
| `cep-rules-editor` app | ✅ | `app: RUNNING`, OAuth flow live, `/api/rules` endpoint serves Volume contents. |

### Authoring rules

The rule files in `rules/` use the [GoRules JDM](https://gorules.io/docs) format. Two gotchas the live-test surfaced:

- **Function nodes** must declare a `handler` function (no `export`). The expression-only form (`({foo: 1})`) is not enough — zen-engine looks up `handler` by name:

  ```js
  function handler(input) {
    return { pattern: '%sapDHCPLseStatePopulateErr%' };
  }
  ```

- **Decision tables** input cells are lambda-style expressions on the input field (`>= 50`, `"gold"`). Output cells are JSON-ish expressions (`true`, `"Critical"`, `42`). String literals must be quoted. *Caveat:* Spark Decimal columns serialized to the rule did not match the lambda comparison reliably for S3's `diff_ratio` even with `CAST AS DOUBLE` upstream — when the input is numeric and might come from Spark, prefer a function node that does its own `Number(input.x)` coercion (see `rules/3-2.json`). Decision tables are still the right call for string-equality enums (severity tier, status code) and threshold ladders against integer columns (see `rules/1-2.json`).

The pipeline reloads rules on every microbatch (`os.path.getmtime` check), so saving a new rule via the app produces a hot-reload within ~60 seconds — no pipeline restart.

### Live data

`notebooks/live_injector.py` is the 4th task in `cep_pipelines`. It injects fresh-timestamped rows into all source tables every minute so the pipelines' `now - N min` windows actually find data. **In production you'd disable this task** and point the pipelines at real upstream tables (Event Hubs / Zerobus / SDP into Bronze).

The injector exists because the bulk synthetic data from the generators (notebooks/s\*/generate\*.py) covers `2026-02-01 ~ 2026-03-01` — useful for backfill / replay scenarios but invisible to time-windowed streaming queries.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `bundle deploy` fails with `unable to verify checksums signature: openpgp: key expired` | Stale Terraform bundled with the CLI | `brew upgrade databricks` *or* set `DATABRICKS_TF_EXEC_PATH` and `DATABRICKS_TF_VERSION` to your system Terraform |
| Streaming task fails with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` | Pipeline scheduled on serverless compute | Use a classic job cluster (already configured for `cep_pipelines`); serverless does not support `processingTime` triggers |
| Pipeline fails with `unsupported keyword: export` | Function-node rule uses ES module syntax | Use `function handler(input) { ... }` form, no `export const handler` |
| Pipeline fails with `handler is not defined` | Function node uses an expression-only body | zen-engine looks up `handler` by name; declare a function literal |
| S3 alarms never fire even though spikes exist | `diff_ratio` returned as Spark Decimal, serialized as JSON string instead of number | Already fixed: pipeline.py casts `diff_ratio` to `DOUBLE` before passing rows to the rule. If you change the SQL, keep the cast |
| App URL 401/redirects forever | App compute STOPPED, or source not deployed yet | `databricks apps start cep-rules-editor`, then `databricks apps deploy ...` |
| `bundle run cep_setup` is slow on `gen_s2` | Per-batch Delta commits in the synthetic data generator | Generator is correctness-first, not speed; safe to cancel after the s1/s3 generators succeed (they are independent tasks) |
| `Run setup workflow` GitHub Action errors with "demo_workflow not found" | Template CI references the original `demo_workflow` job. Update `.github/workflows/databricks-ci.yml` to point at `cep_setup` (one-shot) — `cep_pipelines` is infinite-streaming and not suitable for CI | n/a |

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
