# dbt-icebreaker 🧊

**Local-first development for your Iceberg lakehouse.**

Run dbt models locally with DuckDB, sync results to Snowflake automatically. Zero dev costs, same production data.

---

## Why Icebreaker?

Your data lives in S3 as Iceberg tables. You use Snowflake for analytics. But every `dbt run` costs money:

| Dev Activity | Snowflake Cost |
|--------------|----------------|
| 60-second minimum per query | $0.033 |
| × 10 dbt runs per day | $0.33/dev/day |
| × 20 developers × 22 days | **$145/month** |

**Icebreaker eliminates these costs** by running locally against your Iceberg data.

---

## Use Cases

### 1. Development Without the Cloud Bill
**Problem**: Every `dbt run` during development spins up Snowflake compute. With 60-second minimum billing, even a quick syntax check costs money.

**Solution**: Icebreaker runs your models on local DuckDB. You iterate freely, validate SQL, and only sync final results to Snowflake.

```bash
# Iterate 50 times locally = $0
# vs. 50 Snowflake queries = $1.65+
dbt run -s my_model
```

### 2. CI/CD Cost Control
**Problem**: Pull request validation runs the full dbt project against the cloud warehouse. A 20-model PR check costs ~$0.66 per run.

**Solution**: Run `dbt run` with Icebreaker in your CI pipeline — models execute on local compute within the runner, eliminating warehouse costs for validation.

### 3. Lakehouse-First Analytics
**Problem**: Your data lands in S3 as Iceberg tables (via Estuary, Airbyte, Fivetran). You want analytics without migrating everything to Snowflake first.

**Solution**: Icebreaker queries Iceberg directly. Only transformed results go to Snowflake for BI consumption.

### 4. Multi-Engine Flexibility
**Problem**: You're locked into one warehouse's pricing and capabilities.

**Solution**: Icebreaker lets you run transforms wherever makes sense—local for dev, Snowflake for production, with the same dbt code.

---

## Why Not Just Use DuckDB?

`dbt-duckdb` exists. Here's why Icebreaker is different:

| Capability | dbt-duckdb | dbt-icebreaker |
|------------|------------|----------------|
| **Configuration** | Two separate profiles (DuckDB + Snowflake) | Single hybrid profile |
| **Cloud-only SQL** | Fails on `CORTEX`, UDFs, etc. | Auto-routes to cloud |
| **Sync to warehouse** | Manual (re-run on cloud) | Automatic after each model |
| **Dialect** | Write DuckDB SQL | Write Snowflake SQL (transpiled) |
| **Incremental state** | Isolated (breaks state) | Synchronized across engines |
| **Iceberg access** | Manual setup | Native catalog integration |

### The Real Difference

With `dbt-duckdb`, you're maintaining **two workflows**:
1. Dev locally in DuckDB
2. Re-run everything in Snowflake for production

With Icebreaker, you have **one workflow**:
1. Run `dbt run` — it routes automatically, syncs automatically, tracks savings automatically

---

## Catalog Integration

Icebreaker connects directly to your Iceberg catalog. No data movement, no duplication.

### Supported Catalogs

| Catalog | Config | Status |
|---------|--------|--------|
| **Snowflake Polaris** | `iceberg_catalog_type: rest` | ✅ Production |
| **AWS Glue** | `iceberg_catalog_type: glue` | ✅ Production |
| **Nessie** | `iceberg_catalog_type: nessie` | ✅ Production |
| **REST Catalog** | `iceberg_catalog_type: rest` | ✅ Production |

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                       Your Iceberg Catalog                      │
│  (Polaris / Glue / Nessie)                                      │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Icebreaker connects via REST API                       │   │
│  │  → Discovers namespaces and tables                      │   │
│  │  → Reads S3 file locations from metadata                │   │
│  │  → DuckDB reads Parquet files directly                  │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Key benefits:**
- **No data copy**: DuckDB reads your existing Iceberg files in S3
- **Schema-aware**: Catalog metadata ensures correct types and partitions
- **Governance preserved**: Your catalog remains the source of truth

---

## Validation & Proof Points

### Architecture Validation

| Component | Technology | Maturity |
|-----------|------------|----------|
| Local compute | DuckDB 1.0+ | Production-ready |
| Iceberg reading | DuckDB Iceberg extension | Stable since 2024 |
| SQL transpilation | sqlglot | 50k+ GitHub stars |
| Cloud sync | Snowflake COPY INTO | Battle-tested |

### Why This Stack Now?

Three technologies matured simultaneously:

1. **Apache Iceberg** became the standard open table format (adopted by Snowflake, Databricks, AWS)
2. **DuckDB 1.0** shipped with production-ready Iceberg support
3. **Iceberg REST catalogs** (Polaris, Glue) enabled standardized metadata access

Icebreaker is the first adapter to provide automatic hybrid routing between local and cloud warehouses, with built-in transpilation and sync.

### Cost Savings Example

| Scenario | Traditional | With Icebreaker | Savings |
|----------|-------------|-----------------|---------|
| 10 devs, 10 runs/day, 22 days | $145/month | $0 | **100%** |
| CI: 50 PRs/week @ 20 models each | $132/month | $0 | **100%** |
| Staging refresh (nightly) | $22/month | $22/month* | 0% |

*Heavy production workloads still route to cloud by design.

---

## Setup Guide

### Prerequisites

- Python 3.9+
- AWS credentials (for S3 access)
- Snowflake account (for sync destination)
- Iceberg REST catalog (Polaris, Glue, etc.)

### Step 1: Install

```bash
pip install git+https://github.com/tysondoberneck/dbt-icebreaker.git
```

With Snowflake connector:
```bash
pip install "git+https://github.com/tysondoberneck/dbt-icebreaker.git#egg=dbt-icebreaker[snowflake]"
```

### Step 2: Configure Environment

Icebreaker uses your existing AWS and Snowflake credentials:
- **AWS**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (for S3/Iceberg)
- **Snowflake**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

These are typically already set in your environment or `.env` file.

### Step 3: Configure profiles.yml

```yaml
# ~/.dbt/profiles.yml

my_project:
  target: dev
  outputs:
    dev:
      type: icebreaker
      iceberg_catalog_url: https://your-polaris.snowflakecomputing.com
      iceberg_warehouse: your_warehouse
```

**Other catalog types:**

```yaml
# AWS Glue
iceberg_catalog_type: glue
iceberg_s3_region: us-east-1

# REST Catalog
iceberg_catalog_type: rest
iceberg_catalog_url: https://your-catalog.example.com
iceberg_token: "{{ env_var('ICEBERG_TOKEN') }}"

# Nessie
iceberg_catalog_type: nessie
iceberg_catalog_url: https://nessie.example.com
```

Snowflake sync uses standard env vars: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`.

### Step 4: Verify Setup

```bash
# Check connection status
icebreaker status

# Test with a single model
dbt run -s my_model
```

---

## Usage Guide

### Basic Workflow

```bash
# 1. Run models locally (reads Iceberg, syncs to Snowflake)
dbt run

# 2. Check your tables in Snowflake
#    → Results are available immediately after each model

# 3. View cost savings
icebreaker savings
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `icebreaker status` | Check DuckDB + Snowflake connection |
| `icebreaker savings` | Show cost savings from local runs |
| `icebreaker sync <table>` | Manually sync a table to Snowflake |
| `icebreaker verify` | Compare row counts (local vs Snowflake) |
| `icebreaker explain <file>` | Show why a model routes LOCAL or CLOUD |
| `icebreaker help` | Full command reference |

### Routing Control

Models run locally by default. Force cloud when needed:

```sql
-- models/needs_live_data.sql
{{ config(icebreaker_route='cloud') }}

SELECT * FROM external_api.live_feed
```

### Sync Modes

| Mode | Behavior | Best For |
|------|----------|----------|
| `model` | Sync after each model | Interactive dev (check results immediately) |
| `batch` | Sync all at end of run | CI/CD (minimize Snowflake overhead) |

---

## How It Works

```
┌───────────────────────────────────────────────────────────────┐
│                                                               │
│   S3 (Iceberg)  ───────►  DuckDB (Local)  ───────►  Snowflake │
│        ▲                       │                       │      │
│        │                       │ (FREE)                │      │
│        └───────────────────────┘                       ▼      │
│                                               BI/Dashboards   │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

1. **Read**: DuckDB reads your Iceberg tables directly from S3
2. **Execute**: SQL transpiled from Snowflake → DuckDB dialect
3. **Sync**: Results uploaded to Snowflake via COPY INTO
4. **Query**: BI tools read from Snowflake as usual

---

## Configuration Reference

| Setting | Description | Default |
|---------|-------------|---------|
| `iceberg_catalog_url` | Iceberg REST catalog URL | Required |
| `iceberg_catalog_type` | `rest`, `glue`, or `nessie` | `rest` |
| `iceberg_warehouse` | Catalog warehouse/namespace | Required |
| `cloud_type` | Cloud warehouse type | `snowflake` |
| `account` | Snowflake account | Required |
| `user` | Snowflake username | Required |
| `password` | Snowflake password | Required |
| `database` | Target database | Required |
| `schema` | Target schema | Required |
| `sync_mode` | `model` or `batch` | `model` |
| `sync_enabled` | Enable auto-sync | `true` |
| `max_local_size_gb` | Max data for local processing | `5.0` |

---

## Troubleshooting

### "Iceberg catalog connection failed"
- Check `iceberg_catalog_url` is correct
- Verify `iceberg_token` or AWS credentials

### "Snowflake sync failed"  
- Ensure Snowflake credentials are set in environment
- Check `icebreaker status` for connection state

### Model routes to CLOUD unexpectedly
- Run `icebreaker explain models/my_model.sql` 
- Check for cloud-only functions (CORTEX, etc.)

---

## Roadmap

**Coming Soon**
- [ ] BigQuery and Databricks cloud adapters
- [ ] MotherDuck hybrid cloud support  
- [ ] `icebreaker init` for zero-config setup
- [ ] GitHub Actions workflow template

**Planned**
- [ ] Query cost prediction before execution
- [ ] Smart Iceberg metadata caching
- [ ] Team-wide savings dashboard
- [ ] Unity Catalog (Databricks) integration

**Future**
- [ ] Delta Lake support
- [ ] dbt Cloud execution environment
- [ ] Observability hooks (Datadog, Prometheus)

---

## Limitations

- **MVP: Snowflake only** - Other warehouses coming later
- **Memory-bound** - Large tables may exceed local RAM  
- **SQL gaps** - Some Snowflake UDFs not yet transpiled

---

## License

Apache 2.0

---

*"The best Snowflake credit is the one you don't spend."*
