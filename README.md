# dbt-icebreaker 🧊

**Run dbt locally. Sync to Snowflake. Save money.**

Zero-config local development for dbt. Run models on free DuckDB compute, sync results to your cloud warehouse automatically.

---

## 3-Minute Quickstart

```bash
# 1. Install
pip install git+https://github.com/tysondoberneck/dbt-icebreaker.git

# 2. Configure (just your Snowflake creds)
cat > ~/.dbt/profiles.yml << EOF
my_project:
  target: dev
  outputs:
    dev:
      type: icebreaker
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: ANALYTICS
      schema: DEV
EOF

# 3. Run - sources are cached locally, models run FREE
dbt run
```

That's it! No Iceberg catalog, no S3 setup, no infrastructure.

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

## Data Sources

Icebreaker supports two modes for accessing your source data:

| Feature | Source Cache (Default) | Iceberg Catalog |
|---------|:----------------------:|:---------------:|
| **Setup complexity** | ✅ Zero config | ⚙️ Requires catalog setup |
| **Infrastructure** | ✅ None (just Snowflake creds) | ⚙️ Polaris/Glue/Nessie + S3 |
| **Data freshness** | ⏱️ Point-in-time (TTL refresh) | ✅ Real-time / streaming |
| **Max data size** | ⚠️ Limited by local disk (~10GB) | ✅ Unlimited (S3-backed) |
| **Team collaboration** | ⚠️ Single developer (local) | ✅ Shared catalog state |
| **Works offline** | ✅ Yes (after initial cache) | ❌ Requires S3 access |
| **Cost** | ✅ Free (local compute) | ✅ Free (local compute) |

**Start with Source Cache** — it works out of the box. Upgrade to Iceberg when you hit scale limits.

---

### Source Cache (Default)

Zero-config local development. Sources are cached automatically from Snowflake.

```
Snowflake  ──(first run)──►  ~/.icebreaker/cache/*.parquet
    ▲                              │
    │                              ▼
    └────(sync results)────── DuckDB (FREE)
```

**How it works:**
1. First `dbt run`: Downloads source tables from Snowflake as Parquet
2. Subsequent runs: DuckDB reads from local cache (free!)
3. Results sync back to Snowflake automatically

**Cache management:**
```bash
icebreaker cache status    # Show cached tables, sizes, freshness
icebreaker cache refresh   # Force refresh from Snowflake  
icebreaker cache clear     # Delete local cache
```

**Configuration:**
```yaml
# profiles.yml - Source Cache is ON by default
cache_enabled: true       # Toggle caching
cache_ttl_hours: 24       # Refresh after 24 hours
cache_max_gb: 10          # Max local cache size
```

---

### Iceberg Catalog (Advanced)

For production-scale data or team collaboration, connect to an Iceberg catalog.

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

**Supported Catalogs:**

| Catalog | Config | Status |
|---------|--------|--------|
| Snowflake Polaris | `iceberg_catalog_type: rest` | ✅ Production |
| AWS Glue | `iceberg_catalog_type: glue` | ✅ Production |
| Nessie | `iceberg_catalog_type: nessie` | ✅ Production |

**Configuration:**
```yaml
# profiles.yml - Iceberg Catalog mode
iceberg_catalog_url: https://your-polaris.snowflakecomputing.com
iceberg_catalog_type: rest
iceberg_warehouse: your_warehouse
iceberg_token: "{{ env_var('ICEBERG_TOKEN') }}"
```

**Key benefits:**
- **No data copy**: DuckDB reads your existing Iceberg files in S3
- **Schema-aware**: Catalog metadata ensures correct types and partitions
- **Governance preserved**: Your catalog remains the source of truth

---

## How Routing Works

Icebreaker uses a **7-gate priority system** to decide whether each model runs locally (free) or in the cloud. No configuration needed — it analyzes your SQL automatically.

### Routing Priority

| Gate | Check | Routes to CLOUD if... |
|------|-------|----------------------|
| 1 | **User Override** | You set `icebreaker_route: 'cloud'` in model config |
| 2 | **Failure History** | Model previously failed locally (OOM, etc.) |
| 3 | **External Sources** | SQL references `@stage`, `s3://`, cross-database refs |
| 4 | **Cloud Functions** | SQL uses `CORTEX`, `snowflake.ml`, geo functions |
| 5 | **Dependencies** | Upstream model was routed to cloud |
| 6 | **Data Volume** | Estimated input size > 5GB (configurable) |
| 7 | **Historical Cost** | Used for predictive optimization |

**If all gates pass → routes to LOCAL (free compute)**

### What Gets Detected

**Cloud-only SQL patterns:**
```sql
-- These auto-route to CLOUD:
SELECT cortex.complete('llama3', 'Summarize: ' || text) FROM ...
SELECT * FROM @my_stage/file.csv
SELECT * FROM other_database.schema.table  -- cross-database
COPY INTO my_table FROM @stage
```

**Local-safe SQL patterns:**
```sql
-- These run locally for FREE:
SELECT * FROM {{ ref('stg_orders') }}
SELECT * FROM iceberg_catalog.my_schema.my_table  -- Iceberg!
SELECT *, data:nested.field::string FROM ...  -- Semi-structured
```

### Volume Estimation

Icebreaker queries `INFORMATION_SCHEMA.TABLES` to estimate input data volume:

```python
# If combined input tables > 5GB, route to cloud
volume_gb = sum(upstream_table.bytes for table in dependencies)
if volume_gb > max_local_gb:
    return CLOUD
```

### Historical Cost Prediction (Optional)

If you grant Icebreaker access to `snowflake.account_usage.query_history`, it uses historical execution patterns:

```sql
-- Requires ACCOUNTADMIN or explicit grant
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE my_role;
```

This enables smarter routing based on actual query costs, not just estimates.

### Debug Routing Decisions

```bash
# See why a model routes where it does
icebreaker explain models/my_model.sql
```

Output:
```
Model: my_model
Decision: 🏠 LOCAL: Automatic routing (free compute)

Analysis:
  External sources: ✓ None detected
  Cloud functions: ✓ None detected
  Iceberg sources: ✓ iceberg_catalog.analytics.events
  Estimated volume: 1.2GB (under 5GB limit)
```

---

## Validation & Proof Points

### Architecture Validation

| Component | Technology | Maturity |
|-----------|------------|----------|
| Local compute | DuckDB 1.0+ | Production-ready |
| Iceberg reading | DuckDB Iceberg extension | Stable since 2024 |
| SQL transpilation | sqlglot | 50k+ GitHub stars |
| Cloud sync | Snowflake COPY INTO | Battle-tested |


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
| `icebreaker status` | Check DuckDB + Snowflake connection + health |
| `icebreaker savings` | Show cost savings from local runs |
| `icebreaker savings --dashboard` | Enhanced dashboard with trends & projections |
| `icebreaker health` | Run system health checks |
| `icebreaker summary` | Show last dbt run summary |
| `icebreaker cache status` | Show cached source tables |
| `icebreaker cache refresh` | Force refresh all cached sources |
| `icebreaker cache clear` | Clear local cache |
| `icebreaker sync <table>` | Manually sync a table to Snowflake |
| `icebreaker verify` | Compare row counts (local vs Snowflake) |
| `icebreaker explain <file>` | Show why a model routes LOCAL or CLOUD |

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
| `cache_enabled` | Enable source caching | `true` |
| `cache_ttl_hours` | Cache freshness (hours) | `24.0` |
| `cache_max_gb` | Max cache size | `10.0` |
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

**v1.0 Complete! ✅**
- [x] Zero-config source caching
- [x] Enhanced savings dashboard with trends
- [x] Health check system
- [x] 12 SQL transpilation transforms
- [x] Run summary after each dbt run
- [x] Actionable error messages

**Coming Soon**
- [ ] BigQuery and Databricks cloud adapters
- [ ] MotherDuck hybrid cloud support
- [ ] GitHub Actions workflow template
- [ ] Unity Catalog (Databricks) integration

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
