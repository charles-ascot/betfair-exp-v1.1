# CHIMERA Project Variables Reference

## Last Updated: 2026-01-13

---

## GCP Project Configuration

| Variable Name | Value | Purpose | Type |
|---------------|-------|---------|------|
| GCP Project ID | `betfair-data-explorer` | Core Google Cloud project | Config |
| GCP Account | `cloud@ascotwm.com` | Service account / user | Auth |
| Default Region | `us-central1` | Dataflow execution region (switched from europe-west2 due to resource exhaustion) | Config |
| Secondary Region | `europe-west2` | UK-based region (original, had zone exhaustion) | Config |

---

## Cloud Storage (GCS)

| Variable Name | Value | Purpose | Type |
|---------------|-------|---------|------|
| Source Bucket | `gs://betfair-basic-historic` | Contains Betfair historical data | GCS |
| Source Path (2024) | `BASIC/2024/` | Full 2024 data location | Path |
| Month Folders | `Jan/`, `Feb/`, `Mar/`, `Apr/`, `May/`, `Jun/`, `Jul/`, `Aug/`, `Sep/`, `Oct/`, `Nov/`, `Dec/` | Monthly subfolders | Path |
| File Pattern | `**/*.bz2` | All compressed market files | Glob |
| Temp Bucket | `gs://betfair-data-explorer-temp` | Pipeline temporary storage | GCS |
| Temp Location | `gs://betfair-data-explorer-temp/tmp/` | Beam temp files | Path |
| Staging Location | `gs://betfair-data-explorer-temp/staging/` | Beam staging files | Path |

---

## BigQuery Configuration

| Variable Name | Value | Purpose | Type |
|---------------|-------|---------|------|
| BQ Project | `betfair-data-explorer` | BigQuery project | Config |
| Main Dataset | `chimera_horses_2024` | Primary data warehouse | BQ Dataset |
| Features Dataset | `chimera_features_2024` | Feature engineering | BQ Dataset |
| Backtest Dataset | `chimera_backtest_2024` | Backtesting results | BQ Dataset |
| Legacy Dataset | `BASIC_2024` | Previous data attempts | BQ Dataset |
| Transfer Dataset | `betfairhistoricaldata_dfbd7cbe_ab9c_4fe5_8854_81f65528ef78` | Data transfer service | BQ Dataset |

### Tables in `chimera_horses_2024`

| Table Name | Partitioned By | Clustered By | Purpose |
|------------|----------------|--------------|---------|
| `market_snapshots` | `snapshot_date` (DAY) | `horse_id`, `race_id`, `snapshot_time` | Time-series market data |
| `horse_race_participations` | `race_date` (DAY) | `horse_id`, `track_name`, `race_id` | Per-race horse data |
| `horse_master` | `created_at` (DAY) | `horse_id`, `last_race_date` | Horse master data |
| `raw_mcm_staging` | None | None | Raw MCM message staging |

---

## Dataflow Configuration

| Variable Name | Value | Purpose | Type |
|---------------|-------|---------|------|
| Runner | `DataflowRunner` | Cloud execution | Config |
| SDK Version | Apache Beam Python 3.13 SDK 2.70.0 | Pipeline framework | Software |
| Job Type | `FNAPI_BATCH` | Batch processing | Config |
| Shuffle Mode | `SERVICE_BASED` | Google-managed shuffle | Config |
| Use Public IPs | `true` | Network configuration | Config |
| Failed Job ID (us-central1) | `2026-01-12_02_30_16-15273293712672769827` | Most recent failed job | Reference |
| Failed Job ID (europe-west2) | `2026-01-12_02_27_29-1617757874806862121` | Earlier failed job | Reference |
| Job Name Pattern | `beamapp-livestreammachine-{timestamp}` | Auto-generated job names | Pattern |

---

## GitHub Repository

| Variable Name | Value | Purpose | Type |
|---------------|-------|---------|------|
| Repository URL | `https://github.com/charles-ascot/betfair-exp-v1.1.git` | Source code repository | Git |
| Main Branch | `main` | Default branch | Git |
| Auto Deploy | Cloudflare Pages | Frontend deployment | CI/CD |

---

## Application Endpoints

| Variable Name | Value | Purpose | Type |
|---------------|-------|---------|------|
| Frontend URL | Cloudflare Pages (auto-deployed) | Data explorer UI | Web |
| GCP Console | `https://console.cloud.google.com/dataflow/jobs?project=betfair-data-explorer` | Dataflow monitoring | Web |
| BigQuery Console | `https://console.cloud.google.com/bigquery?project=betfair-data-explorer` | Data warehouse | Web |

---

## Data Format Reference

| Variable Name | Value | Purpose | Type |
|---------------|-------|---------|------|
| Input Format | NDJSON (Newline Delimited JSON) | Betfair stream format | Format |
| Compression | BZ2 | File compression | Format |
| Message Type | MCM (Market Change Message) | Betfair protocol | Format |
| File Types | M (Market), E (Event) | Betfair file classification | Format |
| Selected File Types | `['M']` | Frontend filter setting | Config |

---

## Pipeline Schema: market_snapshots (Pipeline Output)

```json
{
  "fields": [
    {"name": "race_date", "type": "DATE"},
    {"name": "horse_id", "type": "STRING"},
    {"name": "horse_name", "type": "STRING"},
    {"name": "race_id", "type": "STRING"},
    {"name": "market_id", "type": "STRING"},
    {"name": "runner_id", "type": "INTEGER"},
    {"name": "snapshot_time", "type": "TIMESTAMP"},
    {"name": "ltp", "type": "FLOAT64"},
    {"name": "back_price", "type": "FLOAT64"},
    {"name": "lay_price", "type": "FLOAT64"},
    {"name": "back_volume", "type": "FLOAT64"},
    {"name": "lay_volume", "type": "FLOAT64"},
    {"name": "total_matched_volume", "type": "FLOAT64"},
    {"name": "implied_probability", "type": "FLOAT64"},
    {"name": "track_name", "type": "STRING"},
    {"name": "race_name", "type": "STRING"},
    {"name": "market_type", "type": "STRING"},
    {"name": "country_code", "type": "STRING"},
    {"name": "event_id", "type": "STRING"},
    {"name": "ingest_timestamp", "type": "TIMESTAMP"},
    {"name": "source_file", "type": "STRING"}
  ]
}
```

---

## Pipeline Schema: horse_race_participations (Pipeline Output)

```json
{
  "fields": [
    {"name": "race_date", "type": "DATE"},
    {"name": "horse_id", "type": "STRING"},
    {"name": "horse_name", "type": "STRING"},
    {"name": "race_id", "type": "STRING"},
    {"name": "market_id", "type": "STRING"},
    {"name": "runner_id", "type": "INTEGER"},
    {"name": "track_name", "type": "STRING"},
    {"name": "race_name", "type": "STRING"},
    {"name": "market_type", "type": "STRING"},
    {"name": "event_id", "type": "STRING"},
    {"name": "country_code", "type": "STRING"},
    {"name": "closing_ltp", "type": "FLOAT64"},
    {"name": "closing_implied_prob", "type": "FLOAT64"},
    {"name": "ingest_timestamp", "type": "TIMESTAMP"},
    {"name": "source_file", "type": "STRING"}
  ]
}
```

---

## Current BigQuery Table Schemas (MISMATCHED - Root Cause of Failure)

### market_snapshots (Existing - Wrong Schema)

| Field | Type | Notes |
|-------|------|-------|
| snapshot_date | DATE | Should be `race_date` |
| horse_id | STRING | OK |
| race_id | STRING | OK |
| market_id | STRING | OK |
| runner_id | INTEGER | OK |
| snapshot_time | TIMESTAMP | OK |
| minutes_to_off | INTEGER | NOT in pipeline output |
| back_price | FLOAT | OK |
| lay_price | FLOAT | OK |
| back_volume | FLOAT | OK |
| lay_volume | FLOAT | OK |
| total_matched_volume | FLOAT | OK |
| implied_probability | FLOAT | OK |
| data_quality_flag | STRING | NOT in pipeline output |
| source_file_name | STRING | Pipeline uses `source_file` |
| ingest_timestamp | TIMESTAMP | OK |

**Missing from existing table:** `race_date`, `horse_name`, `ltp`, `track_name`, `race_name`, `market_type`, `country_code`, `event_id`

### horse_race_participations (Existing - Wrong Schema)

| Field | Type | Notes |
|-------|------|-------|
| race_date | DATE | OK |
| horse_id | STRING | OK |
| horse_name | STRING | OK |
| race_id | STRING | OK |
| market_id | STRING | OK |
| runner_id | INTEGER | OK |
| track_name | STRING | OK |
| race_number | INTEGER | NOT in pipeline output |
| race_time | TIME | NOT in pipeline output |
| race_distance_meters | INTEGER | NOT in pipeline output |
| race_class | STRING | NOT in pipeline output |
| race_type | STRING | NOT in pipeline output |
| going | STRING | NOT in pipeline output |
| field_size | INTEGER | NOT in pipeline output |
| saddle_cloth_number | INTEGER | NOT in pipeline output |
| handicap_weight_lbs | INTEGER | NOT in pipeline output |
| jockey_name | STRING | NOT in pipeline output |
| trainer_name | STRING | NOT in pipeline output |
| starting_price | FLOAT | NOT in pipeline output |
| finishing_position | INTEGER | NOT in pipeline output |
| distance_beaten_lengths | FLOAT | NOT in pipeline output |
| was_withdrawn | BOOLEAN | NOT in pipeline output |
| ingest_timestamp | TIMESTAMP | OK |

**Missing from existing table:** `race_name`, `market_type`, `event_id`, `country_code`, `closing_ltp`, `closing_implied_prob`, `source_file`

---

## Environment Variables (if applicable)

| Variable Name | Value | Purpose |
|---------------|-------|---------|
| GOOGLE_APPLICATION_CREDENTIALS | (user default) | GCP authentication |
| GOOGLE_CLOUD_PROJECT | `betfair-data-explorer` | Default project |

---

## Key Commands Reference

```bash
# Run Dataflow pipeline
python3 betfair_dataflow_pipeline.py \
  --runner DataflowRunner \
  --project betfair-data-explorer \
  --region us-central1 \
  --temp_location gs://betfair-data-explorer-temp/tmp/ \
  --staging_location gs://betfair-data-explorer-temp/staging/

# List Dataflow jobs
gcloud dataflow jobs list --project=betfair-data-explorer --region=us-central1

# Describe job details
gcloud dataflow jobs describe JOB_ID --project=betfair-data-explorer --region=us-central1

# List BigQuery tables
bq ls --project_id=betfair-data-explorer chimera_horses_2024

# Show table schema
bq show --schema --format=prettyjson betfair-data-explorer:chimera_horses_2024.market_snapshots
```
