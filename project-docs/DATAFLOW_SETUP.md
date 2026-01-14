# Betfair Dataflow Pipeline Setup & Deployment

## What This Does

✅ **Reads** .bz2 files from GCS bucket  
✅ **Automatically filters out E-type files** (ignores `{EventID}_{EventID}.bz2`)  
✅ **Keeps only M-type files** (`{EventID}_{MarketID}.bz2`)  
✅ **Decompresses** NDJSON data  
✅ **Normalizes** to BigQuery schema  
✅ **Writes** to `market_snapshots` and `horse_race_participations` tables  

---

## Step 1: Install Dependencies

```bash
pip install apache-beam[gcp]
```

Verify:
```bash
python3 -c "import apache_beam; print('Apache Beam installed:', apache_beam.__version__)"
```

---

## Step 2: Authenticate

```bash
gcloud auth application-default login
```

This enables the pipeline to access GCS and BigQuery.

---

## Step 3: Test Locally (DirectRunner)

**Run a quick local test** before deploying to Dataflow (saves cost):

```bash
python3 betfair_dataflow_pipeline.py \
  --runner DirectRunner \
  --project betfair-data-explorer
```

**Expected output:**
```
================================================================================
BETFAIR 2024 DATAFLOW PIPELINE
================================================================================
Runner: DirectRunner
Project: betfair-data-explorer
Region: europe-west1
================================================================================
INFO - PROCESSING M-type file: BASIC_2024_Jan_1_33123456_1_226402308.bz2
INFO - PROCESSING M-type file: BASIC_2024_Jan_1_33123456_1_226402309.bz2
INFO - SKIPPING E-type file: BASIC_2024_Jan_1_33123456_33123456.bz2
...
================================================================================
PIPELINE COMPLETE
================================================================================
```

⏱ Local test takes **30 minutes to 1 hour** for a small subset.

---

## Step 4: Deploy to Dataflow (Production Run)

Once local test succeeds, deploy to **Cloud Dataflow** for full 2024 dataset:

```bash
python3 betfair_dataflow_pipeline.py \
  --runner DataflowRunner \
  --project betfair-data-explorer \
  --region europe-west1 \
  --temp_location gs://betfair-data-explorer-temp/tmp/ \
  --staging_location gs://betfair-data-explorer-temp/staging/
```

**What happens:**
- Pipeline runs on Google's infrastructure (managed, scalable)
- You can monitor in [Dataflow Console](https://console.cloud.google.com/dataflow)
- Automatically processes all 2024 files in parallel
- ⏱ Takes 4-12 hours depending on file volume

---

## How E-File Filtering Works

The pipeline automatically skips Event files by checking filename pattern:

```
FILENAME FORMAT: BASIC_YYYY_Mon_Day_{EventID}_{ID}.bz2

E-type files (SKIP):  EventID == ID
  Example: BASIC_2024_Jan_1_33123456_33123456.bz2

M-type files (KEEP):  EventID != ID
  Example: BASIC_2024_Jan_1_33123456_1_226402308.bz2
```

When you run the pipeline, you'll see:
```
INFO - SKIPPING E-type file: ...33123456_33123456.bz2
INFO - PROCESSING M-type file: ...33123456_1_226402308.bz2
```

**No manual deletion needed** - the pipeline just ignores E-type files.

---

## Monitor Progress

### Local Test:
```bash
tail -f /tmp/betfair_pipeline.log
```

### Dataflow Cloud:
Go to: https://console.cloud.google.com/dataflow/jobs?project=betfair-data-explorer

View:
- Job status (Running → Succeeded)
- Worker progress
- Data volume processed
- Estimated completion time

---

## Verify Results

After pipeline completes, check BigQuery:

```bash
bq query --use_legacy_sql=false "
  SELECT 
    'market_snapshots' as table_name, COUNT(*) as row_count 
  FROM \`betfair-data-explorer.chimera_horses_2024.market_snapshots\`
  UNION ALL
  SELECT 
    'horse_race_participations', COUNT(*) 
  FROM \`betfair-data-explorer.chimera_horses_2024.horse_race_participations\`
"
```

Expected output:
```
+----------------------------+-----------+
| table_name                 | row_count |
+----------------------------+-----------+
| market_snapshots           | ~50M      |
| horse_race_participations  | ~1.5M     |
+----------------------------+-----------+
```

---

## Troubleshooting

### Error: "Permission denied" on GCS
```bash
gcloud auth application-default login
# Sign in with account that has access to betfair-basic-historic bucket
```

### Error: "Table not found" in BigQuery
Verify tables exist:
```bash
bq ls betfair-data-explorer:chimera_horses_2024
```

### Local test is slow
- Normal for first 30 min - Beam scans all GCS files
- E-type files are being skipped (logged as INFO)
- Let it run, or check CloudShell for faster execution

### Dataflow job fails
- Check job logs: [Dataflow Console](https://console.cloud.google.com/dataflow)
- Common issue: insufficient permissions (re-run `gcloud auth`)
- Restart: just run the command again (Dataflow resumes where it left off)

---

## Next Steps

Once data loads successfully:

1. **Run CHIMERA advisor test**
   - Tell me a historical race to analyze
   - I'll query the BigQuery data
   - Walk through my prediction process

2. **Verify data quality**
   - Check sample races
   - Validate horse names, odds, dates

3. **Tune parameters**
   - Adjust sampling interval
   - Modify normalization logic if needed

---

## Command Reference

| Command | Purpose |
|---------|---------|
| `python3 betfair_dataflow_pipeline.py --runner DirectRunner` | Test locally |
| `python3 betfair_dataflow_pipeline.py --runner DataflowRunner --region europe-west1 --temp_location gs://...` | Deploy to cloud |
| `bq ls betfair-data-explorer:chimera_horses_2024` | List tables |
| `bq query "SELECT COUNT(*) FROM betfair-data-explorer.chimera_horses_2024.market_snapshots"` | Check row count |

---

**Ready to test? Run Step 1-3 and let me know the results.**
