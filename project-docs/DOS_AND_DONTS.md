# CHIMERA Pipeline - Do's and Don'ts

## Based on Phase 1 Lessons Learned (Jan 2026)

---

## DO ✅

### Data Reading & Processing

- [x] **Use Beam's `TextIO.read()` with `compression_type=AUTO`**
  - Automatically handles .bz2 decompression based on file extension
  - Works correctly even when GCS reports Content-Type as `text/plain`
  ```python
  beam.io.ReadFromText(
      'gs://bucket/path/**/*.bz2',
      compression_type=beam.io.filesystem.CompressionTypes.AUTO
  )
  ```

- [x] **Process BASIC tier data** (if that's what you have)
  - BASIC tier contains MCM (Market Change Messages) with pricing data
  - File structure: BASIC/{year}/{month}/{day}/{event_id}/1.{market_id}.bz2
  - Each file is NDJSON (one JSON object per line)

- [x] **Filter for `op=mcm` messages**
  - MCM = Market Change Message (contains price/volume updates)
  - Other ops (connection, status) should be skipped

- [x] **Handle missing horse names gracefully**
  - Names may be in `marketDefinition.runners` or in `rc` directly
  - Fallback to `runner_{id}` when unavailable
  ```python
  if not horse_name and market_context.get('runners'):
      horse_name = market_context['runners'].get(runner_id, f'runner_{runner_id}')
  ```

- [x] **Run Dataflow in parallel** (DataflowRunner)
  - Steps 1-6 scaled well on Cloud Dataflow
  - Processing 12 months of data completed in ~11 hours
  - Service-based shuffle mode works correctly

- [x] **Monitor Dataflow logs** for step durations
  - Console: https://console.cloud.google.com/dataflow/jobs
  - Check stage states to identify bottlenecks
  - Job logs show timing for each transform

- [x] **Keep successful step logic isolated**
  - Separate DoFn classes for each transformation
  - Makes debugging and testing easier
  - Can swap out individual steps without affecting others

- [x] **Version control the pipeline code**
  - Repository: https://github.com/charles-ascot/betfair-exp-v1.1
  - Keep schemas in code alongside transforms
  - Tag releases before major pipeline runs

- [x] **Test on small subset first**
  - Use DirectRunner for local testing
  - Process 1-2 files before full dataset
  - Example: Limit to `BASIC/2024/Jan/1/` only
  ```python
  # For testing, modify GCS_PREFIX:
  GCS_PREFIX = "BASIC/2024/Jan/1"
  ```

- [x] **Create BigQuery tables BEFORE running pipeline**
  - Use explicit schema definitions
  - Match pipeline output schema exactly
  - Use appropriate partitioning and clustering
  ```bash
  bq mk --table \
      --time_partitioning_field race_date \
      --time_partitioning_type DAY \
      project:dataset.table schema.json
  ```

- [x] **Use `CREATE_IF_NEEDED` for safer deployments**
  ```python
  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
  ```

---

## DON'T ❌

### Data Processing

- [ ] **Don't manually call `bz2.decompress()`**
  - Beam handles this automatically with compression_type=AUTO
  - Manual decompression causes "Invalid data stream" errors
  - GCS auto-decompresses when Content-Type is text/plain
  ```python
  # WRONG - Don't do this:
  decompressed = bz2.decompress(file_contents)

  # RIGHT - Let Beam handle it:
  beam.io.ReadFromText('*.bz2', compression_type=beam.io.filesystem.CompressionTypes.AUTO)
  ```

- [ ] **Don't assume fixed schema**
  - Betfair data is heterogeneous (fields may be missing)
  - Always use `.get()` with defaults
  - Handle None values in calculations
  ```python
  # WRONG:
  price = record['ltp']  # May raise KeyError

  # RIGHT:
  price = record.get('ltp')  # Returns None if missing
  ```

- [ ] **Don't try to write directly to BigQuery without matching schemas**
  - `CREATE_NEVER` requires exact schema match
  - Schema mismatch wastes hours of processing time
  - Always verify table schema before running

- [ ] **Don't process one file at a time** (for production)
  - Use glob patterns: `**/*.bz2`
  - Let Beam parallelize file reading
  - DirectRunner is only for testing

- [ ] **Don't ignore missing fields in JSON**
  - They're systematic - some fields only appear in marketDefinition
  - Later messages may lack context from earlier ones
  - Build context from marketDefinition, apply to rc updates

- [ ] **Don't run pipeline without monitoring logs**
  - Check Dataflow console regularly
  - Watch for failed stages
  - Monitor worker utilization

- [ ] **Don't modify working code (steps 1-6) when rebuilding**
  - These transforms work correctly
  - Focus fixes on BigQuery write step
  - If changing transforms, test incrementally

### Infrastructure

- [ ] **Don't use `europe-west2` if it has resource exhaustion**
  - Zone resource errors are common in popular regions
  - Use `us-central1` as fallback
  - Or try `europe-west1` for UK proximity

- [ ] **Don't forget temp_location for DataflowRunner**
  - Required for BigQuery batch writes
  - Must be a GCS path
  ```python
  --temp_location gs://bucket/tmp/
  --staging_location gs://bucket/staging/
  ```

- [ ] **Don't use StandardOptions for project/region in DataflowRunner**
  - Use GoogleCloudOptions instead
  ```python
  # WRONG:
  pipeline_options.view_as(StandardOptions).project = 'my-project'  # No such attribute

  # RIGHT:
  pipeline_options.view_as(GoogleCloudOptions).project = 'my-project'
  ```

### Schema Management

- [ ] **Don't create tables with different schema than pipeline output**
  - This was the root cause of the 11-hour failure
  - Pipeline schema must match table schema exactly
  - Field names, types, and modes must all match

- [ ] **Don't mix data models in same tables**
  - Original tables were for official race results
  - Pipeline outputs Betfair market change data
  - These are different data models - use different tables

---

## Quick Reference: Working vs Broken

### Working Configuration ✅

```python
# Read files
beam.io.ReadFromText(
    'gs://betfair-basic-historic/BASIC/2024/**/*.bz2',
    compression_type=beam.io.filesystem.CompressionTypes.AUTO
)

# Write to BigQuery
beam.io.WriteToBigQuery(
    table='project:dataset.table',
    schema=MATCHING_SCHEMA,  # Must match table exactly
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
)

# Pipeline options for DataflowRunner
google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'betfair-data-explorer'
google_cloud_options.region = 'us-central1'
google_cloud_options.temp_location = 'gs://bucket/tmp/'
```

### Broken Configuration ❌

```python
# Manual decompression - BREAKS
with bz2.open(file_path) as f:
    data = f.read()  # Already decompressed by GCS!

# Wrong schema - BREAKS after hours of processing
beam.io.WriteToBigQuery(
    table='project:dataset.table',  # Has different schema
    schema=PIPELINE_SCHEMA,  # Doesn't match table
    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,  # Will fail
)

# Wrong options class - BREAKS
pipeline_options.view_as(StandardOptions).project = 'x'  # AttributeError
```

---

## Pre-Flight Checklist

Before running the pipeline:

1. [ ] **Verify BigQuery table schemas match pipeline output**
   ```bash
   bq show --schema project:dataset.table
   ```
   Compare with `MARKET_SNAPSHOTS_SCHEMA` and `HORSE_RACE_PARTICIPATIONS_SCHEMA`

2. [ ] **Test locally with DirectRunner first**
   ```bash
   python3 pipeline.py --runner DirectRunner
   ```

3. [ ] **Check temp bucket exists**
   ```bash
   gsutil ls gs://betfair-data-explorer-temp/
   ```

4. [ ] **Verify GCP authentication**
   ```bash
   gcloud auth list
   gcloud config get-value project
   ```

5. [ ] **Check Dataflow API is enabled**
   ```bash
   gcloud services list --enabled | grep dataflow
   ```

6. [ ] **Monitor job after submission**
   - Watch first 10 minutes for startup errors
   - Check stage states are progressing
   - Look for "JOB_STATE_FAILED" early

---

## Recovery Procedures

### If Pipeline Fails at BigQuery Write:

1. **Don't rerun immediately** - Fix schema first
2. **Compare schemas:**
   ```bash
   bq show --schema project:dataset.table
   ```
3. **Recreate tables or change to CREATE_IF_NEEDED**
4. **Clear temp bucket before retry:**
   ```bash
   gsutil -m rm -r gs://bucket/tmp/**
   ```

### If Pipeline Runs Too Long:

1. **Check worker count** in Dataflow console
2. **Consider processing subset** (one month at a time)
3. **Use streaming insert** for incremental updates

### If Zone Resource Exhaustion:

1. **Change region:**
   ```bash
   --region us-central1  # Instead of europe-west2
   ```
2. **Or specify explicit zone:**
   ```bash
   --zone us-central1-a
   ```
