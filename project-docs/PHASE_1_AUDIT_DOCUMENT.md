# CHIMERA Phase 1 Audit Document

## Pipeline Execution Summary

| Metric | Value |
|--------|-------|
| **Job ID** | `2026-01-12_02_30_16-15273293712672769827` |
| **Job Name** | `beamapp-livestreammachine-0112103009-025847-mwyabkad` |
| **Region** | `us-central1` |
| **Start Time** | `2026-01-12T10:30:18.662018Z` |
| **End Time** | `2026-01-12T21:40:33.643336Z` |
| **Total Duration** | **~11 hours 10 minutes** |
| **Final State** | `JOB_STATE_FAILED` |
| **SDK Version** | Apache Beam Python 3.13 SDK 2.70.0 |

---

## WHAT WORKED (Steps 1-6)

### Step 1: Read .bz2 Files from GCS ✅

**Status:** PASSED
**How it works:** Uses Beam's `TextIO.read()` with `compression_type=AUTO`, which automatically detects and decompresses BZ2 files based on file extension.

**Code snippet:**
```python
ndjson_lines = (
    p
    | 'Read NDJSON from BZ2' >> beam.io.ReadFromText(
        f'{GCS_BUCKET}/{GCS_PREFIX}/**/*.bz2',
        compression_type=beam.io.filesystem.CompressionTypes.AUTO,
        skip_header_lines=0
    )
)
```

**Key insight:**
- GCS stores files with `Content-Type: text/plain` even though they're BZ2 compressed
- Beam's AUTO compression handles this correctly by looking at file extension
- **DO NOT** attempt manual `bz2.decompress()` - files are auto-decompressed

**Stage completion time:** `2026-01-12T10:35:57.928Z` (within first 6 minutes)

---

### Step 2: Parse NDJSON ✅

**Status:** PASSED
**How it works:** Simple JSON parsing of each line. Beam receives already-decompressed text lines.

**Code snippet:**
```python
class ParseNDJSON(beam.DoFn):
    """Parse NDJSON lines into JSON objects.

    NOTE: Beam's TextIO.read() with compression_type=AUTO already handles
    decompression of .bz2 files automatically. We receive plain text lines here.
    """

    def process(self, line: str):
        if not line.strip():
            return

        try:
            msg = json.loads(line)
            yield msg
        except json.JSONDecodeError as e:
            logger.warning(f"Skipped malformed JSON: {e}")
```

**Key insight:**
- NDJSON = one JSON object per line
- Error handling for malformed JSON is essential (some files may have corrupted lines)
- Empty lines are silently skipped

---

### Step 3: Parse Betfair MCM Messages ✅

**Status:** PASSED
**How it works:** Filters for `op=mcm` (Market Change Messages) and extracts runner-level data.

**Code snippet:**
```python
class ParseBetfairMessage(beam.DoFn):
    """Parse Betfair market change message (MCM) into normalized records."""

    def process(self, msg: Dict):
        if msg.get('op') != 'mcm':
            return

        timestamp_ms = msg.get('pt', 0)
        try:
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000).isoformat()
        except:
            timestamp = datetime.now().isoformat()

        # Process each market in the message
        for market in msg.get('mc', []):
            market_id = market.get('id')

            # Extract market definition
            market_context = {}
            if 'marketDefinition' in market:
                mdef = market['marketDefinition']
                market_context = {
                    'market_id': market_id,
                    'event_id': mdef.get('eventId'),
                    'event_name': mdef.get('eventName'),
                    'race_name': mdef.get('name'),
                    'market_time': mdef.get('marketTime'),
                    'market_type': mdef.get('marketType'),
                    'country_code': mdef.get('countryCode'),
                    'runners': {r['id']: r['name'] for r in mdef.get('runners', [])},
                }

            # Process runner changes
            for runner in market.get('rc', []):
                runner_id = runner.get('id')
                horse_name = runner.get('name')

                if not horse_name and market_context.get('runners'):
                    horse_name = market_context['runners'].get(runner_id, f'runner_{runner_id}')

                record = {
                    'market_id': market_id,
                    'runner_id': runner_id,
                    'horse_name': horse_name or f'runner_{runner_id}',
                    'timestamp': timestamp,
                    'timestamp_ms': timestamp_ms,
                    'ltp': runner.get('ltp'),
                    'back_price': runner.get('bp'),
                    'lay_price': runner.get('lp'),
                    'back_volume': runner.get('bv'),
                    'lay_volume': runner.get('lv'),
                    'total_matched': runner.get('tv'),
                    'market_context': market_context,
                }

                yield record
```

**Key insights:**
- MCM format: `op` = operation type, `pt` = publish timestamp (ms), `mc` = market changes
- `marketDefinition` contains static info (event name, runners, etc.)
- `rc` = runner changes (price/volume updates)
- Horse names may be in `marketDefinition.runners` or directly in `rc`
- Fallback to `runner_{id}` when name unavailable

---

### Step 4: Normalize to Market Snapshots ✅

**Status:** PASSED
**How it works:** Transforms parsed records into the market_snapshots schema.

**Code snippet:**
```python
class NormalizeToMarketSnapshots(beam.DoFn):
    """Normalize to market_snapshots table schema."""

    def process(self, record: Dict):
        market_context = record.get('market_context', {})
        race_date = market_context.get('market_time', record['timestamp'])[:10]
        horse_id = f"{record['horse_name']}_{record['runner_id']}"

        snapshot = {
            'race_date': race_date,
            'horse_id': horse_id,
            'horse_name': record['horse_name'],
            'race_id': f"{market_context.get('event_id')}_{record['market_id']}",
            'market_id': record['market_id'],
            'runner_id': record['runner_id'],
            'snapshot_time': record['timestamp'],
            'ltp': record['ltp'],
            'back_price': record['back_price'],
            'lay_price': record['lay_price'],
            'back_volume': record['back_volume'],
            'lay_volume': record['lay_volume'],
            'total_matched_volume': record['total_matched'],
            'implied_probability': 1.0 / record['ltp'] if record['ltp'] and record['ltp'] > 0 else None,
            'track_name': market_context.get('event_name', ''),
            'race_name': market_context.get('race_name', ''),
            'market_type': market_context.get('market_type'),
            'country_code': market_context.get('country_code'),
            'event_id': market_context.get('event_id'),
            'ingest_timestamp': datetime.now().isoformat(),
            'source_file': None,
        }

        yield snapshot
```

**Key insights:**
- `horse_id` is composite: `{horse_name}_{runner_id}` for uniqueness
- `race_id` is composite: `{event_id}_{market_id}`
- `implied_probability` = `1.0 / ltp` (only when ltp > 0)
- `race_date` extracted from `market_time` or falls back to timestamp

---

### Step 5: Normalize to Race Participations ✅

**Status:** PASSED
**How it works:** Transforms parsed records into horse_race_participations schema.

**Code snippet:**
```python
class NormalizeToRaceParticipations(beam.DoFn):
    """Normalize to horse_race_participations table schema (aggregate latest)."""

    def process(self, record: Dict):
        market_context = record.get('market_context', {})
        race_date = market_context.get('market_time', record['timestamp'])[:10]
        horse_id = f"{record['horse_name']}_{record['runner_id']}"

        participation = {
            'race_date': race_date,
            'horse_id': horse_id,
            'horse_name': record['horse_name'],
            'race_id': f"{market_context.get('event_id')}_{record['market_id']}",
            'market_id': record['market_id'],
            'runner_id': record['runner_id'],
            'track_name': market_context.get('event_name', ''),
            'race_name': market_context.get('race_name', ''),
            'market_type': market_context.get('market_type'),
            'event_id': market_context.get('event_id'),
            'country_code': market_context.get('country_code'),
            'closing_ltp': record['ltp'],
            'closing_implied_prob': 1.0 / record['ltp'] if record['ltp'] and record['ltp'] > 0 else None,
            'ingest_timestamp': datetime.now().isoformat(),
            'source_file': None,
        }

        yield participation
```

**Key insights:**
- Same ID generation logic as market_snapshots
- `closing_ltp` and `closing_implied_prob` for final prices
- Note: Current implementation outputs ALL records, not just closing prices (may need deduplication)

---

### Step 6: Write Temporary Files to GCS ✅

**Status:** PASSED
**How it works:** BigQuery batch loading writes temp files to GCS before loading.

**Evidence from job stages:**
- `Write Market Snapshots/BigQueryBatchFileLoads/WriteGroupedRecordsToFile` - DONE
- `Write Race Participations/BigQueryBatchFileLoads/WriteGroupedRecordsToFile` - DONE
- All shuffle and grouping stages completed successfully

---

## WHAT FAILED (Steps 7-8)

### Step 7: Write Market Snapshots to BigQuery ❌

**Status:** FAILED
**Error:** `Provided Schema does not match Table betfair-data-explorer:chimera_horses_2024.market_snapshots. Cannot add fields (field: race_date)`

**Failed Stage:** `F95`, `F96` (JOB_STATE_FAILED at `2026-01-12T21:38:46`)

**Root Cause:**
The BigQuery table was created with a **different schema** than what the pipeline outputs.

**Schema Comparison:**

| Pipeline Outputs | Table Has | Status |
|------------------|-----------|--------|
| `race_date` | `snapshot_date` | ❌ MISMATCH |
| `horse_name` | (missing) | ❌ MISSING |
| `ltp` | (missing) | ❌ MISSING |
| `track_name` | (missing) | ❌ MISSING |
| `race_name` | (missing) | ❌ MISSING |
| `market_type` | (missing) | ❌ MISSING |
| `country_code` | (missing) | ❌ MISSING |
| `event_id` | (missing) | ❌ MISSING |
| `source_file` | `source_file_name` | ❌ MISMATCH |
| (missing) | `minutes_to_off` | ❌ EXTRA |
| (missing) | `data_quality_flag` | ❌ EXTRA |

---

### Step 8: Write Race Participations to BigQuery ❌

**Status:** FAILED
**Error:** `Provided Schema does not match Table betfair-data-explorer:chimera_horses_2024.horse_race_participations. Cannot add fields (field: race_name)`

**Failed Stages:** `F91`, `F101` (JOB_STATE_FAILED)

**Root Cause:**
The table was designed for **official race results data** (jockey names, finishing positions, etc.) but pipeline outputs **Betfair market change data**.

**Schema Comparison:**

| Pipeline Outputs | Table Has | Status |
|------------------|-----------|--------|
| `race_name` | (missing) | ❌ MISSING |
| `market_type` | (missing) | ❌ MISSING |
| `event_id` | (missing) | ❌ MISSING |
| `country_code` | (missing) | ❌ MISSING |
| `closing_ltp` | (missing) | ❌ MISSING |
| `closing_implied_prob` | (missing) | ❌ MISSING |
| `source_file` | (missing) | ❌ MISSING |
| (missing) | `race_number` | ❌ EXTRA |
| (missing) | `race_time` | ❌ EXTRA |
| (missing) | `race_distance_meters` | ❌ EXTRA |
| (missing) | `jockey_name` | ❌ EXTRA |
| (missing) | `trainer_name` | ❌ EXTRA |
| (missing) | `finishing_position` | ❌ EXTRA |
| (many more extra fields...) | | |

---

## LESSONS LEARNED

### 1. Schema Management is Critical
- **Always** create BigQuery tables AFTER finalizing pipeline output schema
- Or use `CREATE_IF_NEEDED` with explicit schema in pipeline
- Current setting `CREATE_NEVER` requires pre-existing tables with matching schemas

### 2. Test on Small Dataset First
- 11+ hours of processing wasted due to schema mismatch
- Should have tested with 1-2 files locally first
- Use DirectRunner for small-scale validation

### 3. GCS Auto-Decompression
- Beam's `TextIO.read()` with `compression_type=AUTO` works correctly
- Do NOT manually call `bz2.decompress()` - files are already decompressed
- GCS file metadata says `text/plain` but Beam handles based on extension

### 4. Region Selection
- `europe-west2` had zone resource exhaustion
- `us-central1` worked but is farther from UK data consumers
- Consider `europe-west1` as alternative

### 5. Schema Versioning
- Original tables were designed for official race results
- Pipeline outputs Betfair market change data (different data model)
- Need separate tables or schema alignment

---

## CLEANUP CHECKLIST

- [ ] **Delete failed Dataflow job** (optional - auto-archived)
  ```bash
  # Jobs auto-clean but can cancel if stuck:
  gcloud dataflow jobs cancel 2026-01-12_02_30_16-15273293712672769827 \
    --project=betfair-data-explorer --region=us-central1
  ```

- [ ] **Clear temp bucket** (if needed for reruns)
  ```bash
  gsutil -m rm -r gs://betfair-data-explorer-temp/tmp/**
  gsutil -m rm -r gs://betfair-data-explorer-temp/staging/**
  ```

- [ ] **Recreate BigQuery tables with correct schema**
  ```bash
  # Delete old tables
  bq rm -f -t betfair-data-explorer:chimera_horses_2024.market_snapshots
  bq rm -f -t betfair-data-explorer:chimera_horses_2024.horse_race_participations

  # Recreate with pipeline schema (see PIPELINE_CODE.md)
  ```

- [ ] **Archive documentation** - This file captures what happened

- [ ] **Update pipeline to use CREATE_IF_NEEDED** (alternative fix)
  ```python
  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
  ```

---

## NEXT STEPS

1. **Option A: Recreate tables with pipeline schema**
   - Drop existing `market_snapshots` and `horse_race_participations`
   - Create new tables matching `MARKET_SNAPSHOTS_SCHEMA` and `HORSE_RACE_PARTICIPATIONS_SCHEMA`
   - Re-run pipeline

2. **Option B: Modify pipeline to match existing tables**
   - Would require removing fields from output
   - Would lose valuable data (horse_name, ltp, track_name, etc.)
   - NOT RECOMMENDED

3. **Option C: Use different table names**
   - Keep existing tables for future official results data
   - Create new tables: `mcm_market_snapshots`, `mcm_race_participations`
   - Update pipeline to target new tables

**Recommended:** Option A or Option C
