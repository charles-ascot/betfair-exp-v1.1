# Betfair Dataflow Pipeline - Comprehensive Reference Document

**File:** `betfair_dataflow_pipeline.py`
**Location:** `/Users/livestreammachine/projects/betfair-exp-v1.1/project-docs/betfair_dataflow_pipeline.py`
**Total Lines:** 341
**Purpose:** Process Betfair 2024 BASIC tier data and write to BigQuery

---

## 1. COMPLETE CODE LISTING

```python
#!/usr/bin/env python3
"""
Apache Beam/Dataflow Pipeline: Betfair 2024 Data → BigQuery

PIPELINE (FIXED VERSION):
  GCS bucket (.bz2 files)
    → TextIO.read() with compression_type=AUTO (auto-decompresses)
    → Parse NDJSON
    → Parse Betfair market change messages
    → Normalize to two schemas
    → Write to BigQuery tables

KEY FIX: Uses Beam's TextIO.read() which automatically decompresses .bz2 files
based on file extension. No manual bz2.decompress() needed.

USAGE (Cloud Dataflow - Recommended):
  python3 betfair_dataflow_pipeline.py \
    --runner DataflowRunner \
    --project betfair-data-explorer \
    --region europe-west2 \
    --temp_location gs://betfair-data-explorer-temp/tmp/ \
    --staging_location gs://betfair-data-explorer-temp/staging/

Monitor at:
  https://console.cloud.google.com/dataflow/jobs?project=betfair-data-explorer

Author: CHIMERA AI System
Fixed by: Gemini AI + Claude
"""

import argparse
import json
import logging
from datetime import datetime
from typing import Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

# ============================================================================
# CONFIGURATION
# ============================================================================

GCS_BUCKET = "gs://betfair-basic-historic"
GCS_PREFIX = "BASIC/2024"
BQ_PROJECT = "betfair-data-explorer"
BQ_DATASET = "chimera_horses_2024"

# BigQuery table schemas
MARKET_SNAPSHOTS_SCHEMA = {
    'fields': [
        {'name': 'race_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'horse_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'horse_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'race_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'market_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'runner_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'snapshot_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'ltp', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'back_price', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'lay_price', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'back_volume', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'lay_volume', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'total_matched_volume', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'implied_probability', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'track_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'race_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'market_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'event_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ingest_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'source_file', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}

HORSE_RACE_PARTICIPATIONS_SCHEMA = {
    'fields': [
        {'name': 'race_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'horse_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'horse_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'race_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'market_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'runner_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'track_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'race_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'market_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'event_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'closing_ltp', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'closing_implied_prob', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'ingest_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'source_file', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# PIPELINE TRANSFORMS
# ============================================================================

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


# ============================================================================
# PIPELINE DEFINITION
# ============================================================================

def run(argv=None):
    """Build and run the Dataflow pipeline."""

    parser = argparse.ArgumentParser(description='Betfair 2024 Dataflow Pipeline')
    parser.add_argument('--runner', default='DirectRunner',
                        help='Apache Beam runner (DirectRunner or DataflowRunner)')
    parser.add_argument('--project', default='betfair-data-explorer',
                        help='Google Cloud project ID')
    parser.add_argument('--region', default='europe-west1',
                        help='Dataflow region')
    parser.add_argument('--temp_location',
                        help='Temporary location for Dataflow (required for DataflowRunner)')
    parser.add_argument('--staging_location',
                        help='Staging location for Dataflow (required for DataflowRunner)')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).runner = known_args.runner

    if known_args.runner == 'DataflowRunner':
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project
        google_cloud_options.region = known_args.region
        if known_args.temp_location:
            google_cloud_options.temp_location = known_args.temp_location
        if known_args.staging_location:
            google_cloud_options.staging_location = known_args.staging_location

    logger.info("=" * 80)
    logger.info("BETFAIR 2024 DATAFLOW PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Runner: {known_args.runner}")
    logger.info(f"Project: {known_args.project}")
    logger.info(f"Region: {known_args.region}")
    logger.info("=" * 80)

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Read NDJSON from .bz2 files in bucket
        # Beam's TextIO.read() automatically decompresses based on file extension (.bz2)
        ndjson_lines = (
            p
            | 'Read NDJSON from BZ2' >> beam.io.ReadFromText(
                f'{GCS_BUCKET}/{GCS_PREFIX}/**/*.bz2',
                compression_type=beam.io.filesystem.CompressionTypes.AUTO,
                skip_header_lines=0
            )
        )

        # Step 2: Parse NDJSON lines into JSON objects
        parsed_messages = (
            ndjson_lines
            | 'Parse NDJSON' >> beam.ParDo(ParseNDJSON())
        )

        # Step 3: Parse Betfair market change messages
        records = (
            parsed_messages
            | 'Parse Betfair Messages' >> beam.ParDo(ParseBetfairMessage())
        )

        # Step 4a: Normalize to market_snapshots
        market_snapshots = (
            records
            | 'Normalize Market Snapshots' >> beam.ParDo(NormalizeToMarketSnapshots())
            | 'Write Market Snapshots' >> beam.io.WriteToBigQuery(
                table=f'{known_args.project}:{BQ_DATASET}.market_snapshots',
                schema=MARKET_SNAPSHOTS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

        # Step 4b: Normalize to horse_race_participations
        race_participations = (
            records
            | 'Normalize Race Participations' >> beam.ParDo(NormalizeToRaceParticipations())
            | 'Write Race Participations' >> beam.io.WriteToBigQuery(
                table=f'{known_args.project}:{BQ_DATASET}.horse_race_participations',
                schema=HORSE_RACE_PARTICIPATIONS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

    logger.info("=" * 80)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 80)


if __name__ == '__main__':
    run()
```

---

## 2. ALL VARIABLES & PARAMETERS TABLE

### Hardcoded Configuration Variables

| Variable Name | Value | Type | Line Number | Purpose |
|---------------|-------|------|-------------|---------|
| `GCS_BUCKET` | `"gs://betfair-basic-historic"` | STRING | 46 | Source GCS bucket URL |
| `GCS_PREFIX` | `"BASIC/2024"` | STRING | 47 | Path prefix within bucket |
| `BQ_PROJECT` | `"betfair-data-explorer"` | STRING | 48 | BigQuery project ID |
| `BQ_DATASET` | `"chimera_horses_2024"` | STRING | 49 | BigQuery dataset name |

### Command-Line Arguments (with defaults)

| Argument | Default Value | Type | Line Number | Purpose |
|----------|---------------|------|-------------|---------|
| `--runner` | `'DirectRunner'` | STRING | 253 | Apache Beam runner type |
| `--project` | `'betfair-data-explorer'` | STRING | 255 | Google Cloud project ID |
| `--region` | `'europe-west1'` | STRING | 257 | Dataflow execution region |
| `--temp_location` | None (required for Dataflow) | STRING | 259 | GCS path for temp files |
| `--staging_location` | None (required for Dataflow) | STRING | 261 | GCS path for staging files |

### Schema Variables

| Variable Name | Type | Line Number | Purpose |
|---------------|------|-------------|---------|
| `MARKET_SNAPSHOTS_SCHEMA` | DICT | 52-76 | BigQuery schema for market_snapshots table |
| `HORSE_RACE_PARTICIPATIONS_SCHEMA` | DICT | 78-96 | BigQuery schema for horse_race_participations table |

### Derived/Constructed Values

| Expression | Constructed Value | Line Number | Purpose |
|------------|-------------------|-------------|---------|
| `f'{GCS_BUCKET}/{GCS_PREFIX}/**/*.bz2'` | `gs://betfair-basic-historic/BASIC/2024/**/*.bz2` | 292 | Input file glob pattern |
| `f'{known_args.project}:{BQ_DATASET}.market_snapshots'` | `betfair-data-explorer:chimera_horses_2024.market_snapshots` | 315 | Output table 1 |
| `f'{known_args.project}:{BQ_DATASET}.horse_race_participations'` | `betfair-data-explorer:chimera_horses_2024.horse_race_participations` | 327 | Output table 2 |

### Beam/IO Configuration Values

| Setting | Value | Line Number | Purpose |
|---------|-------|-------------|---------|
| `compression_type` | `beam.io.filesystem.CompressionTypes.AUTO` | 293 | Auto-detect compression |
| `skip_header_lines` | `0` | 294 | No header lines to skip |
| `write_disposition` | `beam.io.BigQueryDisposition.WRITE_APPEND` | 317, 329 | Append to existing data |
| `create_disposition` | `beam.io.BigQueryDisposition.CREATE_NEVER` | 318, 330 | Do not create tables |

---

## 3. GCP SERVICES & CONFIGURATION

### Google Cloud Services Used

**Service: Apache Dataflow**
- Runner: `DataflowRunner` (when specified via `--runner`)
- Default Runner: `DirectRunner` (local execution)
- Region: `europe-west1` (default, line 257)
- Region in docstring: `europe-west2` (line 20)

**Service: Cloud Storage (GCS)**
- Buckets referenced:
  - `gs://betfair-basic-historic` (source data, line 46)
  - `gs://betfair-data-explorer-temp` (temp/staging, docstring lines 21-22)
- Paths:
  - Input: `BASIC/2024/**/*.bz2`
  - Temp: `/tmp/` subdirectory
  - Staging: `/staging/` subdirectory

**Service: BigQuery**
- Project: `betfair-data-explorer` (line 48)
- Dataset: `chimera_horses_2024` (line 49)
- Tables:
  - `market_snapshots`
  - `horse_race_participations`

### Authentication & Credentials

- Authentication method: Not explicitly defined in code
- Relies on: Default application credentials (ADC)
- Service account: Not specified in code (uses environment default)
- Permissions required:
  - `storage.objects.get` (read from GCS)
  - `storage.objects.list` (list files in GCS)
  - `bigquery.tables.updateData` (write to BigQuery)
  - `dataflow.jobs.create` (create Dataflow jobs)

---

## 4. SOURCE DATA SPECIFICATION

### Input Source

| Property | Value | Line Number |
|----------|-------|-------------|
| Bucket name | `gs://betfair-basic-historic` | 46 |
| Path prefix | `BASIC/2024` | 47 |
| Full pattern | `gs://betfair-basic-historic/BASIC/2024/**/*.bz2` | 292 |
| File type | `.bz2` (BZ2 compressed) | 292 |
| Recursive | Yes (`**` glob pattern) | 292 |
| Date range specified | No explicit date filtering | - |
| Data tier | BASIC | 47 |
| Data year | 2024 | 47 |

### Output Destination

| Property | Value | Line Number |
|----------|-------|-------------|
| Destination type | BigQuery | 314-319, 326-331 |
| Project | `betfair-data-explorer` (from `--project`) | 315, 327 |
| Dataset | `chimera_horses_2024` | 315, 327 |
| Table 1 | `market_snapshots` | 315 |
| Table 2 | `horse_race_participations` | 327 |
| Write mode | APPEND | 317, 329 |
| Create mode | NEVER (tables must exist) | 318, 330 |

---

## 5. PIPELINE STAGES

### Stage 1: Read NDJSON from BZ2

| Property | Value |
|----------|-------|
| Stage Name | `'Read NDJSON from BZ2'` |
| Code Location | Lines 289-296 |
| Input | GCS files matching `gs://betfair-basic-historic/BASIC/2024/**/*.bz2` |
| Processing | `beam.io.ReadFromText()` with auto-decompression |
| Output | Raw text lines (one per NDJSON record) |
| Compression Handling | `CompressionTypes.AUTO` |

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

### Stage 2: Parse NDJSON

| Property | Value |
|----------|-------|
| Stage Name | `'Parse NDJSON'` |
| Code Location | Lines 299-302 |
| Transform Class | `ParseNDJSON` (lines 105-120) |
| Input | Raw text lines |
| Processing | `json.loads()` on each line |
| Output | Python dict objects |
| Error Handling | Logs warning, skips malformed JSON |

```python
parsed_messages = (
    ndjson_lines
    | 'Parse NDJSON' >> beam.ParDo(ParseNDJSON())
)
```

### Stage 3: Parse Betfair Messages

| Property | Value |
|----------|-------|
| Stage Name | `'Parse Betfair Messages'` |
| Code Location | Lines 304-308 |
| Transform Class | `ParseBetfairMessage` (lines 123-178) |
| Input | Parsed JSON dicts |
| Processing | Filters `op=mcm`, extracts market/runner data |
| Output | Normalized record dicts with pricing data |
| Filter | Only processes messages where `msg.get('op') == 'mcm'` |

```python
records = (
    parsed_messages
    | 'Parse Betfair Messages' >> beam.ParDo(ParseBetfairMessage())
)
```

### Stage 4a: Normalize Market Snapshots

| Property | Value |
|----------|-------|
| Stage Name | `'Normalize Market Snapshots'` |
| Code Location | Lines 310-320 |
| Transform Class | `NormalizeToMarketSnapshots` (lines 181-213) |
| Input | Parsed Betfair records |
| Processing | Maps fields to `market_snapshots` schema |
| Output | BigQuery-compatible dicts |

```python
market_snapshots = (
    records
    | 'Normalize Market Snapshots' >> beam.ParDo(NormalizeToMarketSnapshots())
    | 'Write Market Snapshots' >> beam.io.WriteToBigQuery(
        table=f'{known_args.project}:{BQ_DATASET}.market_snapshots',
        schema=MARKET_SNAPSHOTS_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
    )
)
```

### Stage 4b: Normalize Race Participations

| Property | Value |
|----------|-------|
| Stage Name | `'Normalize Race Participations'` |
| Code Location | Lines 322-332 |
| Transform Class | `NormalizeToRaceParticipations` (lines 216-242) |
| Input | Parsed Betfair records (same as 4a) |
| Processing | Maps fields to `horse_race_participations` schema |
| Output | BigQuery-compatible dicts |

```python
race_participations = (
    records
    | 'Normalize Race Participations' >> beam.ParDo(NormalizeToRaceParticipations())
    | 'Write Race Participations' >> beam.io.WriteToBigQuery(
        table=f'{known_args.project}:{BQ_DATASET}.horse_race_participations',
        schema=HORSE_RACE_PARTICIPATIONS_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
    )
)
```

---

## 6. DATA FLOW DIAGRAM (Text Format)

```
Source: gs://betfair-basic-historic/BASIC/2024/**/*.bz2
    │
    │ [BZ2 compressed NDJSON files]
    ↓
┌─────────────────────────────────────────────────────────┐
│ Stage 1: Read NDJSON from BZ2                           │
│ - beam.io.ReadFromText()                                │
│ - compression_type=AUTO                                 │
│ - Lines 289-296                                         │
└─────────────────────────────────────────────────────────┘
    │
    │ [Raw text lines - one JSON object per line]
    ↓
┌─────────────────────────────────────────────────────────┐
│ Stage 2: Parse NDJSON                                   │
│ - ParseNDJSON DoFn                                      │
│ - json.loads() each line                                │
│ - Lines 299-302                                         │
└─────────────────────────────────────────────────────────┘
    │
    │ [Python dict objects]
    ↓
┌─────────────────────────────────────────────────────────┐
│ Stage 3: Parse Betfair Messages                         │
│ - ParseBetfairMessage DoFn                              │
│ - Filter: op == 'mcm' only                              │
│ - Extract: market_id, runner_id, prices, volumes        │
│ - Lines 304-308                                         │
└─────────────────────────────────────────────────────────┘
    │
    │ [Normalized record dicts with market_context]
    ├──────────────────────────────────────┐
    ↓                                      ↓
┌──────────────────────────────┐  ┌──────────────────────────────┐
│ Stage 4a: Normalize Market   │  │ Stage 4b: Normalize Race     │
│           Snapshots          │  │           Participations     │
│ - NormalizeToMarketSnapshots │  │ - NormalizeToRaceParticip... │
│ - Lines 310-313              │  │ - Lines 322-325              │
└──────────────────────────────┘  └──────────────────────────────┘
    │                                      │
    ↓                                      ↓
┌──────────────────────────────┐  ┌──────────────────────────────┐
│ Write Market Snapshots       │  │ Write Race Participations    │
│ - WriteToBigQuery            │  │ - WriteToBigQuery            │
│ - Lines 314-320              │  │ - Lines 326-332              │
└──────────────────────────────┘  └──────────────────────────────┘
    │                                      │
    ↓                                      ↓
┌──────────────────────────────┐  ┌──────────────────────────────┐
│ betfair-data-explorer:       │  │ betfair-data-explorer:       │
│ chimera_horses_2024.         │  │ chimera_horses_2024.         │
│ market_snapshots             │  │ horse_race_participations    │
└──────────────────────────────┘  └──────────────────────────────┘
```

---

## 7. BEAM/DATAFLOW CONFIGURATION

### Pipeline Options (from code)

```
Pipeline Options:
- runner: DirectRunner (default, line 253) or DataflowRunner (via --runner)
- project: betfair-data-explorer (default, line 255)
- region: europe-west1 (default, line 257)
- temp_location: None (default, line 259) - required for DataflowRunner
- staging_location: None (default, line 261) - required for DataflowRunner
```

### Docstring Usage Example (lines 16-22)

```
Documented Dataflow Settings:
- runner: DataflowRunner
- project: betfair-data-explorer
- region: europe-west2  <-- NOTE: Different from code default (europe-west1)
- temp_location: gs://betfair-data-explorer-temp/tmp/
- staging_location: gs://betfair-data-explorer-temp/staging/
```

### BigQuery Write Configuration

```
WriteToBigQuery Settings:
- write_disposition: WRITE_APPEND (lines 317, 329)
- create_disposition: CREATE_NEVER (lines 318, 330)
- schema: Provided via MARKET_SNAPSHOTS_SCHEMA / HORSE_RACE_PARTICIPATIONS_SCHEMA
```

---

## 8. FILE/PATH SPECIFICATIONS

| Purpose | Full Path | Line Number |
|---------|-----------|-------------|
| Input Pattern | `gs://betfair-basic-historic/BASIC/2024/**/*.bz2` | 292 |
| Temp Location (docstring) | `gs://betfair-data-explorer-temp/tmp/` | 21 |
| Staging Location (docstring) | `gs://betfair-data-explorer-temp/staging/` | 22 |
| Output Table 1 | `betfair-data-explorer:chimera_horses_2024.market_snapshots` | 315 |
| Output Table 2 | `betfair-data-explorer:chimera_horses_2024.horse_race_participations` | 327 |

### Path Construction Details

| Component | Value | Source |
|-----------|-------|--------|
| GCS_BUCKET | `gs://betfair-basic-historic` | Line 46 (hardcoded) |
| GCS_PREFIX | `BASIC/2024` | Line 47 (hardcoded) |
| Glob Pattern | `**/*.bz2` | Line 292 (hardcoded in f-string) |
| Full Input | `{GCS_BUCKET}/{GCS_PREFIX}/**/*.bz2` | Line 292 |

---

## 9. FILTERING & SELECTION LOGIC

### Date Selection

| Question | Answer | Code Reference |
|----------|--------|----------------|
| How is the date/date range specified in code? | Via `GCS_PREFIX = "BASIC/2024"` | Line 47 |
| Is it hardcoded? | Yes | Line 47 |
| Is it a parameter? | No | - |
| What format is it in? | Path component: `BASIC/{YEAR}` | Line 47 |
| Does it filter specific months/days? | No - `**` matches all subfolders | Line 292 |

**Actual code:**
```python
GCS_PREFIX = "BASIC/2024"  # Line 47
```

```python
f'{GCS_BUCKET}/{GCS_PREFIX}/**/*.bz2'  # Line 292
# Expands to: gs://betfair-basic-historic/BASIC/2024/**/*.bz2
```

### File Selection

| Question | Answer | Code Reference |
|----------|--------|----------------|
| Pattern used | `**/*.bz2` | Line 292 |
| Does it filter by filename? | Only by extension (`.bz2`) | Line 292 |
| Does it filter M vs E files? | No | - |
| Does it filter by tier? | Yes - path includes `BASIC` | Line 47 |

**Actual glob pattern:**
```python
f'{GCS_BUCKET}/{GCS_PREFIX}/**/*.bz2'
# Result: gs://betfair-basic-historic/BASIC/2024/**/*.bz2
```

### Record Selection

| Filter | Location | Code |
|--------|----------|------|
| Operation type filter | Line 127 | `if msg.get('op') != 'mcm': return` |
| Empty line filter | Line 113 | `if not line.strip(): return` |
| Malformed JSON filter | Lines 116-120 | `try/except json.JSONDecodeError` |

**MCM Filter Code (lines 126-128):**
```python
def process(self, msg: Dict):
    if msg.get('op') != 'mcm':
        return
```

**Empty Line Filter (lines 112-114):**
```python
def process(self, line: str):
    if not line.strip():
        return
```

---

## 10. DEPENDENCIES & IMPORTS

### Import Statements (lines 31-40)

```python
import argparse
import json
import logging
from datetime import datetime
from typing import Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
```

### Standard Library Modules

| Module | Purpose | Line |
|--------|---------|------|
| `argparse` | Command-line argument parsing | 31 |
| `json` | JSON parsing | 32 |
| `logging` | Logging framework | 33 |
| `datetime` | Timestamp handling | 34 |
| `typing.Dict` | Type hints | 35 |

### Third-Party Modules

| Module | Import | Purpose | Line |
|--------|--------|---------|------|
| `apache_beam` | `beam` | Pipeline framework | 37 |
| `apache_beam.options.pipeline_options` | `PipelineOptions` | Base options class | 38 |
| `apache_beam.options.pipeline_options` | `StandardOptions` | Runner options | 39 |
| `apache_beam.options.pipeline_options` | `GoogleCloudOptions` | GCP-specific options | 40 |

### Implicit Dependencies (not imported but used)

| Dependency | Used Via | Purpose |
|------------|----------|---------|
| `beam.io.ReadFromText` | `beam.io` | Read from GCS |
| `beam.io.WriteToBigQuery` | `beam.io` | Write to BigQuery |
| `beam.io.filesystem.CompressionTypes` | `beam.io.filesystem` | Compression handling |
| `beam.io.BigQueryDisposition` | `beam.io` | Write/create modes |
| `beam.DoFn` | `beam` | Transform base class |
| `beam.ParDo` | `beam` | Parallel processing |
| `beam.Pipeline` | `beam` | Pipeline container |

---

## SCHEMA DEFINITIONS

### MARKET_SNAPSHOTS_SCHEMA (Lines 52-76)

| Field Name | Type | Mode | Line |
|------------|------|------|------|
| race_date | DATE | NULLABLE | 54 |
| horse_id | STRING | NULLABLE | 55 |
| horse_name | STRING | NULLABLE | 56 |
| race_id | STRING | NULLABLE | 57 |
| market_id | STRING | NULLABLE | 58 |
| runner_id | INTEGER | NULLABLE | 59 |
| snapshot_time | TIMESTAMP | NULLABLE | 60 |
| ltp | FLOAT64 | NULLABLE | 61 |
| back_price | FLOAT64 | NULLABLE | 62 |
| lay_price | FLOAT64 | NULLABLE | 63 |
| back_volume | FLOAT64 | NULLABLE | 64 |
| lay_volume | FLOAT64 | NULLABLE | 65 |
| total_matched_volume | FLOAT64 | NULLABLE | 66 |
| implied_probability | FLOAT64 | NULLABLE | 67 |
| track_name | STRING | NULLABLE | 68 |
| race_name | STRING | NULLABLE | 69 |
| market_type | STRING | NULLABLE | 70 |
| country_code | STRING | NULLABLE | 71 |
| event_id | STRING | NULLABLE | 72 |
| ingest_timestamp | TIMESTAMP | NULLABLE | 73 |
| source_file | STRING | NULLABLE | 74 |

### HORSE_RACE_PARTICIPATIONS_SCHEMA (Lines 78-96)

| Field Name | Type | Mode | Line |
|------------|------|------|------|
| race_date | DATE | NULLABLE | 80 |
| horse_id | STRING | NULLABLE | 81 |
| horse_name | STRING | NULLABLE | 82 |
| race_id | STRING | NULLABLE | 83 |
| market_id | STRING | NULLABLE | 84 |
| runner_id | INTEGER | NULLABLE | 85 |
| track_name | STRING | NULLABLE | 86 |
| race_name | STRING | NULLABLE | 87 |
| market_type | STRING | NULLABLE | 88 |
| event_id | STRING | NULLABLE | 89 |
| country_code | STRING | NULLABLE | 90 |
| closing_ltp | FLOAT64 | NULLABLE | 91 |
| closing_implied_prob | FLOAT64 | NULLABLE | 92 |
| ingest_timestamp | TIMESTAMP | NULLABLE | 93 |
| source_file | STRING | NULLABLE | 94 |

---

## CLASS DEFINITIONS

### Class: ParseNDJSON (Lines 105-120)

| Property | Value |
|----------|-------|
| Parent Class | `beam.DoFn` |
| Method | `process(self, line: str)` |
| Input Type | `str` |
| Output Type | `dict` (via yield) |
| Error Handling | Logs warning, returns nothing on error |

### Class: ParseBetfairMessage (Lines 123-178)

| Property | Value |
|----------|-------|
| Parent Class | `beam.DoFn` |
| Method | `process(self, msg: Dict)` |
| Input Type | `Dict` |
| Output Type | `dict` (via yield) |
| Filter | `msg.get('op') != 'mcm'` returns early |

### Class: NormalizeToMarketSnapshots (Lines 181-213)

| Property | Value |
|----------|-------|
| Parent Class | `beam.DoFn` |
| Method | `process(self, record: Dict)` |
| Input Type | `Dict` |
| Output Type | `dict` (via yield) |

### Class: NormalizeToRaceParticipations (Lines 216-242)

| Property | Value |
|----------|-------|
| Parent Class | `beam.DoFn` |
| Method | `process(self, record: Dict)` |
| Input Type | `Dict` |
| Output Type | `dict` (via yield) |

---

## FUNCTION DEFINITIONS

### Function: run (Lines 249-336)

| Property | Value |
|----------|-------|
| Parameters | `argv=None` |
| Return Type | None (implicit) |
| Purpose | Main entry point - builds and runs pipeline |

---

## END OF DOCUMENT
