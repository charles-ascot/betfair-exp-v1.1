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
