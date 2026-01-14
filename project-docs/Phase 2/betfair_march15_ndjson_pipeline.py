#!/usr/bin/env python3
"""
Apache Beam/Dataflow Pipeline: Betfair ADVANCED March 15 2016 → GCS NDJSON
PIPELINE: Extract & Normalize to NDJSON (no BigQuery write yet)

GCS bucket (.bz2 files)
    → TextIO.read() with compression_type=AUTO (auto-decompresses)
    → Parse NDJSON
    → Parse Betfair market change messages
    → Flatten to normalized records
    → Write to GCS as NDJSON (one record per line)

This pipeline focuses on understanding data structure without BigQuery schema constraints.

USAGE (Cloud Dataflow):
python3 betfair_march15_ndjson_pipeline.py \
    --runner DataflowRunner \
    --project betfair-data-explorer \
    --region us-central1 \
    --temp_location gs://betfair-data-explorer-temp/tmp/ \
    --staging_location gs://betfair-data-explorer-temp/staging/ \
    --output_bucket gs://betfair-chimera-march15-normalized

Monitor at:
https://console.cloud.google.com/dataflow/jobs?project=betfair-data-explorer

USAGE (Local Testing with DirectRunner):
python3 betfair_march15_ndjson_pipeline.py \
    --runner DirectRunner \
    --output_bucket /tmp/betfair_output
"""

import argparse
import json
import logging
from datetime import datetime
from typing import Dict, Any
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

# ============================================================================
# CONFIGURATION
# ============================================================================

GCS_BUCKET = "gs://betfair-basic-historic"
GCS_PREFIX = "ADVANCED/2016/Mar/15"  # March 15 2016 ADVANCED tier data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# PIPELINE TRANSFORMS
# ============================================================================


class ParseNDJSON(beam.DoFn):
    """Parse NDJSON lines into JSON objects.
    
    Beam's TextIO.read() with compression_type=AUTO automatically handles
    decompression of .bz2 files. We receive plain text lines here.
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
    """Parse Betfair market change message (MCM) into normalized records.
    
    Extracts:
    - Market metadata from marketDefinition
    - Runner-level pricing from rc (runner changes)
    - Combines them for each runner snapshot
    """
    
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
                    'venue': mdef.get('venue'),
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
                
                # Fallback: get horse name from marketDefinition.runners
                if not horse_name and market_context.get('runners'):
                    horse_name = market_context['runners'].get(runner_id, f'runner_{runner_id}')
                
                # Build normalized record with ALL available fields
                record = {
                    # Identifiers
                    'market_id': market_id,
                    'runner_id': runner_id,
                    
                    # Names/identifiers
                    'horse_name': horse_name or f'runner_{runner_id}',
                    'event_id': market_context.get('event_id'),
                    'event_name': market_context.get('event_name'),
                    'venue': market_context.get('venue'),
                    'race_name': market_context.get('race_name'),
                    
                    # Market metadata
                    'market_type': market_context.get('market_type'),
                    'country_code': market_context.get('country_code'),
                    'market_time': market_context.get('market_time'),
                    
                    # Pricing data
                    'ltp': runner.get('ltp'),  # Last Traded Price
                    'back_price': runner.get('bp'),
                    'lay_price': runner.get('lp'),
                    'back_volume': runner.get('bv'),
                    'lay_volume': runner.get('lv'),
                    'total_matched_volume': runner.get('tv'),
                    
                    # Odds ladder (if available in ADVANCED tier)
                    'batb': runner.get('batb'),  # Best Available To Back
                    'batl': runner.get('batl'),  # Best Available To Lay
                    'trd': runner.get('trd'),    # Trade history
                    
                    # Timestamp
                    'timestamp': timestamp,
                    'timestamp_ms': timestamp_ms,
                    
                    # Metadata
                    'ingest_timestamp': datetime.now().isoformat(),
                }
                
                yield record


class RecordToNDJSON(beam.DoFn):
    """Convert record dict to NDJSON string."""
    
    def process(self, record: Dict[str, Any]):
        # Serialize to JSON, handling None values
        json_str = json.dumps(record, default=str)
        yield json_str


# ============================================================================
# PIPELINE DEFINITION
# ============================================================================


def run(argv=None):
    """Build and run the Dataflow pipeline."""
    
    parser = argparse.ArgumentParser(description='Betfair March 15 2016 ADVANCED Data Pipeline')
    parser.add_argument('--runner', default='DirectRunner',
                        help='Apache Beam runner (DirectRunner or DataflowRunner)')
    parser.add_argument('--project', default='betfair-data-explorer',
                        help='Google Cloud project ID')
    parser.add_argument('--region', default='us-central1',
                        help='Dataflow region')
    parser.add_argument('--temp_location',
                        help='Temporary location for Dataflow (required for DataflowRunner)')
    parser.add_argument('--staging_location',
                        help='Staging location for Dataflow (required for DataflowRunner)')
    parser.add_argument('--output_bucket', default='gs://betfair-chimera-march15-normalized',
                        help='Output bucket for NDJSON file')
    
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
    logger.info("BETFAIR MARCH 15 2016 ADVANCED DATA PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Runner: {known_args.runner}")
    logger.info(f"Project: {known_args.project}")
    logger.info(f"Region: {known_args.region}")
    logger.info(f"Source: {GCS_BUCKET}/{GCS_PREFIX}/**/*.bz2")
    logger.info(f"Output: {known_args.output_bucket}/march_15_2016_normalized.ndjson")
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
        
        # Step 4: Convert records to NDJSON strings
        ndjson_records = (
            records
            | 'Convert to NDJSON' >> beam.ParDo(RecordToNDJSON())
        )
        
        # Step 5: Write to GCS as NDJSON
        output = (
            ndjson_records
            | 'Write NDJSON' >> beam.io.WriteToText(
                f'{known_args.output_bucket}/march_15_2016_normalized.ndjson',
                file_name_suffix='',  # Don't add .txt suffix
                num_shards=10  # Shard into 10 files for parallelism
            )
        )
    
    logger.info("=" * 80)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 80)


if __name__ == '__main__':
    run()
