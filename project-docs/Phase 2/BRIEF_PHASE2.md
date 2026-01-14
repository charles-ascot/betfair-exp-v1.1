# PHASE 2: COLLECT & NORMALIZE MARCH 15 2016 DATA

## THE TASK

Collect ALL Betfair ADVANCED tier .bz2 files from March 15 2016, decompress them, normalize to NDJSON format, and save to a GCS bucket.

## WHAT TO DO

Run this command:

```bash
python3 betfair_march15_ndjson_pipeline.py \
    --runner DataflowRunner \
    --project betfair-data-explorer \
    --region us-central1 \
    --temp_location gs://betfair-data-explorer-temp/tmp/ \
    --staging_location gs://betfair-data-explorer-temp/staging/ \
    --output_bucket gs://betfair-chimera-march15-normalized
```

## WHAT THE PIPELINE DOES

1. Reads ALL .bz2 files from `gs://betfair-basic-historic/ADVANCED/2016/Mar/15/` (recursive, all subdirectories)
2. Automatically decompresses them
3. Normalizes the data to NDJSON format (one record per line, all fields flattened)
4. Writes output to `gs://betfair-chimera-march15-normalized/march_15_2016_normalized.ndjson`

## EXPECTED TIME

- Setup: 3-5 minutes (worker provisioning)
- Processing: 30-60 minutes (depends on file count)
- **Total: ~1 hour**

## WHEN IT FINISHES

- Check the bucket: `gs://betfair-chimera-march15-normalized/`
- You'll have NDJSON files ready to download (~700MB total)
- Report back when done

That's it.
