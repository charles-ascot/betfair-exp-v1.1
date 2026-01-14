# BETFAIR DATAFLOW SOLUTION - COMPLETE

## The Problem You Had

❌ Downloaded both M-type and E-type files (duplicates)  
❌ Claude having trouble deleting E-type files (too many to list)  
❌ Need to decompress, normalize, and load to BigQuery  

## The Solution

✅ **Dataflow pipeline** that automatically:
- Filters OUT E-type files (ignores them, no manual deletion needed)
- Keeps ONLY M-type files
- Decompresses .bz2 files
- Parses NDJSON
- Normalizes to BigQuery schema
- Writes to both target tables in one run

---

## What You Get

### File 1: `betfair_dataflow_pipeline.py` (600 lines)
- Complete Apache Beam/Dataflow pipeline
- E-file filtering built-in (see `FilterMarketFiles` class)
- Decompression and parsing
- BigQuery writing
- Ready to run

### File 2: `DATAFLOW_SETUP.md` (Detailed guide)
- Step-by-step instructions
- Local test vs. Cloud deployment
- How E-file filtering works
- Monitoring and verification
- Troubleshooting

### File 3: `CLAUDE_VSCODE_PROMPT.txt`
- Copy-paste prompt for Claude in VSCode
- Just run the 3 commands
- Claude handles everything

---

## How E-File Filtering Works

**No deletion needed.** The pipeline automatically skips them:

```python
class FilterMarketFiles(beam.DoFn):
    """Filter out Event (E) type files, keep only Market (M) type files.
    
    Pattern:
    - E-type: {EventID}_{EventID}.bz2 (same number twice at end)
    - M-type: {EventID}_{MarketID}.bz2 (different numbers)
    """
```

When you run the pipeline, you'll see:
```
INFO - PROCESSING M-type file: BASIC_2024_Jan_1_33123456_1_226402308.bz2
INFO - PROCESSING M-type file: BASIC_2024_Jan_1_33123456_1_226402309.bz2
INFO - SKIPPING E-type file: BASIC_2024_Jan_1_33123456_33123456.bz2
```

The E-files are completely ignored. **No deletion, no manual work.**

---

## Quick Start (3 Steps)

### Step 1: Copy the prompt
Copy the text from `CLAUDE_VSCODE_PROMPT.txt`

### Step 2: Paste into Claude in VSCode
Open VSCode, go to Claude chat, paste the prompt

### Step 3: Claude runs everything
Claude will:
- Install dependencies
- Authenticate
- Run local test
- Show you results

---

## Timeline

**Local Test (DirectRunner):**
- ⏱ 30-60 minutes
- Tests everything locally
- No cloud charges
- Verifies data pipeline works

**Cloud Deployment (DataflowRunner):**
- ⏱ 4-12 hours
- Runs on Google's infrastructure
- Processes full 2024 dataset in parallel
- Automatically scales

---

## What Claude Will Skip Automatically

Your bucket has ~15,000 files total:
- ~7,500 M-type files (PROCESSED)
- ~7,500 E-type files (SKIPPED, no deletion needed)

The pipeline processes ONLY the 7,500 M-type files.

The E-type files stay in the bucket but are completely ignored. **Nothing gets deleted, you don't manage anything.**

---

## Expected Results

After successful run:

**BigQuery tables:**
```
market_snapshots: ~50M rows (sampled time-series)
horse_race_participations: ~1.5M rows (one per horse per race)
```

**Then you can do:**
1. Query the data
2. Run CHIMERA prediction on historical races
3. See my full advisory reasoning process

---

## Next Action

1. **Download the 3 files** (already in outputs folder)
2. **Copy `CLAUDE_VSCODE_PROMPT.txt`** into Claude/VSCode
3. **Let Claude run it**
4. **Report results back to me**

**That's it. No manual deletion. No bucket cleanup. The pipeline handles everything.**

---

## Key Advantages

✅ **No manual file deletion** (pipeline ignores E-files)  
✅ **Fully automatic** (one command runs everything)  
✅ **Scalable** (Dataflow handles 2TB+ data)  
✅ **Recoverable** (can restart if interrupted)  
✅ **Production-ready** (handles errors gracefully)  
✅ **Follows Gemini's recommendations** (Transform + BigQuery load)  

---

**Ready? Paste the prompt into Claude/VSCode and let me know when it starts running.**
