# ETL: Update Workflow for `ddf--gapminder--billionaires`

This document explains the end-to-end process to update the dataset.

For methodology and matching rules, see:

- `../methodology.md`

---

## Overview

The update flow is:

1. Download source data (Hurun and Forbes).
2. Transform each source into clean intermediate tables.
3. Generate embeddings for matching in MCP.
4. Run MCP-based matching with an LLM to produce candidate mappings.
5. Review mappings manually.
6. Run final ETL to produce dataset outputs.

---

## Directory conventions

- Raw source files: `../source/`
- Clean intermediate files: `../intermediate/`
- Final dataset output: repository root (`../../` from `etl/scripts`)

---

## Prerequisites

From `etl/`:

1. Install all ETL + matching dependencies:
   - `pip install -r requirements.txt`

> Note:
> - `requirements.txt` is the single dependency file for this workflow.

---

## Step-by-step update process

All commands below are intended to be run from `etl/scripts/` unless noted otherwise.

### 1) Download latest Hurun and Forbes source data

#### Forbes
- Single year:
  - `python update_source_forbes.py 2025`
- All years:
  - `python update_source_forbes.py all`

Files are saved to:
- `../source/forbes/*.csv`

#### Hurun
- Per year (supported by configured year map in script):
  - `python update_source_hurun.py 2025`

Files are saved to:
- `../source/hurun/*.csv`

---

### 2) Transform source data to clean intermediate format

#### Hurun transform
- `python hurun_data_transformation.py`

Outputs:
- `../intermediate/hurun/ddf--entities--person.csv`
- `../intermediate/hurun/ddf--datapoints--wealth--by--person--year.csv`

#### Forbes transform
- `python forbes_data_transformation.py`

Outputs:
- `../intermediate/forbes/ddf--entities--person.csv`
- `../intermediate/forbes/ddf--datapoints--worth--by--person--year.csv`

---

### 3) Generate embeddings for MCP matching

- `python generate_embeddings.py`

Output:
- `../intermediate/embeddings/billionaire_embeddings.pkl`

---

### 4) Run MCP server + LLM matching

There are two components:

1. MCP server for similarity tools:
   - `python mcp_name_matcher.py`
2. Agent/LLM-driven mapping workflow:
   - `python agent/agent_name_matcher.py`

Expected result:
- `agent/agent_name_matcher.py` writes `mapping.json` to the current working directory by default.
- Move/copy that file to `../intermediate/mapping.json` before validation and final dataset build.

---

### 5) Human review of mapping

This step is required.

Recommended checks:

- Validate high-value unmatched entities.
- Confirm cross-language and transliteration matches.
- Verify family/group naming cases.
- Ensure one source ID does not map to multiple unified IDs.

Optional validation helper:
- `python validate_mappings.py`

---

### 6) Build final unified dataset

Run:
- `python create_unified_dataset.py`

Then run:
- `python income_from_worth_new.py`

This produces/updates final outputs in repo root, including:

- `ddf--entities--person.csv`
- `ddf--datapoints--worth--by--person--time.csv`
- `ddf--datapoints--annual_income--by--person--time.csv`
- `ddf--datapoints--daily_income--by--person--time.csv`
- country-level derived indicators

---

## Suggested script-folder cleanup

Current `etl/scripts` contains both production and exploratory/legacy files.

### Keep as primary pipeline scripts

- `update_source_hurun.py`
- `update_source_forbes.py`
- `hurun_data_transformation.py`
- `forbes_data_transformation.py`
- `generate_embeddings.py`
- `mcp_name_matcher.py`
- `agent/agent_name_matcher.py`
- `validate_mappings.py`
- `create_unified_dataset.py`
- `income_from_worth_new.py`

### Consider archiving or moving to `scripts/legacy/` (if not actively used)

- `check_interest_rates.py` (analysis notebook-style script)
- `query_billionaires.py` (utility/debug)
- `fetch_forbes_photos.py` (not part of core ETL)
- `etl_template.py` (template/example)
- `etllib.py` (if no longer referenced)

Also move planning notes from `TODO.md` into issues/docs once decisions are implemented.

---

## Notes

- The methodology file is `methodology.md` (repository root).
- Mapping quality directly determines final dataset quality: prioritize manual review before final build.
- If source formats change (especially Hurun yearly schema), update transformation logic before running full pipeline.