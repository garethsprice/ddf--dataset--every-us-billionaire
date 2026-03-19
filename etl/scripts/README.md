# Scripts Index

This folder contains scripts for building and maintaining the `ddf--dataset--every-us-billionaire` dataset.

Use this index to distinguish the **active pipeline** from **utility/experimental** scripts.

## Active pipeline (production workflow)

Run these in order when updating the dataset.

1. **Download source data**
   - `update_source_hurun.py`  
     Downloads Hurun source files into `etl/source/hurun/`.
   - `update_source_forbes.py`  
     Downloads Forbes source files into `etl/source/forbes/`.

2. **Transform each source to clean intermediate format**
   - `hurun_data_transformation.py`  
     Produces cleaned Hurun entities + wealth datapoints in `etl/intermediate/hurun/`.
   - `forbes_data_transformation.py`  
     Produces cleaned Forbes entities + worth datapoints in `etl/intermediate/forbes/`.

3. **Create embeddings for MCP matching**
   - `generate_embeddings.py`  
     Builds `etl/intermediate/embeddings/billionaire_embeddings.pkl`.

4. **Run MCP + LLM matching to build ID mapping**
   - `mcp_name_matcher.py`  
     MCP server exposing matching tools (`embedding_search`, `fuzzy_name_search`).
   - `agent/agent_name_matcher.py`  
     LLM agent that uses MCP tools to propose cross-list mappings and writes `mapping.json` to the current working directory.

5. **Human review**
   - Review and correct `mapping.json` (created by the agent in the current working directory).
   - Place the reviewed file at `etl/intermediate/mapping.json` for downstream scripts.

6. **Validate mapping quality**
   - `validate_mappings.py`  
     Checks mapping consistency, duplicates, and unmapped IDs.

7. **Build final dataset files**
   - `create_unified_dataset.py`  
     Uses reviewed mapping + transformed inputs to generate final DDF outputs in repository root.

---

## Utility / support scripts

These are helpful tools but not required for every pipeline run.

- `query_billionaires.py`  
  Local query helper for exploring candidate matches.
- `etllib.py`  
  Shared helper functions used by scripts.

## Future improvements (from previous TODO notes)

- Add a Forbes profile scraper in the MCP workflow to fetch profile pages and extract bios.
- Consider extracting company names from Forbes bios (via LLM) and appending them to the Forbes intermediate CSV used for matching.
- Improve the agent prompt so it can generate multiple mappings in one run more reliably.

---

## Experimental / analysis scripts

Use with caution; these are not the default production path.

- `income_from_worth_new.py`  
  Alternative income-estimation approach (different return-rate model).
- `check_interest_rates.py`  
  Analysis notebook-style script for validating/inspecting rate assumptions.
- `fetch_forbes_photos.py`  
  Legacy enrichment experiment (Forbes profile/photo scraping).
- `etl_template.py`  
  Old template-style ETL runner, not part of current pipeline.

---

## Methodology reference

See project-level methodology in:

- `../../methodology.md`

This describes matching assumptions, entity resolution rules, and expected manual review process.

---

## Quick start (from `etl/scripts/`)

Typical refresh flow:

- `python update_source_hurun.py <year>`
- `python update_source_forbes.py <year|all>`
- `python hurun_data_transformation.py`
- `python forbes_data_transformation.py`
- `python generate_embeddings.py`
- run MCP + agent mapping workflow
- review `mapping.json`, then place it at `../intermediate/mapping.json`
- `python validate_mappings.py`
- `python create_unified_dataset.py`
