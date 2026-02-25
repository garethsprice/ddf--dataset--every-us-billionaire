# Gapminder Billionaires Dataset

This repository contains a DDF dataset on dollar billionaires, built by combining and harmonizing data from:

- [Forbes Billionaires List](https://www.forbes.com/billionaires/)
- [Hurun Global Rich List](https://www.hurun.net/en-US/Rank/HsRankDetails?pagetype=global)

If you are new to DDF:

- [Introduction to DDF](https://open-numbers.github.io/ddf.html)
- [DDFcsv format documentation](https://docs.google.com/document/d/1aynARjsrSgOKsO1dEqboTqANRD1O9u7J_xmxy8m5jW8)

## Key documentation

- **Methodology (matching/merging rules):** [`methodology.md`](./methodology.md)
- **ETL update workflow (how to refresh the dataset):** [`etl/README.md`](./etl/README.md)
- **Concepts and indicator metadata:** [`ddf--concepts.csv`](./ddf--concepts.csv)

## What this dataset includes

The dataset contains:

- Person-level entity data (billionaires)
- Person-level time series (e.g. `worth`, `annual_income`, `daily_income`)
- Country-level derived indicators (e.g. billionaire counts, billionaires per million, average age)

You can inspect available files directly in the repository root (for datapoints/entities) and in [`ddf--concepts.csv`](./ddf--concepts.csv) for definitions and metadata.

## Data processing summary

At a high level, updates follow this process:

1. Download source data (Hurun + Forbes)
2. Transform each source to clean intermediate tables
3. Generate embeddings for matching
4. Run MCP/LLM-assisted matching to create mappings
5. Human review of mappings
6. Build final DDF outputs

See full instructions in [`etl/README.md`](./etl/README.md).