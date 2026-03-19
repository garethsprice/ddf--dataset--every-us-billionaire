# Every US Billionaire

A DDF dataset of **1,324 US dollar billionaires** (1997–2025) with 97 attributes per person. This is a US-focused, research-augmented fork of the [Gapminder Billionaires](https://github.com/open-numbers/ddf--gapminder--billionaires) global dataset, built by combining and harmonizing data from multiple sources:

- [Forbes Billionaires List](https://www.forbes.com/billionaires/) — net worth time series, rank, demographics, education
- [Hurun Global Rich List](https://www.hurun.net/en-US/Rank/HsRankDetails?pagetype=global) — supplementary wealth data and Chinese names
- [SEC EDGAR](https://www.sec.gov/edgar/searchedgar/companysearch) — equity stakes, executive compensation, company financials (XBRL + DEF 14A proxy filings)
- [Open Library](https://openlibrary.org/) — autobiographies and biographies
- LLM-inferred attributes (Claude Opus 4) — 67 research-augmented fields marked with `*` in concept names

The dataset is filtered to **US-based individuals only**. All net worth figures are in millions of 2021 USD.

If you are new to DDF:

- [Introduction to DDF](https://open-numbers.github.io/ddf.html)
- [DDFcsv format documentation](https://docs.google.com/document/d/1aynARjsrSgOKsO1dEqboTqANRD1O9u7J_xmxy8m5jW8)

## Key documentation

- **Methodology (matching/merging rules):** [`methodology.md`](./methodology.md)
- **ETL update workflow (how to refresh the dataset):** [`etl/README.md`](./etl/README.md)
- **Concepts and indicator metadata:** [`ddf--concepts.csv`](./ddf--concepts.csv)

## What this dataset includes

### Person entities (1,324 US billionaires)

Each person has up to 97 attributes across these categories:

| Category | Example fields |
|---|---|
| Identity | `name`, `gender`, `birth_year`, `birth_date`, `ethnicity` |
| Company & career | `company`, `industry`, `ticker`, `year_founded`, `age_at_founding` |
| Wealth & ownership | `equity_stake_pct`, `voting_control_pct`, `self_made_score` |
| Compensation (SEC) | `total_comp_m`, `base_salary_k`, `stock_awards_m` |
| Demographics | `city`, `state`, `marital_status`, `number_of_children` |
| Family background | `family_socioeconomic_class`, `father_occupation`, `mother_occupation`, `parent_education_level` |
| Philanthropy | `philanthropy_score`, `giving_pledge_signatory`, `estimated_lifetime_giving_m`, `foundation_name` |
| Business strategy | `moat_type`, `revenue_model_type`, `market_position`, `skill_profile` |
| Lifestyle | `private_jet`, `yacht`, `known_art_collector`, `consumption_index` |
| Books | `autobiography`, `biography`, `books_authored` |
| Legal | `major_lawsuits` (web-verified — see lawsuit datapoints below) |
| Network | `cofounder_person` (links to other billionaires at the same company) |

Fields marked with `*` in [`ddf--concepts.csv`](./ddf--concepts.csv) are LLM-inferred from Claude Opus 4's training knowledge and have not been verified against primary sources.

### Time series datapoints

| File | Rows | Description |
|---|---|---|
| `worth--by--person--time` | 12,869 | Net worth (millions, 2021 USD), 1997–2025 |
| `forbes_rank--by--person--time` | 12,069 | Forbes billionaire rank per year |
| `annual_income` / `daily_income` | — | Derived from net worth |

### Join table datapoints

| File | Rows | Description |
|---|---|---|
| `education--by--person--education_order` | 1,025 | Schools and degrees (from Forbes profiles) |
| `major_lawsuit_claim--by--person--major_lawsuit_order` | 209 | Web-verified lawsuit claims with source URLs |

### Company financials (SEC EDGAR)

| File | Rows | Description |
|---|---|---|
| `revenue_m--by--listed_company--time` | 2,554 | Annual revenue ($M) |
| `gross_margin_pct--by--listed_company--time` | 1,015 | Gross margin (%) |
| `operating_margin_pct--by--listed_company--time` | 1,768 | Operating margin (%) |

### Country-level aggregates

Billionaire counts, billionaires per million people, and average age — by country and year.

### Entity domains

15 entity domain files define valid values for categorical fields: `ethnicity`, `listed_company`, `skill_profile`, `moat_type`, `revenue_model_type`, `initial_funding_type`, `family_socioeconomic_class`, `consumption_index`, `market_position`, `media_visibility`, `parent_education_level`, `party`, `social_media_activity`, plus geographic entities.

## Data quality notes

- **Lawsuit verification:** All 255 LLM-generated `major_lawsuits` entries were individually verified via web search. 42 fabricated entries were removed, 67 partially correct entries were corrected, and 209 verified claims are published with source URLs in the lawsuit datapoints file.
- **SEC data:** Financial data and compensation are sourced from XBRL company facts and DEF 14A proxy filings. Cross-period mismatches (e.g. revenue from one filing period, margins from another) have been corrected.
- **LLM-inferred fields:** The 67 fields marked with `*` are research-augmented estimates from Claude Opus 4's training knowledge. They should be treated as approximate and used for exploratory analysis rather than as ground truth.

## Data processing summary

The ETL pipeline has these stages:

1. **Download sources** — Forbes list JSONs, Forbes profile JSONs, Hurun data, SEC EDGAR XBRL/proxy filings, Open Library metadata
2. **Transform each source** — `forbes_data_transformation.py`, `book_data_transformation.py`, EDGAR scripts
3. **Extract LLM-inferred data** — `extract_llm_inferred.py` (from augmented survey spreadsheet)
4. **Verify sensitive claims** — `compile_lawsuits.py` (web-verified lawsuit data)
5. **Match Forbes ↔ Hurun entities** — embedding-based matching with human review
6. **Build unified dataset** — `create_unified_dataset.py` (merges all sources, filters to US, writes DDF output)

See full instructions in [`etl/README.md`](./etl/README.md).