"""
A notebook to connect hurun and forbes lists to a unified dataset.
"""

import pandas as pd
import json
import os
import shutil

mapping_file = "../intermediate/mapping.json"

forbes_data_folder = "../intermediate/forbes"
hurun_data_folder = "../intermediate/hurun"
edgar_data_folder = "../intermediate/edgar"
books_data_folder = "../intermediate/books"
llm_data_folder = "../intermediate/claude_opus_4"


mapping = json.load(open(mapping_file, "r"))

mapping[0]
# {'unified_person_id': 'ye_lipei', 'hurun_ids': ['ye_lipei'], 'forbes_ids': []}
#

# create datapoints
#
# datapoints for forbes
forbes_map = {}
for m in mapping:
    for fid in m.get("forbes_ids", []):
        forbes_map[fid] = m["unified_person_id"]

forbes_worth_file = forbes_data_folder + "/ddf--datapoints--worth--by--person--year.csv"
forbes_worth = pd.read_csv(forbes_worth_file)
forbes_worth["person"] = forbes_worth["person"].map(forbes_map)
forbes_worth.columns = ["person", "time", "worth"]
forbes_worth = forbes_worth.dropna(
    subset=["person"]
)  # need this because we dropped some dunk data.
forbes_worth = forbes_worth.drop_duplicates(subset=["person", "time"], keep="last")

forbes_worth


# datapoints for hurun
hurun_map = {}
for m in mapping:
    for hid in m.get("hurun_ids", []):
        hurun_map[hid] = m["unified_person_id"]

hurun_worth_file = hurun_data_folder + "/ddf--datapoints--wealth--by--person--year.csv"
hurun_worth = pd.read_csv(hurun_worth_file)
hurun_worth["person"] = hurun_worth["person"].map(hurun_map)
hurun_worth.columns = ["person", "time", "worth"]
hurun_worth = hurun_worth.dropna(subset=["person"])  # need this because we dropped some dunk data.
hurun_worth = hurun_worth.drop_duplicates(subset=["person", "time"], keep="last")

hurun_worth


# merge 2 list.
# We will use forbes as a base, and add records from hurun for person-years not present in forbes.
# That's because forbes has more data in general.
# To do this, we combine both dataframes, and for any duplicates on (person, time), we keep the forbes one.
forbes_worth_copy = forbes_worth.copy()
hurun_worth_copy = hurun_worth.copy()
forbes_worth_copy["source"] = "forbes"
hurun_worth_copy["source"] = "hurun"

unified_worth_tmp = pd.concat([forbes_worth_copy, hurun_worth_copy], ignore_index=True)
# sort by source to prioritize forbes (forbes > hurun in sorting)
unified_worth_tmp = unified_worth_tmp.sort_values("source", ascending=True)
unified_worth = unified_worth_tmp.drop_duplicates(subset=["person", "time"], keep="first")
unified_worth = unified_worth.drop(columns=["source"])
unified_worth

# double check
adf = forbes_worth[forbes_worth.person == "zygmunt_solorz_zak"].sort_values(by="time")
bdf = unified_worth[unified_worth.person == "zygmunt_solorz_zak"].sort_values(by="time")
assert adf.reset_index(drop=True).equals(bdf.reset_index(drop=True)), "they should be the same"

# now we should convert them to 2021 PPP dollars.
# conversion rate data was generated from the converter:
# https://docs.google.com/spreadsheets/d/1cTTPAZFztutN9FojsXd-YYEGqW2bekToBcV20_3Blkg
# Downloaded to source/GDP inflation Calculator - output.csv
rates_df = pd.read_csv("../source/gdp_inflation_conversion_rates.csv")
rates = rates_df.set_index("time")["conversion_rate"]

rates

# convert all wealth to 2021 USD.
unified_worth["worth"] = unified_worth["worth"] * unified_worth["time"].map(rates)

# convert to millions
unified_worth["worth"] = (unified_worth["worth"] * 1000).astype(int)

# NOTE: worth/income CSVs are written after the US filter below

# create entity for person

# read entity files
forbes_person = pd.read_csv("../intermediate/forbes/ddf--entities--person.csv")
hurun_person = pd.read_csv("../intermediate/hurun/ddf--entities--person.csv")

forbes_person.columns
# Index(['person', 'name', 'last_name', 'age', 'birth_year', 'gender', 'country',
#  'source', 'industry', 'title', 'image_uri', 'latest_year'],
# dtype='object')

hurun_person.columns
# Index(['person', 'name', 'chinese_name', 'gender', 'birth_year', 'country',
#        'industry', 'company', 'headquarter', 'latest_year'],
#       dtype='object')

# 1. convert person id to unified id for both
forbes_person["person"] = forbes_person["person"].map(forbes_map)
hurun_person["person"] = hurun_person["person"].map(hurun_map)

# remove those not mapped
forbes_person = forbes_person.dropna(subset=["person"])
hurun_person = hurun_person.dropna(subset=["person"])

# 2. merge both df into one. add data_source column about which source it's from for each row
forbes_person["data_source"] = "forbes"
hurun_person["data_source"] = "hurun"

unified_person = pd.concat([forbes_person, hurun_person], ignore_index=True, sort=False)

# 3. on duplicates:
# 3.1 sort by following factors: data_source: forbes first, latest_year: latest first
unified_person = unified_person.sort_values(
    by=["person", "data_source", "latest_year"], ascending=[True, True, False]
)

# 3.2 for each column, back fill data if it's empty
# by grouping by person and taking the first non-null value for each column, we effectively create a consolidated record.
unified_person_final = unified_person.groupby("person").first()

# 4. keep columns: [person, name, last_name, chinese_name, gender, birth_year, country, industry, company, source, title]
unified_person_final = unified_person_final.reset_index()

unified_person_final["birth_year"] = unified_person_final["birth_year"].astype("Int64")

unified_person_final["country"] = unified_person_final["country"].str.split("-").str[0]

# 5. map country name to geo id
# use synonyms from open-numbers
synonyms = pd.read_csv("../../../ddf--open_numbers/ddf--synonyms--geo.csv")
geo_map = synonyms.set_index("synonym")["geo"].to_dict()

# manually add more
geo_map["Eswatini (Swaziland)"] = "swz"

unified_person_final["country"] = unified_person_final["country"].map(geo_map)


unified_person_final[pd.isnull(unified_person_final["country"])][["name", "country"]]
# note 20250825: there are 588 rows without country. They are all come from hurun list before 2019 where country info was not available.
# totally 6875 rows, so about 8.5% missing country.
# FIXME: try fixing the country in hurun list.

# For people with missing country data, check if they have multiple Hurun IDs in the mapping.
# This might indicate that we can find country information from another of their Hurun profiles.
missing_country_persons = unified_person_final[pd.isnull(unified_person_final["country"])]["person"]
mapping_by_unified_id = {m["unified_person_id"]: m for m in mapping}

for person_id in missing_country_persons:
    m = mapping_by_unified_id.get(person_id)
    if m and (
        len(m.get("hurun_ids", [])) > 1 or len(m.get("forbes_ids", [])) > 0
    ):  # also output forbes for reference
        print(f"Mapping for {person_id} (missing country, multiple hurun IDs): {m}")


# Merge EDGAR person-level data (ticker, comp, ownership)
edgar_person_file = edgar_data_folder + "/ddf--entities--person.csv"
if os.path.exists(edgar_person_file):
    edgar_person = pd.read_csv(edgar_person_file)
    unified_person_final = unified_person_final.merge(edgar_person, on="person", how="left")
    print(f"EDGAR person merge: {edgar_person.shape[0]} rows joined, {edgar_person.columns.tolist()}")
else:
    print("WARNING: EDGAR person entity file not found, skipping EDGAR merge")

# Merge Forbes profile data (self-made, birth_date, city, state, marital_status, etc.)
profile_person_file = forbes_data_folder + "/ddf--entities--person--profile.csv"
if os.path.exists(profile_person_file):
    profile_person = pd.read_csv(profile_person_file)
    profile_person["person"] = profile_person["person"].map(forbes_map)
    profile_person = profile_person.dropna(subset=["person"])
    # Use birth_date to improve birth_year before merging
    if "birth_date" in profile_person.columns:
        profile_birth_year = pd.to_datetime(profile_person["birth_date"], errors="coerce").dt.year
        profile_by = profile_person[["person"]].copy()
        profile_by["profile_birth_year"] = profile_birth_year
        profile_by = profile_by.dropna(subset=["profile_birth_year"])
        profile_by["profile_birth_year"] = profile_by["profile_birth_year"].astype("Int64")
        unified_person_final = unified_person_final.merge(profile_by, on="person", how="left")
        # Overwrite birth_year with the more precise profile value where available
        mask = unified_person_final["profile_birth_year"].notna()
        unified_person_final.loc[mask, "birth_year"] = unified_person_final.loc[mask, "profile_birth_year"]
        unified_person_final = unified_person_final.drop(columns=["profile_birth_year"])
        print(f"  birth_year improved from profile birth_date for {mask.sum()} persons")
    unified_person_final = unified_person_final.merge(profile_person, on="person", how="left")
    print(f"Profile person merge: {profile_person.shape[0]} rows joined, {profile_person.columns.tolist()}")
else:
    print("WARNING: Forbes profile entity file not found, skipping profile merge")

# Merge books person-level data (autobiography, biography)
books_person_file = books_data_folder + "/ddf--entities--person.csv"
if os.path.exists(books_person_file):
    books_person = pd.read_csv(books_person_file)
    unified_person_final = unified_person_final.merge(books_person, on="person", how="left")
    print(f"Books person merge: {books_person.shape[0]} rows joined, {books_person.columns.tolist()}")
else:
    print("WARNING: Books person entity file not found, skipping books merge")

# Merge LLM-inferred person-level data (Claude Opus 4)
llm_person_file = llm_data_folder + "/ddf--entities--person.csv"
if os.path.exists(llm_person_file):
    llm_person = pd.read_csv(llm_person_file)
    # person IDs already match unified IDs (Gapminder slugs from spreadsheet)
    unified_person_final = unified_person_final.merge(llm_person, on="person", how="left")
    print(f"LLM person merge: {llm_person.shape[0]} rows joined, {len(llm_person.columns) - 1} fields")
else:
    print("WARNING: LLM-inferred person entity file not found, skipping LLM merge")

# Read EDGAR company entities (separate entity domain)
edgar_company_file = edgar_data_folder + "/ddf--entities--company.csv"
edgar_companies = None
if os.path.exists(edgar_company_file):
    edgar_companies = pd.read_csv(edgar_company_file, dtype={"cik": str, "ipo_year": "Int64"})
    print(f"EDGAR companies loaded: {len(edgar_companies)} companies")
else:
    print("WARNING: EDGAR company entity file not found")

# Copy ethnicity entity domain file to final output
ethnicity_entity_src = llm_data_folder + "/ddf--entities--ethnicity.csv"
if os.path.exists(ethnicity_entity_src):
    shutil.copy2(ethnicity_entity_src, "../../ddf--entities--ethnicity.csv")
    eth_count = len(pd.read_csv(ethnicity_entity_src))
    print(f"Ethnicity entities copied: {eth_count} entities")
else:
    print("WARNING: Ethnicity entity file not found, skipping")

# Copy LLM entity domain files to final output
for entity_domain in ["initial_funding_type", "skill_profile", "moat_type", "revenue_model_type",
                       "family_socioeconomic_class", "consumption_index", "market_position",
                       "media_visibility", "parent_education_level", "party", "social_media_activity"]:
    entity_src = llm_data_folder + f"/ddf--entities--{entity_domain}.csv"
    if os.path.exists(entity_src):
        shutil.copy2(entity_src, f"../../ddf--entities--{entity_domain}.csv")
        ent_count = len(pd.read_csv(entity_src))
        print(f"{entity_domain} entities copied: {ent_count} entities")
    else:
        print(f"WARNING: {entity_domain} entity file not found, skipping")

# output to entity
edgar_person_cols = [
    "ticker",
    "equity_stake_pct",
    "voting_control_pct",
    "total_comp_m",
    "base_salary_k",
    "stock_awards_m",
]
final_cols = [
    "person",
    "name",
    "last_name",
    "chinese_name",
    "gender",
    "birth_year",
    "country",
    "industry",
    "company",
    "source",
    "title",
] + [c for c in edgar_person_cols if c in unified_person_final.columns] + [c for c in ["birth_date", "birth_country", "birth_city", "birth_state", "city", "state", "marital_status", "number_of_children", "self_made_score", "self_made_type", "philanthropy_score"] if c in unified_person_final.columns] + [c for c in ["autobiography", "biography"] if c in unified_person_final.columns]

# Append any LLM-inferred columns not already in final_cols
if os.path.exists(llm_data_folder + "/ddf--entities--person.csv"):
    llm_cols = pd.read_csv(llm_data_folder + "/ddf--entities--person.csv", nrows=0).columns.tolist()
    llm_cols = [c for c in llm_cols if c != "person" and c not in final_cols and c in unified_person_final.columns]
    final_cols += llm_cols
unified_person_final = unified_person_final[final_cols]

# Filter to US individuals only
us_before = len(unified_person_final)
unified_person_final = unified_person_final[unified_person_final["country"] == "usa"].copy()
print(f"US filter: {us_before} → {len(unified_person_final)} persons")

# Also filter worth datapoints to US persons only
us_persons = set(unified_person_final["person"])
unified_worth = unified_worth[unified_worth["person"].isin(us_persons)].copy()
print(f"US worth datapoints: {len(unified_worth)} rows")

# Create cofounder_person links (other billionaires at the same company)
GENERIC_COMPANIES = {"Investments", "Investment", "Movies", "Music"}
company_groups = (
    unified_person_final.dropna(subset=["company"])
    .groupby("company")["person"]
    .apply(lambda x: sorted(set(x)))
    .to_dict()
)
cofounder_map = {}
for company, persons in company_groups.items():
    if company in GENERIC_COMPANIES or len(persons) < 2:
        continue
    for p in persons:
        others = [x for x in persons if x != p]
        if others:
            cofounder_map[p] = ",".join(others)
unified_person_final["cofounder_person"] = unified_person_final["person"].map(cofounder_map)
linked = unified_person_final["cofounder_person"].notna().sum()
print(f"Cofounder links: {linked} persons linked across {sum(1 for v in company_groups.values() if len(v) >= 2 and v[0] not in GENERIC_COMPANIES)} companies")

# Clean major_lawsuits using verified data: clear FALSE/fabricated entries,
# update PARTIAL entries with corrected claims, clear "none" variants
lawsuit_verified_file = llm_data_folder + "/ddf--entities--major_lawsuit.csv"
if os.path.exists(lawsuit_verified_file):
    lawsuit_verified = pd.read_csv(lawsuit_verified_file)
    # Build lookup: person -> (verdict, corrected_claim)
    lawsuit_lookup = {}
    for _, row in lawsuit_verified.iterrows():
        lawsuit_lookup[row["major_lawsuit"]] = (row["verdict"], row["name"])

    # Persons with FALSE verdicts -> clear their major_lawsuits
    false_persons = {p for p, (v, _) in lawsuit_lookup.items() if v == "FALSE"}
    # "None" variant values to clear
    none_values = {"N", "None identified", "None known", "None stated", "unknown"}

    cleared_false = 0
    cleared_none = 0
    updated_partial = 0
    for idx, row in unified_person_final.iterrows():
        person = row["person"]
        lawsuit = row.get("major_lawsuits", "")
        if pd.isna(lawsuit) or not str(lawsuit).strip():
            continue
        lawsuit_str = str(lawsuit).strip()
        if person in false_persons:
            unified_person_final.at[idx, "major_lawsuits"] = ""
            cleared_false += 1
        elif lawsuit_str in none_values:
            unified_person_final.at[idx, "major_lawsuits"] = ""
            cleared_none += 1
        elif person in lawsuit_lookup and lawsuit_lookup[person][0] == "PARTIAL":
            unified_person_final.at[idx, "major_lawsuits"] = lawsuit_lookup[person][1]
            updated_partial += 1
    print(f"Lawsuit cleanup: cleared {cleared_false} FALSE, {cleared_none} 'none' variants, updated {updated_partial} PARTIAL claims")

unified_person_final.to_csv("../../ddf--entities--person.csv", index=False)

# Write listed_company entities (filtered to tickers used by US persons)
if edgar_companies is not None:
    us_tickers = set(unified_person_final["ticker"].dropna())
    edgar_companies_us = edgar_companies[edgar_companies["company"].isin(us_tickers)].copy()
    # Rename 'company' column to 'listed_company' for DDF entity domain
    edgar_companies_us = edgar_companies_us.rename(columns={"company": "listed_company"})
    edgar_companies_us.sort_values("listed_company").to_csv("../../ddf--entities--listed_company.csv", index=False)
    print(f"Listed company entities: {len(edgar_companies_us)} companies (US persons' tickers)")

# Map and write Forbes rank datapoints (US only)
forbes_rank_file = forbes_data_folder + "/ddf--datapoints--rank--by--person--year.csv"
if os.path.exists(forbes_rank_file):
    forbes_rank = pd.read_csv(forbes_rank_file)
    forbes_rank["person"] = forbes_rank["person"].map(forbes_map)
    forbes_rank.columns = ["person", "time", "forbes_rank"]
    forbes_rank = forbes_rank.dropna(subset=["person"])
    forbes_rank = forbes_rank.drop_duplicates(subset=["person", "time"], keep="last")
    forbes_rank = forbes_rank[forbes_rank["person"].isin(us_persons)].copy()
    forbes_rank.sort_values(by=["person", "time"]).to_csv(
        "../../ddf--datapoints--forbes_rank--by--person--time.csv", index=False
    )
    print(f"Forbes rank datapoints: {len(forbes_rank)} rows (US only)")
else:
    print("WARNING: Forbes rank datapoints file not found, skipping")

# Map and write Forbes education data (US only)
forbes_education_file = forbes_data_folder + "/ddf--entities--person--education.csv"
if os.path.exists(forbes_education_file):
    forbes_education = pd.read_csv(forbes_education_file)
    forbes_education["person"] = forbes_education["person"].map(forbes_map)
    forbes_education = forbes_education.dropna(subset=["person"])
    forbes_education = forbes_education[forbes_education["person"].isin(us_persons)].copy()
    forbes_education.sort_values(by=["person", "education_order"]).to_csv(
        "../../ddf--datapoints--education--by--person--education_order.csv", index=False
    )
    print(f"Education data: {len(forbes_education)} rows ({forbes_education['person'].nunique()} US persons)")
else:
    print("WARNING: Forbes education file not found, skipping")

# Write verified lawsuit datapoints (from compile_lawsuits.py output)
lawsuit_verified_file = llm_data_folder + "/ddf--entities--major_lawsuit.csv"
if os.path.exists(lawsuit_verified_file):
    lawsuit_df = pd.read_csv(lawsuit_verified_file)
    # Filter to US persons only, exclude FALSE verdicts (only TRUE + PARTIAL get datapoints)
    lawsuit_df = lawsuit_df.rename(columns={"major_lawsuit": "person", "name": "claim"})
    lawsuit_df = lawsuit_df[lawsuit_df["person"].isin(us_persons)].copy()
    lawsuit_df = lawsuit_df[lawsuit_df["verdict"] != "FALSE"].copy()
    # Build DDF datapoints format: person, major_lawsuit_order, major_lawsuit_claim, major_lawsuit_source_url
    lawsuit_dp = lawsuit_df[["person", "claim", "source_url"]].copy()
    lawsuit_dp = lawsuit_dp.rename(columns={"claim": "major_lawsuit_claim", "source_url": "major_lawsuit_source_url"})
    lawsuit_dp["major_lawsuit_order"] = 1
    lawsuit_dp = lawsuit_dp[["person", "major_lawsuit_order", "major_lawsuit_claim", "major_lawsuit_source_url"]]
    lawsuit_dp.sort_values(by=["person"]).to_csv(
        "../../ddf--datapoints--major_lawsuit_claim--by--person--major_lawsuit_order.csv", index=False
    )
    print(f"Lawsuit datapoints: {len(lawsuit_dp)} rows ({lawsuit_dp['person'].nunique()} US persons)")
else:
    print("WARNING: ddf--entities--major_lawsuit.csv not found, skipping lawsuit datapoints")

# Write worth and income datapoints (after US filter)
unified_worth.sort_values(by=["person", "time"]).to_csv(
    "../../ddf--datapoints--worth--by--person--time.csv", index=False
)

income_df = unified_worth.copy()
income_df["annual_income"] = (income_df["worth"] * 1_000_000 * 0.03).astype(int)
income_df["daily_income"] = (income_df["annual_income"] / 365).astype(int)

income_df[["person", "time", "annual_income"]].sort_values(by=["person", "time"]).to_csv(
    "../../ddf--datapoints--annual_income--by--person--time.csv", index=False
)

income_df[["person", "time", "daily_income"]].sort_values(by=["person", "time"]).to_csv(
    "../../ddf--datapoints--daily_income--by--person--time.csv", index=False
)

unified_person_final


# now calculate some more indicators.
# average_age_of_dollar_billionaires_years  - by country/time
# dollar_billionaires_per_million_people - by country/time
# total_number_of_dollar_billionaires - by country/time
#

# Average age of billionaires
person_info = unified_person_final[["person", "birth_year", "country"]].copy()
merged_df = pd.merge(unified_worth, person_info, on="person")

# Drop rows with missing birth_year or country
merged_df = merged_df.dropna(subset=["birth_year", "country"]).copy()

merged_df

# Calculate age
merged_df["age"] = merged_df["time"] - merged_df["birth_year"]

# check suspious values
to_check = (
    merged_df[merged_df["age"] < 20].sort_values(by=["person", "time"]).groupby("person").first()
)
print("here are some possible errors (only one row for each person displayed):")
print(to_check)

# Group by country and time and calculate average age
average_age_df = merged_df.groupby(["country", "time"])["age"].mean().reset_index()

# Rename columns to match DDF format
average_age_df.columns = ["country", "time", "average_age_of_dollar_billionaires_years"]
average_age_df["average_age_of_dollar_billionaires_years"] = average_age_df[
    "average_age_of_dollar_billionaires_years"
].round(2)

# Save to CSV
average_age_df.sort_values(by=["country", "time"]).to_csv(
    "../../ddf--datapoints--average_age_of_dollar_billionaires_years--by--country--time.csv",
    index=False,
)

average_age_df

# total_number_of_dollar_billionaires - by country/time
person_country = unified_person_final[["person", "country"]].copy()
merged_df_for_count = pd.merge(unified_worth, person_country, on="person")
merged_df_for_count = merged_df_for_count.dropna(subset=["country"])

total_billionaires_df = (
    merged_df_for_count.groupby(["country", "time"])
    .size()
    .reset_index(name="total_number_of_dollar_billionaires")
)

total_billionaires_df.sort_values(by=["country", "time"]).to_csv(
    "../../ddf--datapoints--total_number_of_dollar_billionaires--by--country--time.csv", index=False
)

total_billionaires_df


# load population data
pop = pd.read_csv("../source/ddf--datapoints--pop--by--country--time.csv")

# dollar_billionaires_per_million_people - by country/time
merged_pop_df = pd.merge(total_billionaires_df, pop, on=["country", "time"], how="inner")
merged_pop_df["dollar_billionaires_per_million_people"] = (
    merged_pop_df["total_number_of_dollar_billionaires"] / merged_pop_df["pop"]
) * 1_000_000

per_million_df = merged_pop_df[["country", "time", "dollar_billionaires_per_million_people"]]

per_million_df.sort_values(by=["country", "time"]).to_csv(
    "../../ddf--datapoints--dollar_billionaires_per_million_people--by--country--time.csv",
    index=False,
)

per_million_df


# Copy EDGAR company-keyed datapoint files to final output (filtered to US persons' tickers)
us_tickers = set(unified_person_final["ticker"].dropna())
edgar_datapoint_src_files = [
    "ddf--datapoints--revenue_m--by--company--time.csv",
    "ddf--datapoints--gross_margin_pct--by--company--time.csv",
    "ddf--datapoints--operating_margin_pct--by--company--time.csv",
]
for dpf in edgar_datapoint_src_files:
    src = edgar_data_folder + "/" + dpf
    # Rename 'company' → 'listed_company' in output filename and column
    dst_name = dpf.replace("--by--company--", "--by--listed_company--")
    dst = "../../" + dst_name
    if os.path.exists(src):
        df = pd.read_csv(src)
        df = df[df["company"].isin(us_tickers)]
        df = df.rename(columns={"company": "listed_company"})
        df.sort_values(by=["listed_company", "time"]).to_csv(dst, index=False)
        print(f"Wrote {dst_name}: {len(df)} rows (US tickers)")
    else:
        print(f"WARNING: {src} not found, skipping")


# create concepts
# 1. load concepts from open_numbers name space
concepts_on = pd.read_csv("../source/ddf--concepts.csv")

# 2. Create concepts for person entity and measures
person_concepts_data = [
    {"concept": "person", "name": "Person", "concept_type": "entity_domain"},
    {"concept": "last_name", "name": "Last Name", "concept_type": "string", "domain": "person"},
    {
        "concept": "chinese_name",
        "name": "Chinese Name",
        "concept_type": "string",
        "domain": "person",
    },
    {"concept": "birth_year", "name": "Birth Year", "concept_type": "string", "domain": "person"},
    {"concept": "industry", "name": "Industry", "concept_type": "string", "domain": "person"},
    {"concept": "company", "name": "Company", "concept_type": "string", "domain": "person"},
    {"concept": "source", "name": "Source", "concept_type": "string", "domain": "person"},
    {"concept": "title", "name": "Title", "concept_type": "string", "domain": "person"},
    {"concept": "gender", "name": "Gender", "concept_type": "string", "domain": "person"},
    {"concept": "cofounder_person", "name": "Co-Founder(s) in Dataset", "concept_type": "string", "domain": "person"},
]
person_concepts = pd.DataFrame(person_concepts_data)

# Ethnicity entity domain concepts
ethnicity_concepts_data = [
    {"concept": "ethnicity", "name": "Ethnicity / National Origin*", "concept_type": "entity_domain"},
    {"concept": "broad_group", "name": "Broad Ethnic Group", "concept_type": "string", "domain": "ethnicity"},
]
ethnicity_concepts = pd.DataFrame(ethnicity_concepts_data)

# Listed company entity domain concepts (keyed by ticker symbol)
company_concepts_data = [
    {"concept": "listed_company", "name": "Listed Company", "concept_type": "entity_domain"},
    {"concept": "cik", "name": "SEC CIK", "concept_type": "string", "domain": "listed_company"},
    {"concept": "company_name", "name": "Company Name", "concept_type": "string", "domain": "listed_company"},
    {"concept": "ipo_year", "name": "IPO Year", "concept_type": "string", "domain": "listed_company"},
]
company_concepts = pd.DataFrame(company_concepts_data)

# EDGAR person-level concepts (ticker links person → company)
edgar_person_concepts_data = [
    {"concept": "ticker", "name": "Ticker Symbol", "concept_type": "string", "domain": "person"},
    {"concept": "equity_stake_pct", "name": "Equity Stake (%)", "concept_type": "measure", "domain": "person"},
    {"concept": "voting_control_pct", "name": "Voting Control (%)", "concept_type": "measure", "domain": "person"},
    {"concept": "total_comp_m", "name": "Total Compensation ($M)", "concept_type": "measure", "domain": "person"},
    {"concept": "base_salary_k", "name": "Base Salary ($K)", "concept_type": "measure", "domain": "person"},
    {"concept": "stock_awards_m", "name": "Stock Awards ($M)", "concept_type": "measure", "domain": "person"},
]
edgar_person_concepts = pd.DataFrame(edgar_person_concepts_data)

# Forbes profile concepts
profile_concepts_data = [
    {"concept": "birth_date", "name": "Birth Date", "concept_type": "string", "domain": "person"},
    {"concept": "birth_country", "name": "Birth Country", "concept_type": "string", "domain": "person"},
    {"concept": "birth_city", "name": "Birth City", "concept_type": "string", "domain": "person"},
    {"concept": "birth_state", "name": "Birth State", "concept_type": "string", "domain": "person"},
    {"concept": "city", "name": "City of Residence", "concept_type": "string", "domain": "person"},
    {"concept": "state", "name": "State of Residence", "concept_type": "string", "domain": "person"},
    {"concept": "marital_status", "name": "Marital Status", "concept_type": "string", "domain": "person"},
    {"concept": "number_of_children", "name": "Number of Children", "concept_type": "measure", "domain": "person"},
    {"concept": "self_made_score", "name": "Forbes Self-Made Score (1-10)", "concept_type": "measure", "domain": "person"},
    {"concept": "self_made_type", "name": "Self-Made Type", "concept_type": "string", "domain": "person"},
    {"concept": "philanthropy_score", "name": "Forbes Philanthropy Score (1-5)", "concept_type": "measure", "domain": "person"},
]
profile_concepts = pd.DataFrame(profile_concepts_data)

# Education concepts
education_concepts_data = [
    {"concept": "education_order", "name": "Education Order", "concept_type": "measure"},
    {"concept": "school", "name": "School", "concept_type": "string"},
    {"concept": "degree", "name": "Degree", "concept_type": "string"},
]
education_concepts = pd.DataFrame(education_concepts_data)

# Lawsuit datapoints concepts (verified with web sources)
lawsuit_concepts_data = [
    {"concept": "major_lawsuit_order", "name": "Major Lawsuit Order", "concept_type": "measure"},
    {"concept": "major_lawsuit_claim", "name": "Major Lawsuit Claim (Verified)", "concept_type": "string"},
    {"concept": "major_lawsuit_source_url", "name": "Major Lawsuit Source URL", "concept_type": "string"},
]
lawsuit_concepts = pd.DataFrame(lawsuit_concepts_data)

# LLM-inferred concepts (Claude Opus 4, from training knowledge)
llm_measure_cols = {
    "age_at_founding", "age_became_billionaire", "age_education_ended",
    "estimated_inheritance_m", "estimated_lifetime_giving_m",
    "giving_pct_of_net_worth", "number_of_co_founders",
    "number_of_companies_founded", "number_of_marriages",
    "total_pre_founding_career_years", "wealth_concentration_ratio_pct",
    "year_founded", "years_at_employer_1", "years_at_employer_2",
    "years_at_employer_3", "years_founding_to_1b", "children_in_business",
}
llm_concepts_data = [
    {"concept": "age_at_founding", "name": "Age at Founding*"},
    {"concept": "age_became_billionaire", "name": "Age Became Billionaire*"},
    {"concept": "age_education_ended", "name": "Age Education Ended*"},
    {"concept": "books_authored", "name": "Books Authored*"},
    {"concept": "children_in_business", "name": "Children in Business (Count)*"},
    {"concept": "co_founder_skill_complement", "name": "Co-Founder Skill Complement*"},
    {"concept": "employer_1", "name": "Employer 1*"},
    {"concept": "employer_2", "name": "Employer 2*"},
    {"concept": "employer_3", "name": "Employer 3*"},
    {"concept": "estimated_inheritance_m", "name": "Estimated Inheritance ($M)*"},
    {"concept": "estimated_lifetime_giving_m", "name": "Estimated Lifetime Giving ($M)*"},
    {"concept": "heritage_detail", "name": "Heritage Detail*"},
    {"concept": "family_business_type", "name": "Family Business Type*"},
    {"concept": "family_office", "name": "Family Office (Y/N)*"},
    {"concept": "family_office_name", "name": "Family Office Name*"},
    {"concept": "family_owned_business", "name": "Family Owned Business (Y/N)*"},
    {"concept": "father_occupation", "name": "Father Occupation*"},
    {"concept": "first_gen_immigrant", "name": "First-Gen Immigrant (Y/N)*"},
    {"concept": "first_institutional_investor", "name": "First Institutional Investor*"},
    {"concept": "foundation_name", "name": "Foundation Name*"},
    {"concept": "giving_pct_of_net_worth", "name": "Giving as % of Net Worth*"},
    {"concept": "giving_pledge_signatory", "name": "Giving Pledge Signatory (Y/N)*"},
    {"concept": "government_roles_held", "name": "Government Roles Held*"},
    {"concept": "initial_funding_detail", "name": "Initial Funding Detail*"},
    {"concept": "known_art_collector", "name": "Known Art Collector (Y/N)*"},
    {"concept": "known_failed_ventures", "name": "Known Failed Ventures*"},
    {"concept": "major_lawsuits", "name": "Major Lawsuits*"},
    {"concept": "market_position_detail", "name": "Market Position Detail*"},
    {"concept": "moat_type_detail", "name": "Moat Type Detail*"},
    {"concept": "mother_occupation", "name": "Mother Occupation*"},
    {"concept": "notable_political_positions", "name": "Notable Political Positions*"},
    {"concept": "number_of_co_founders", "name": "Number of Co-Founders*"},
    {"concept": "number_of_companies_founded", "name": "Number of Companies Founded*"},
    {"concept": "number_of_marriages", "name": "Number of Marriages*"},
    {"concept": "parent_immigrant", "name": "Parent Immigrant (Y/N)*"},
    {"concept": "parent_immigration_detail", "name": "Parent Immigration Detail*"},
    {"concept": "pre_wealth_bankruptcy", "name": "Pre-Wealth Bankruptcy (Y/N)*"},
    {"concept": "primary_domain_expertise", "name": "Primary Domain Expertise*"},
    {"concept": "party_detail", "name": "Party Affiliation Detail*"},
    {"concept": "primary_philanthropic_causes", "name": "Primary Philanthropic Causes*"},
    {"concept": "private_jet", "name": "Private Jet (Y/N)*"},
    {"concept": "revenue_model_type_detail", "name": "Revenue Model Type Detail*"},
    {"concept": "role_at_employer_1", "name": "Role at Employer 1*"},
    {"concept": "role_at_employer_2", "name": "Role at Employer 2*"},
    {"concept": "role_at_employer_3", "name": "Role at Employer 3*"},
    {"concept": "co_founded", "name": "Co-Founded (Y/N)*"},
    {"concept": "founding_detail", "name": "Founding Detail*"},
    {"concept": "total_pre_founding_career_years", "name": "Total Pre-Founding Career Years*"},
    {"concept": "wealth_concentration_ratio_pct", "name": "Wealth Concentration Ratio (%)*"},
    {"concept": "yacht", "name": "Yacht (Y/N)*"},
    {"concept": "year_founded", "name": "Year Founded*"},
    {"concept": "years_at_employer_1", "name": "Years at Employer 1*"},
    {"concept": "years_at_employer_2", "name": "Years at Employer 2*"},
    {"concept": "years_at_employer_3", "name": "Years at Employer 3*"},
    {"concept": "years_founding_to_1b", "name": "Years from Founding to $1B*"},
]
# * = LLM-inferred (Claude Opus 4, from training knowledge — not verified against primary sources)
for entry in llm_concepts_data:
    entry["concept_type"] = "measure" if entry["concept"] in llm_measure_cols else "string"
    entry["domain"] = "person"
llm_concepts = pd.DataFrame(llm_concepts_data)

# LLM entity domain concepts (initial_funding_type, skill_profile, moat_type, revenue_model_type)
llm_entity_concepts_data = [
    {"concept": "initial_funding_type", "name": "Initial Funding Type*", "concept_type": "entity_domain"},
    {"concept": "skill_profile", "name": "Skill Profile*", "concept_type": "entity_domain"},
    {"concept": "moat_type", "name": "Moat Type*", "concept_type": "entity_domain"},
    {"concept": "revenue_model_type", "name": "Revenue Model Type*", "concept_type": "entity_domain"},
    {"concept": "family_socioeconomic_class", "name": "Family Socioeconomic Class*", "concept_type": "entity_domain"},
    {"concept": "consumption_index", "name": "Consumption Index*", "concept_type": "entity_domain"},
    {"concept": "market_position", "name": "Market Position*", "concept_type": "entity_domain"},
    {"concept": "media_visibility", "name": "Media Visibility*", "concept_type": "entity_domain"},
    {"concept": "parent_education_level", "name": "Parent Education Level*", "concept_type": "entity_domain"},
    {"concept": "party", "name": "Political Party*", "concept_type": "entity_domain"},
    {"concept": "social_media_activity", "name": "Social Media Activity*", "concept_type": "entity_domain"},
]
llm_entity_concepts = pd.DataFrame(llm_entity_concepts_data)

# Book concepts (autobiography, biography)
book_concepts_data = [
    {"concept": "autobiography", "name": "Autobiography", "concept_type": "string", "domain": "person"},
    {"concept": "biography", "name": "Biography", "concept_type": "string", "domain": "person"},
]
book_concepts = pd.DataFrame(book_concepts_data)

measures_data = [
    {"concept": "worth", "name": "Net Worth (millions, 2021 USD)", "concept_type": "measure"},
    {"concept": "annual_income", "name": "Annual Income (2021 USD)", "concept_type": "measure"},
    {"concept": "daily_income", "name": "Daily Income (2021 USD)", "concept_type": "measure"},
    {
        "concept": "average_age_of_dollar_billionaires_years",
        "name": "Average age of dollar billionaires (years)",
        "concept_type": "measure",
    },
    {
        "concept": "total_number_of_dollar_billionaires",
        "name": "Total number of dollar billionaires",
        "concept_type": "measure",
    },
    {
        "concept": "dollar_billionaires_per_million_people",
        "name": "Dollar billionaires per million people",
        "concept_type": "measure",
    },
    {"concept": "forbes_rank", "name": "Forbes Billionaire Rank", "concept_type": "measure"},
    {"concept": "revenue_m", "name": "Revenue ($M)", "concept_type": "measure"},
    {"concept": "gross_margin_pct", "name": "Gross Margin (%)", "concept_type": "measure"},
    {"concept": "operating_margin_pct", "name": "Operating Margin (%)", "concept_type": "measure"},
]
measures = pd.DataFrame(measures_data)

# 3. Combine all concepts and save
all_concepts = pd.concat([concepts_on, person_concepts, ethnicity_concepts, company_concepts, edgar_person_concepts, profile_concepts, education_concepts, lawsuit_concepts, book_concepts, llm_concepts, llm_entity_concepts, measures], ignore_index=True)
all_concepts.to_csv("../../ddf--concepts.csv", index=False)

all_concepts
