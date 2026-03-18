"""
A notebook to connect hurun and forbes lists to a unified dataset.
"""

import pandas as pd
import json
import os

mapping_file = "../intermediate/mapping.json"

forbes_data_folder = "../intermediate/forbes"
hurun_data_folder = "../intermediate/hurun"
edgar_data_folder = "../intermediate/edgar"


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


# Merge EDGAR entity data (ticker, cik, ipo_year, comp, ownership)
edgar_entity_file = edgar_data_folder + "/ddf--entities--person.csv"
if os.path.exists(edgar_entity_file):
    edgar_person = pd.read_csv(edgar_entity_file, dtype={"cik": str, "ipo_year": "Int64"})
    unified_person_final = unified_person_final.merge(edgar_person, on="person", how="left")
    print(f"EDGAR entity merge: {edgar_person.shape[0]} rows joined, {edgar_person.columns.tolist()}")
else:
    print("WARNING: EDGAR entity file not found, skipping EDGAR merge")

# output to entity
edgar_cols = [
    "ticker",
    "cik",
    "ipo_year",
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
] + [c for c in edgar_cols if c in unified_person_final.columns]
unified_person_final = unified_person_final[final_cols]

# Filter to US individuals only
us_before = len(unified_person_final)
unified_person_final = unified_person_final[unified_person_final["country"] == "usa"].copy()
print(f"US filter: {us_before} → {len(unified_person_final)} persons")

# Also filter worth datapoints to US persons only
us_persons = set(unified_person_final["person"])
unified_worth = unified_worth[unified_worth["person"].isin(us_persons)].copy()
print(f"US worth datapoints: {len(unified_worth)} rows")

unified_person_final.to_csv("../../ddf--entities--person.csv", index=False)

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


# Copy EDGAR datapoint files to final output
edgar_datapoint_files = [
    "ddf--datapoints--revenue_m--by--person--time.csv",
    "ddf--datapoints--gross_margin_pct--by--person--time.csv",
    "ddf--datapoints--operating_margin_pct--by--person--time.csv",
]
for dpf in edgar_datapoint_files:
    src = edgar_data_folder + "/" + dpf
    dst = "../../" + dpf
    if os.path.exists(src):
        df = pd.read_csv(src)
        df = df[df["person"].isin(us_persons)]
        df.sort_values(by=["person", "time"]).to_csv(dst, index=False)
        print(f"Copied {dpf}: {len(df)} rows (US only)")
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
]
person_concepts = pd.DataFrame(person_concepts_data)

edgar_entity_concepts_data = [
    {"concept": "ticker", "name": "Ticker Symbol", "concept_type": "string", "domain": "person"},
    {"concept": "cik", "name": "SEC CIK", "concept_type": "string", "domain": "person"},
    {"concept": "ipo_year", "name": "IPO Year", "concept_type": "string", "domain": "person"},
    {"concept": "equity_stake_pct", "name": "Equity Stake (%)", "concept_type": "measure", "domain": "person"},
    {"concept": "voting_control_pct", "name": "Voting Control (%)", "concept_type": "measure", "domain": "person"},
    {"concept": "total_comp_m", "name": "Total Compensation ($M)", "concept_type": "measure", "domain": "person"},
    {"concept": "base_salary_k", "name": "Base Salary ($K)", "concept_type": "measure", "domain": "person"},
    {"concept": "stock_awards_m", "name": "Stock Awards ($M)", "concept_type": "measure", "domain": "person"},
]
edgar_entity_concepts = pd.DataFrame(edgar_entity_concepts_data)

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
    {"concept": "revenue_m", "name": "Revenue ($M)", "concept_type": "measure"},
    {"concept": "gross_margin_pct", "name": "Gross Margin (%)", "concept_type": "measure"},
    {"concept": "operating_margin_pct", "name": "Operating Margin (%)", "concept_type": "measure"},
]
measures = pd.DataFrame(measures_data)

# 3. Combine all concepts and save
all_concepts = pd.concat([concepts_on, person_concepts, edgar_entity_concepts, measures], ignore_index=True)
all_concepts.to_csv("../../ddf--concepts.csv", index=False)

all_concepts
