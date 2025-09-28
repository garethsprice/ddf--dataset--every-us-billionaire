"""
A notebook to connect hurun and forbes lists to a unified dataset.
"""

import pandas as pd
import json

mapping_file = "../intermediate/mapping.json"

forbes_data_folder = "../intermediate/forbes"
hurun_data_folder = "../intermediate/hurun"


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

unified_worth.sort_values(by=["person", "time"]).to_csv(
    "../../ddf--datapoints--worth--by--person--time.csv", index=False
)

# assume 3% of wealth to be the annual income of billionaires. calculate annual income and daily income (annual / 365.) for them
# convert the unit to one dollar and just use int.
income_df = unified_worth.copy()
income_df["annual_income"] = (income_df["worth"] * 1_000_000 * 0.03).astype(int)
income_df["daily_income"] = (income_df["annual_income"] / 365).astype(int)

# create csv for each indicator.
income_df[["person", "time", "annual_income"]].sort_values(by=["person", "time"]).to_csv(
    "../../ddf--datapoints--annual_income--by--person--time.csv", index=False
)

income_df[["person", "time", "daily_income"]].sort_values(by=["person", "time"]).to_csv(
    "../../ddf--datapoints--daily_income--by--person--time.csv", index=False
)

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


# output to entity
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
]
unified_person_final = unified_person_final[final_cols]

unified_person_final.to_csv("../../ddf--entities--person.csv", index=False)

unified_person_final
