import pandas as pd
import json
import os
import datetime


# Reference date is 2025-04-01. we assume this is the release date for latest forbes
# change it when new data comes.
reference_date = datetime.datetime(2025, 4, 1)

# Function to convert gender from M/F to Male/Female
def convert_gender(gender):
    if gender == 'M':
        return 'Male'
    elif gender == 'F':
        return 'Female'
    else:
        return gender


def transform_forbes_data(source_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_files = [os.path.join(source_dir, f) for f in os.listdir(source_dir) if f.endswith(".csv")]
    all_files.sort()

    datapoints_list = []
    rank_list = []
    entities_map = {}

    for file in all_files:
        year = int(os.path.basename(file).split(".")[0])
        df = pd.read_csv(file)

        # Per methodology, remove junk data where 'worth' is a floating-point number.
        # This is interpreted as removing rows where 'worth' is not a whole number.
        df["worth"] = pd.to_numeric(df["worth"], errors="coerce")
        df = df.dropna(subset=["worth"])
        df = df[df["worth"] % 1 == 0]

        df = df.rename(columns={"uri": "person"})

        for _, row in df.iterrows():
            if pd.isna(row["person"]):
                continue

            person_id = str(row["person"]).replace("-", "_")

            # Convert worth from millions to billions
            worth_in_billions = row["worth"] / 1000

            datapoints_list.append({"person": person_id, "year": year, "worth": worth_in_billions})

            # Extract rank if available
            rank = row.get("rank", None)
            if rank is not None and not pd.isna(rank):
                rank_list.append({"person": person_id, "year": year, "rank": int(rank)})

            image_uri = row.get("imageUri", "")

            # Calculate birth year from age (if age is available)
            age = row.get("age", "")
            birth_year = None
            if age and not pd.isna(age):
                try:
                    birth_year = reference_date.year - int(age)
                except (ValueError, TypeError):
                    birth_year = None

            # Overwrite with the latest data. Since files are sorted by year,
            # this will keep the data from the last year a person appears.
            entities_map[person_id] = {
                "person": person_id,
                "name": row.get("name", ""),
                "last_name": row.get("lastName", ""),
                "age": row.get("age", ""),
                "birth_year": birth_year,
                "gender": convert_gender(row.get("gender", "")),
                "country": row.get("country", ""),
                "source": row.get("source", ""),
                "industry": row.get("industry", ""),
                "title": row.get("title", ""),
                "image_uri": pd.NA if not image_uri.startswith("no-pic") else image_uri,
                "latest_year": year,
            }

    datapoints_df = pd.DataFrame(datapoints_list).dropna(subset=["worth"])
    entities_df = pd.DataFrame(list(entities_map.values()))

    # Sort data before saving
    datapoints_df = datapoints_df.sort_values(by=["person", "year"])
    entities_df = entities_df.sort_values(by="person")

    datapoints_df.to_csv(
        os.path.join(output_dir, "ddf--datapoints--worth--by--person--year.csv"),
        index=False,
    )
    entities_df.to_csv(os.path.join(output_dir, "ddf--entities--person.csv"), index=False)

    # Write rank datapoints
    rank_df = pd.DataFrame(rank_list)
    if not rank_df.empty:
        rank_df = rank_df.sort_values(by=["person", "year"])
        rank_df.to_csv(
            os.path.join(output_dir, "ddf--datapoints--rank--by--person--year.csv"),
            index=False,
        )
        print(f"Rank datapoints: {len(rank_df)} rows")

    # Extract profile data from profile JSONs
    profiles_dir = os.path.join(source_dir, "profiles")
    if os.path.exists(profiles_dir):
        profile_list = []
        education_list = []
        for fname in sorted(os.listdir(profiles_dir)):
            if not fname.endswith(".json"):
                continue
            try:
                with open(os.path.join(profiles_dir, fname)) as f:
                    data = json.load(f)
                person_data = data.get("person", data)
                uri = person_data.get("uri", fname.replace(".json", ""))
                person_id = uri.replace("-", "_")

                row = {"person": person_id}

                # Self-made fields
                if person_data.get("selfMadeRank") is not None:
                    row["self_made_score"] = int(person_data["selfMadeRank"])
                if person_data.get("selfMadeType") is not None:
                    row["self_made_type"] = str(person_data["selfMadeType"])

                # Birth date (epoch ms → YYYY-MM-DD)
                bd = person_data.get("birthDate")
                if bd is not None:
                    try:
                        dt = datetime.datetime.fromtimestamp(bd / 1000, tz=datetime.timezone.utc)
                        row["birth_date"] = dt.strftime("%Y-%m-%d")
                    except (ValueError, OSError):
                        pass

                # Location
                if person_data.get("city"):
                    row["city"] = str(person_data["city"])
                if person_data.get("stateProvince"):
                    row["state"] = str(person_data["stateProvince"])

                # Demographics
                if person_data.get("maritalStatus"):
                    row["marital_status"] = str(person_data["maritalStatus"])
                if person_data.get("numberOfChildren") is not None:
                    row["number_of_children"] = int(person_data["numberOfChildren"])

                # Birth country/city/state
                if person_data.get("birthCountry"):
                    row["birth_country"] = str(person_data["birthCountry"])
                if person_data.get("birthCity"):
                    row["birth_city"] = str(person_data["birthCity"])
                if person_data.get("birthState"):
                    row["birth_state"] = str(person_data["birthState"])

                # Philanthropy score (from personLists billionaires entry)
                for entry in person_data.get("personLists", []):
                    if entry.get("listUri") == "billionaires" and entry.get("philanthropyScore") is not None:
                        row["philanthropy_score"] = int(entry["philanthropyScore"])
                        break

                if len(row) > 1:
                    profile_list.append(row)

                # Education (normalized)
                for i, edu in enumerate(person_data.get("educations", []), start=1):
                    education_list.append({
                        "person": person_id,
                        "education_order": i,
                        "school": edu.get("school", ""),
                        "degree": edu.get("degree", ""),
                    })

            except (json.JSONDecodeError, KeyError):
                continue

        if profile_list:
            profile_df = pd.DataFrame(profile_list).sort_values("person")
            profile_df.to_csv(
                os.path.join(output_dir, "ddf--entities--person--profile.csv"),
                index=False,
            )
            counts = {col: profile_df[col].notna().sum() for col in profile_df.columns if col != "person"}
            print(f"Profile data: {len(profile_df)} rows — " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

        if education_list:
            education_df = pd.DataFrame(education_list).sort_values(["person", "education_order"])
            education_df.to_csv(
                os.path.join(output_dir, "ddf--entities--person--education.csv"),
                index=False,
            )
            print(f"Education data: {len(education_df)} rows ({education_df['person'].nunique()} persons)")
    else:
        print(f"WARNING: {profiles_dir} not found, skipping profile extraction")


if __name__ == "__main__":
    transform_forbes_data("../source/forbes", "../intermediate/forbes")
