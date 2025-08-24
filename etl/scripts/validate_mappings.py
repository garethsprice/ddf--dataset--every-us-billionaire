import json
import pandas as pd
from collections import defaultdict
import os

# Global file paths (relative to script directory)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, "..", "..")

HURUN_ENTITIES_FILE = os.path.join(
    BASE_DIR, "etl", "intermediate", "hurun", "ddf--entities--person.csv"
)
FORBES_ENTITIES_FILE = os.path.join(
    BASE_DIR, "etl", "intermediate", "forbes", "ddf--entities--person.csv"
)
MAPPING_FILE = os.path.join(BASE_DIR, "etl", "intermediate", "mapping.json")
HURUN_WEALTH_FILE = os.path.join(
    BASE_DIR, "etl", "intermediate", "hurun", "ddf--datapoints--wealth--by--person--year.csv"
)
FORBES_WORTH_FILE = os.path.join(
    BASE_DIR, "etl", "intermediate", "forbes", "ddf--datapoints--worth--by--person--year.csv"
)


def main():
    """
    Validates the billionaire mappings by checking for consistency, completeness,
    and duplicate data after unification.
    """
    print("Starting mapping validation...")

    # --- Load Source Entity IDs ---
    try:
        hurun_entities_df = pd.read_csv(HURUN_ENTITIES_FILE)
        forbes_entities_df = pd.read_csv(FORBES_ENTITIES_FILE)
        all_hurun_ids = set(hurun_entities_df["person"])
        all_forbes_ids = set(forbes_entities_df["person"])
    except FileNotFoundError as e:
        print(f"ERROR: Could not read entity file: {e}")
        return

    # --- Load Mapping File ---
    try:
        with open(MAPPING_FILE, "r") as f:
            mappings = json.load(f)
    except FileNotFoundError:
        print("ERROR: mapping.json not found.")
        return
    except json.JSONDecodeError:
        print("ERROR: Could not decode mapping.json.")
        return

    # --- Checks 1 & 2: ID existence and multiple mappings ---
    print("\n--- Checking for ID consistency and multiple mappings ---")
    mapped_hurun_ids = set()
    mapped_forbes_ids = set()
    hurun_to_unified = {}
    forbes_to_unified = {}

    for mapping in mappings:
        unified_id = mapping.get("unified_person_id", "N/A")

        for hurun_id in mapping.get("hurun_ids", []):
            if hurun_id in hurun_to_unified:
                print(
                    f"ERROR (Check 2): Hurun ID '{hurun_id}' is mapped to multiple unified IDs: '{hurun_to_unified[hurun_id]}' and '{unified_id}'"
                )
            hurun_to_unified[hurun_id] = unified_id
            mapped_hurun_ids.add(hurun_id)

        for forbes_id in mapping.get("forbes_ids", []):
            if forbes_id in forbes_to_unified:
                print(
                    f"ERROR (Check 2): Forbes ID '{forbes_id}' is mapped to multiple unified IDs: '{forbes_to_unified[forbes_id]}' and '{unified_id}'"
                )
            forbes_to_unified[forbes_id] = unified_id
            mapped_forbes_ids.add(forbes_id)

    # Check 1: IDs in mapping that don't exist in source files
    non_existent_hurun = mapped_hurun_ids - all_hurun_ids
    if non_existent_hurun:
        print(
            f"\nERROR (Check 1): The following Hurun IDs from mapping.json do not exist in the source entity file: {sorted(list(non_existent_hurun))}"
        )

    non_existent_forbes = mapped_forbes_ids - all_forbes_ids
    if non_existent_forbes:
        print(
            f"\nERROR (Check 1): The following Forbes IDs from mapping.json do not exist in the source entity file: {sorted(list(non_existent_forbes))}"
        )

    # --- Check 3: Uniqueness of unified IDs ---
    print("\n--- Checking for duplicate unified IDs ---")
    unified_ids = [mapping.get("unified_person_id", "N/A") for mapping in mappings]
    duplicate_unified_ids = set()
    seen = set()

    for unified_id in unified_ids:
        if unified_id in seen:
            duplicate_unified_ids.add(unified_id)
        else:
            seen.add(unified_id)

    if duplicate_unified_ids:
        print(f"ERROR (Check 3): The following unified IDs are duplicated: {sorted(list(duplicate_unified_ids))}")
    else:
        print("All unified IDs are unique.")

    # --- Check 4: Unmapped IDs from source files ---
    print("\n--- Checking for unmapped IDs from source files ---")
    unmapped_hurun = all_hurun_ids - mapped_hurun_ids
    if unmapped_hurun:
        print(
            f"WARNING (Check 4): The following Hurun IDs are not mapped to a unified ID: {sorted(list(unmapped_hurun))}"
        )
    else:
        print("All Hurun IDs are mapped.")

    unmapped_forbes = all_forbes_ids - mapped_forbes_ids
    if unmapped_forbes:
        print(
            f"WARNING (Check 4): The following Forbes IDs are not mapped to a unified ID: {sorted(list(unmapped_forbes))}"
        )
    else:
        print("All Forbes IDs are mapped.")

    # --- Check 5: Duplicate wealth data after unification ---
    print("\n--- Checking for duplicate wealth/worth data after unification ---")

    # Check Hurun data
    try:
        hurun_wealth_df = pd.read_csv(HURUN_WEALTH_FILE)
        hurun_wealth_df["unified_person_id"] = hurun_wealth_df["person"].map(hurun_to_unified)

        # We only care about duplicates where a unified_id is present
        hurun_wealth_df.dropna(subset=["unified_person_id"], inplace=True)

        hurun_duplicates = hurun_wealth_df[
            hurun_wealth_df.duplicated(subset=["unified_person_id", "year"], keep=False)
        ]
        if not hurun_duplicates.empty:
            print(
                "ERROR (Check 5): Found duplicate unified_person_id/year entries in Hurun wealth data:"
            )

            # Group by unified_person_id to show all duplicates for each person
            grouped_by_person = hurun_duplicates.groupby("unified_person_id")

            for unified_id, group in grouped_by_person:
                print(f"\n  Unified ID: '{unified_id}', duplicated data:")
                # Display the relevant columns in a clean format
                duplicate_data = group[["person", "year", "wealth"]].sort_values(["year", "person"])
                print(duplicate_data.to_string(index=False))
        else:
            print("No duplicate unified entries found in Hurun data.")
    except FileNotFoundError:
        print("WARNING: Hurun wealth data file not found. Skipping check.")

    # Check Forbes data
    try:
        forbes_worth_df = pd.read_csv(FORBES_WORTH_FILE)
        forbes_worth_df["unified_person_id"] = forbes_worth_df["person"].map(forbes_to_unified)

        # We only care about duplicates where a unified_id is present
        forbes_worth_df.dropna(subset=["unified_person_id"], inplace=True)

        forbes_duplicates = forbes_worth_df[
            forbes_worth_df.duplicated(subset=["unified_person_id", "year"], keep=False)
        ]
        if not forbes_duplicates.empty:
            print(
                "\nERROR (Check 5): Found duplicate unified_person_id/year entries in Forbes worth data:"
            )

            # Group by unified_person_id to show all duplicates for each person
            grouped_by_person = forbes_duplicates.groupby("unified_person_id")

            for unified_id, group in grouped_by_person:
                print(f"\n  Unified ID: '{unified_id}', duplicated data:")
                # Display the relevant columns in a clean format
                duplicate_data = group[["person", "year", "worth"]].sort_values(["year", "person"])
                print(duplicate_data.to_string(index=False))
        else:
            print("No duplicate unified entries found in Forbes data.")
    except FileNotFoundError:
        print("WARNING: Forbes worth data file not found. Skipping check.")

    print("\nValidation complete.")


if __name__ == "__main__":
    main()
