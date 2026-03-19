# -*- coding: utf-8 -*-

import requests as req
import pandas as pd
import json
import os
import sys
import time


url_tmpl = "https://www.forbes.com/ajax/list/data?year={}&uri=billionaires&type=person"
profile_url_tmpl = "https://www.forbes.com/forbesapi/person/{uri}.json"


def get_data(year):
    res = req.get(url_tmpl.format(year)).json()
    df = pd.DataFrame.from_records(res)
    return df


def fetch_profiles():
    """Download Forbes profile JSONs for all persons in the entities file."""
    entities_file = "../intermediate/forbes/ddf--entities--person.csv"
    output_dir = "../source/forbes/profiles"

    if not os.path.exists(entities_file):
        print(f"ERROR: {entities_file} not found. Run forbes_data_transformation.py first.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(entities_file)
    # Convert person_id back to URI format (underscores → hyphens)
    uris = df["person"].str.replace("_", "-").tolist()

    total = len(uris)
    skipped = 0
    fetched = 0
    failed = 0

    for i, uri in enumerate(uris):
        out_path = os.path.join(output_dir, f"{uri}.json")
        if os.path.exists(out_path):
            skipped += 1
            continue

        url = profile_url_tmpl.format(uri=uri)
        try:
            resp = req.get(url, timeout=30)
            if resp.status_code == 200:
                # Validate JSON
                json.loads(resp.text)
                with open(out_path, "w") as f:
                    f.write(resp.text)
                fetched += 1
            else:
                failed += 1
                print(f"  {uri}: HTTP {resp.status_code}")
        except Exception as e:
            failed += 1
            print(f"  {uri}: {e}")

        if (i + 1) % 50 == 0:
            print(f"  progress: {i + 1}/{total} (fetched={fetched}, skipped={skipped}, failed={failed})")

        time.sleep(0.5)

    print(f"Done: {total} total, {fetched} fetched, {skipped} skipped, {failed} failed")


def main():
    args = sys.argv
    if len(args) < 2:
        print("Usage: update_source_forbes.py <year|all|--profiles>")
        sys.exit(127)
    if args[1] == '--profiles':
        fetch_profiles()
    elif args[1] == 'all':
        for i in range(2002, 2026):
            print(f"downloading forbes {i}")
            df = get_data(i)
            df.to_csv(f'../source/forbes/{i}.csv', index=False)
    else:
        year = int(args[1])
        print(f"downloading forbes {year}")
        df = get_data(year)
        print(df.head())
        df.to_csv(f'../source/forbes/{year}.csv', index=False)


if __name__ == "__main__":
    main()
