"""
update_source_books.py

Downloads Open Library work metadata for each unique ol_work_id in book_map.csv.

Caches to: source/books/ol/{work_id}.json
Resume-capable: skips existing files.

Usage:
  cd etl/scripts
  python update_source_books.py
"""

import csv
import json
import os
import time

import requests

DELAY = 0.5

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
BOOK_MAP = os.path.join(BASE_DIR, 'source', 'books', 'book_map.csv')
OL_DIR = os.path.join(BASE_DIR, 'source', 'books', 'ol')


def load_work_ids():
    """Load unique ol_work_ids from book_map.csv."""
    ids = set()
    with open(BOOK_MAP, newline='') as f:
        for row in csv.DictReader(f):
            wid = row.get('ol_work_id', '').strip()
            if wid:
                ids.add(wid)
    return sorted(ids)


def download_work(work_id):
    """Download Open Library work JSON."""
    url = f"https://openlibrary.org/works/{work_id}.json"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
    except (requests.RequestException, ValueError):
        pass
    return None


def main():
    print("Loading book map...", flush=True)
    work_ids = load_work_ids()
    print(f"  {len(work_ids)} unique Open Library work IDs")

    os.makedirs(OL_DIR, exist_ok=True)
    downloaded = 0
    skipped = 0
    failed = 0

    for i, wid in enumerate(work_ids):
        out_path = os.path.join(OL_DIR, f"{wid}.json")
        if os.path.exists(out_path):
            skipped += 1
            continue

        data = download_work(wid)
        time.sleep(DELAY)

        if data:
            with open(out_path, 'w') as f:
                json.dump(data, f, indent=1)
            downloaded += 1
        else:
            failed += 1

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(work_ids)} processed ({downloaded} new, {skipped} existing, {failed} failed)", flush=True)

    print(f"\nDone: {downloaded} downloaded, {skipped} existing, {failed} failed")
    print(f"Cache: {OL_DIR}/")


if __name__ == '__main__':
    main()
