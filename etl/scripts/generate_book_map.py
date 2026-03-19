"""
generate_book_map.py

Generates source/books/book_map.csv — mapping between persons and their
autobiography/biography books, resolved to Open Library work IDs.

Reads:
  - billionaire_survey_augmented_fixed.xlsx
    Col 119: Autobiography — "Title (Year)"
    Col 120: Biography — "Title (Author, Year), Title (Author, Year), ..."
    Col 128: Gapminder Slug
    Col 2: Name

  - Open Library Search API for work ID resolution

Output:
  - source/books/book_map.csv with columns:
    person, book_type, title, author, year, ol_work_id

Resume-capable: if book_map.csv exists, skips rows already resolved.

Usage:
  cd etl/scripts
  python generate_book_map.py
"""

import csv
import os
import re
import time
import urllib.parse

import openpyxl
import requests

XLSX = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'billionaire_survey_augmented_fixed.xlsx')
OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'source', 'books', 'book_map.csv')

NAME_COL = 2
AUTOBIOGRAPHY_COL = 119
BIOGRAPHY_COL = 120
GAPMINDER_SLUG_COL = 128

OL_SEARCH_URL = "https://openlibrary.org/search.json"
DELAY = 0.5  # rate limit


def parse_autobiography(raw):
    """Parse 'Title (Year)' → (title, year) or None."""
    if not raw:
        return None
    s = str(raw).strip()
    if s.lower() in ('', 'n', 'none', 'n/a'):
        return None
    m = re.match(r'^(.+?)\s*\((\d{4})\)$', s)
    if m:
        return (m.group(1).strip(), int(m.group(2)))
    return None


def parse_biographies(raw):
    """Parse 'Title (Author, Year), Title (Author, Year), ...' → list of (title, author, year)."""
    if not raw:
        return []
    s = str(raw).strip()
    if s.lower() in ('', 'n', 'none', 'n/a'):
        return []

    # Split on '), ' followed by an uppercase letter (start of next book title)
    parts = re.split(r'\),\s*(?=[A-Z])', s)
    results = []
    for part in parts:
        part = part.strip()
        if not part.endswith(')'):
            part += ')'
        m = re.match(r'^(.+?)\s*\(([^,]+),\s*(\d{4})\)$', part)
        if m:
            results.append((m.group(1).strip(), m.group(2).strip(), int(m.group(3))))
    return results


def search_ol_autobiography(title, year):
    """Search Open Library for an autobiography by title, match by year ±2."""
    params = {'title': title, 'limit': 5}
    try:
        r = requests.get(OL_SEARCH_URL, params=params, timeout=15)
        if r.status_code != 200:
            return None
        docs = r.json().get('docs', [])
        for doc in docs:
            work_key = doc.get('key', '')  # e.g. /works/OL17825802W
            pub_year = doc.get('first_publish_year')
            if pub_year and abs(pub_year - year) <= 2:
                work_id = work_key.split('/')[-1]
                return work_id
        # If no year match, try title-only best match
        if docs:
            work_key = docs[0].get('key', '')
            return work_key.split('/')[-1]
    except (requests.RequestException, ValueError, KeyError):
        pass
    return None


def search_ol_biography(title, author, year):
    """Search Open Library for a biography by title+author, match by year ±2."""
    params = {'title': title, 'author': author, 'limit': 5}
    try:
        r = requests.get(OL_SEARCH_URL, params=params, timeout=15)
        if r.status_code != 200:
            return None
        docs = r.json().get('docs', [])
        for doc in docs:
            work_key = doc.get('key', '')
            pub_year = doc.get('first_publish_year')
            if pub_year and abs(pub_year - year) <= 2:
                return work_key.split('/')[-1]
        # Fallback: title-only search
        if not docs:
            params2 = {'title': title, 'limit': 5}
            r2 = requests.get(OL_SEARCH_URL, params=params2, timeout=15)
            time.sleep(DELAY)
            if r2.status_code == 200:
                docs = r2.json().get('docs', [])
                for doc in docs:
                    work_key = doc.get('key', '')
                    pub_year = doc.get('first_publish_year')
                    if pub_year and abs(pub_year - year) <= 2:
                        return work_key.split('/')[-1]
        if docs:
            return docs[0].get('key', '').split('/')[-1]
    except (requests.RequestException, ValueError, KeyError):
        pass
    return None


def load_existing():
    """Load existing book_map.csv for resume capability. Returns set of (person, book_type, title) tuples."""
    existing = {}
    if os.path.exists(OUTPUT):
        with open(OUTPUT, newline='') as f:
            for row in csv.DictReader(f):
                key = (row['person'], row['book_type'], row['title'])
                existing[key] = row
    return existing


def main():
    print("Loading Excel...", flush=True)
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    ws = wb.active

    existing = load_existing()
    if existing:
        print(f"  Resuming: {len(existing)} entries already in book_map.csv")

    # Parse all book entries from spreadsheet
    all_books = []  # list of (person, book_type, title, author, year)
    no_slug = 0

    for row in ws.iter_rows(min_row=3, values_only=False):
        name = row[NAME_COL - 1].value
        if not name:
            continue

        slug = row[GAPMINDER_SLUG_COL - 1].value
        if not slug or not str(slug).strip():
            no_slug += 1
            continue
        slug = str(slug).strip()
        person_name = str(name).strip()

        # Autobiography
        auto_raw = row[AUTOBIOGRAPHY_COL - 1].value
        parsed = parse_autobiography(auto_raw)
        if parsed:
            title, year = parsed
            all_books.append((slug, 'autobiography', title, '', year))

        # Biography
        bio_raw = row[BIOGRAPHY_COL - 1].value
        bios = parse_biographies(bio_raw)
        for title, author, year in bios:
            all_books.append((slug, 'biography', title, author, year))

    wb.close()

    print(f"  Parsed {len(all_books)} book entries ({len([b for b in all_books if b[1] == 'autobiography'])} autobiographies, {len([b for b in all_books if b[1] == 'biography'])} biographies)")
    print(f"  Skipped: {no_slug} rows with no Gapminder slug")

    # Resolve via Open Library
    rows_out = []
    resolved = 0
    unresolved = 0
    skipped = 0

    for i, (person, book_type, title, author, year) in enumerate(all_books):
        key = (person, book_type, title)

        # Resume: skip if already resolved with an OL ID
        if key in existing and existing[key].get('ol_work_id'):
            rows_out.append(existing[key])
            skipped += 1
            continue

        # Search Open Library
        if book_type == 'autobiography':
            ol_id = search_ol_autobiography(title, year)
        else:
            ol_id = search_ol_biography(title, author, year)

        time.sleep(DELAY)

        row_dict = {
            'person': person,
            'book_type': book_type,
            'title': title,
            'author': author,
            'year': str(year),
            'ol_work_id': ol_id or '',
        }
        rows_out.append(row_dict)

        if ol_id:
            resolved += 1
        else:
            unresolved += 1
            print(f"  UNRESOLVED: {person} / {book_type} / {title} ({year})")

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i+1}/{len(all_books)} ({resolved} resolved, {unresolved} unresolved, {skipped} skipped)", flush=True)

    # Write output
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['person', 'book_type', 'title', 'author', 'year', 'ol_work_id'])
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"\nWrote {OUTPUT}")
    print(f"  {len(rows_out)} rows total")
    print(f"  {resolved} newly resolved, {skipped} already resolved, {unresolved} unresolved")


if __name__ == '__main__':
    main()
