"""
generate_ticker_map.py

Generates source/edgar/ticker_map.csv — the join key between unified person IDs
and SEC EDGAR CIKs.

Reads:
  - billionaire_survey_augmented_fixed.xlsx (Col 52: Ticker, Col 128: Gapminder Slug)
  - SEC company_tickers.json (ticker → CIK resolution)

Output:
  - source/edgar/ticker_map.csv with columns: person, ticker, cik

Usage:
  cd etl/scripts
  python generate_ticker_map.py
"""

import csv
import re
import requests
import time
import os

import openpyxl

XLSX = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'billionaire_survey_augmented_fixed.xlsx')
OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'source', 'edgar', 'ticker_map.csv')
HEADERS = {'User-Agent': 'billionaire-survey-research admin@research.local'}

NAME_COL = 2
TICKER_COL = 52
GAPMINDER_SLUG_COL = 128


def clean_primary_ticker(raw):
    """Extract the primary ticker symbol from a possibly messy string."""
    if not raw:
        return None
    s = str(raw).strip()
    if re.search(r'\b(private|n/a|not public|no ticker)\b', s, re.I):
        return None
    s = re.sub(r'\([^)]*\)', '', s).strip()
    for part in re.split(r'[,;\s]+', s):
        t = part.strip().upper()
        if re.match(r'^[A-Z]{1,6}(\.[A-Z]{1,2})?$', t):
            return t
    return None


def get_ticker_cik_map():
    """Fetch SEC's official ticker → CIK mapping."""
    r = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=HEADERS, timeout=15
    )
    r.raise_for_status()
    return {v['ticker'].upper(): str(v['cik_str']).zfill(10) for v in r.json().values()}


def main():
    print("Loading Excel...", flush=True)
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    ws = wb.active

    print("Fetching SEC ticker→CIK map...", flush=True)
    ticker_cik = get_ticker_cik_map()
    print(f"  {len(ticker_cik):,} tickers in SEC database")

    rows = []
    skipped_no_slug = 0
    skipped_no_ticker = 0
    skipped_no_cik = 0

    for row in ws.iter_rows(min_row=3, values_only=False):
        name = row[NAME_COL - 1].value
        if not name:
            continue

        slug = row[GAPMINDER_SLUG_COL - 1].value
        if not slug or not str(slug).strip():
            skipped_no_slug += 1
            continue

        slug = str(slug).strip()
        ticker = clean_primary_ticker(row[TICKER_COL - 1].value)
        if not ticker:
            skipped_no_ticker += 1
            continue

        cik = ticker_cik.get(ticker) or ticker_cik.get(ticker.split('.')[0])
        if not cik:
            skipped_no_cik += 1
            continue

        rows.append((slug, ticker, cik))

    wb.close()

    # Deduplicate (same person+ticker+cik)
    rows = sorted(set(rows))

    unique_persons = len(set(r[0] for r in rows))
    unique_tickers = len(set(r[1] for r in rows))
    unique_ciks = len(set(r[2] for r in rows))

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['person', 'ticker', 'cik'])
        writer.writerows(rows)

    print(f"\nWrote {OUTPUT}")
    print(f"  {len(rows)} rows ({unique_persons} unique persons, {unique_tickers} unique tickers, {unique_ciks} unique CIKs)")
    print(f"  Skipped: {skipped_no_slug} no slug, {skipped_no_ticker} no ticker, {skipped_no_cik} no CIK in SEC")


if __name__ == '__main__':
    main()
