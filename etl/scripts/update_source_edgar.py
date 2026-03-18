"""
update_source_edgar.py

Downloads raw SEC EDGAR source data for each CIK in ticker_map.csv:

  a) XBRL Company Facts → source/edgar/xbrl/{cik}.json
     Full historical financial data (revenue, costs, margins from 10-K filings)

  b) Proxy Filing Data → source/edgar/proxy/{cik}.json
     Parsed compensation + ownership tables from latest DEF 14A filing

Resume-capable: skips CIKs that already have source files.

Usage:
  cd etl/scripts
  python update_source_edgar.py              # download all
  python update_source_edgar.py --xbrl-only  # only XBRL data
  python update_source_edgar.py --proxy-only # only proxy data
"""

import argparse
import csv
import json
import os
import re
import time

import requests
from bs4 import BeautifulSoup

HEADERS = {'User-Agent': 'billionaire-survey-research admin@research.local'}
DELAY = 0.12  # 10 req/sec SEC rate limit
MAX_DOC_SIZE = 5_000_000  # skip proxy documents > 5MB

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
TICKER_MAP = os.path.join(BASE_DIR, 'source', 'edgar', 'ticker_map.csv')
XBRL_DIR = os.path.join(BASE_DIR, 'source', 'edgar', 'xbrl')
PROXY_DIR = os.path.join(BASE_DIR, 'source', 'edgar', 'proxy')


def load_ciks():
    """Load unique CIKs from ticker_map.csv."""
    ciks = set()
    with open(TICKER_MAP, newline='') as f:
        for row in csv.DictReader(f):
            ciks.add(row['cik'])
    return sorted(ciks)


# ── XBRL download ────────────────────────────────────────────────────────────

def download_xbrl(cik):
    """Download XBRL company facts JSON for a CIK."""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 200:
        return r.json()
    return None


def download_all_xbrl(ciks):
    """Download XBRL data for all CIKs, skipping existing files."""
    os.makedirs(XBRL_DIR, exist_ok=True)
    total = len(ciks)
    downloaded = 0
    skipped = 0
    failed = 0

    for i, cik in enumerate(ciks):
        out_path = os.path.join(XBRL_DIR, f"{cik}.json")
        if os.path.exists(out_path):
            skipped += 1
            continue

        data = download_xbrl(cik)
        time.sleep(DELAY)

        if data:
            with open(out_path, 'w') as f:
                json.dump(data, f)
            downloaded += 1
        else:
            failed += 1

        if (i + 1) % 50 == 0:
            print(f"  XBRL: {i+1}/{total} processed ({downloaded} new, {skipped} existing, {failed} failed)", flush=True)

    print(f"  XBRL done: {downloaded} downloaded, {skipped} existing, {failed} failed")


# ── Proxy download ────────────────────────────────────────────────────────────

def get_latest_def14a(cik):
    """Returns (accession_number, filing_date) for the most recent DEF 14A."""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r = requests.get(url, headers=HEADERS, timeout=15)
    time.sleep(DELAY)
    if r.status_code != 200:
        return None, None
    data = r.json()
    recent = data.get('filings', {}).get('recent', {})
    forms = recent.get('form', [])
    dates = recent.get('filingDate', [])
    accns = recent.get('accessionNumber', [])
    for form, date, accn in zip(forms, dates, accns):
        if form == 'DEF 14A':
            return accn, date
    return None, None


def get_proxy_html(cik, accn):
    """Downloads the main HTML document from a DEF 14A filing."""
    acc_no_dashes = accn.replace('-', '')
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no_dashes}"

    index_url = f"{base}/{accn}-index.html"
    r = requests.get(index_url, headers=HEADERS, timeout=15)
    time.sleep(DELAY)
    if r.status_code != 200:
        return None

    try:
        soup = BeautifulSoup(r.text, 'lxml')
        doc_name = None
        doc_size = 0
        for tr in soup.find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            if len(cells) < 4:
                continue
            type_text = cells[3].get_text(strip=True) if len(cells) > 3 else ''
            if 'DEF 14A' in type_text or 'def14a' in type_text.lower():
                raw_name = cells[2].get_text(strip=True).replace('iXBRL', '').strip()
                if not re.search(r'\.html?$', raw_name, re.I):
                    continue
                size_text = cells[4].get_text(strip=True) if len(cells) > 4 else '0'
                try:
                    size = int(re.sub(r'[^\d]', '', size_text))
                except ValueError:
                    size = 0
                if size > doc_size:
                    doc_size = size
                    doc_name = raw_name

        if not doc_name:
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.lower().endswith(('.htm', '.html')) and 'index' not in href.lower():
                    links.append(href.split('/')[-1])
            if links:
                doc_name = links[0]

        if not doc_name:
            return None

        doc_url = f"{base}/{doc_name}"
        r2 = requests.get(doc_url, headers=HEADERS, timeout=45)
        time.sleep(DELAY)
        if r2.status_code == 200:
            if len(r2.content) > MAX_DOC_SIZE:
                return None
            return r2.text
    except Exception:
        pass
    return None


def clean_num(s):
    """Extract a float from a string like '$1,234,567' or '45.6%'."""
    s = re.sub(r'[,$%\s\u2014\u2013\u2014\u2013]', '', str(s))
    s = re.sub(r'\((\d[\d.]*)\)', r'-\1', s)
    try:
        return float(s)
    except ValueError:
        return None


def name_match_score(cell_text, target_name):
    """Score how well a table cell matches a target person's name."""
    cell = cell_text.lower().strip()
    parts = target_name.lower().split()
    last = parts[-1]
    if last not in cell:
        return 0
    first = parts[0]
    score = 1
    if first in cell or (len(first) > 1 and first[:3] in cell):
        score += 1
    return score


def parse_table_rows(table):
    """Extract rows from HTML table, collapsing iXBRL $ separator cells."""
    rows = []
    for tr in table.find_all('tr'):
        cells = []
        for td in tr.find_all(['td', 'th']):
            text = td.get_text(' ', strip=True)
            text = text.replace('\u200b', '').replace('\u00a0', ' ').strip()
            if text:
                cells.append(text)
        if cells:
            collapsed = []
            i = 0
            while i < len(cells):
                if cells[i].strip() == '$' and i + 1 < len(cells):
                    i += 1
                else:
                    collapsed.append(cells[i])
                    i += 1
            rows.append(collapsed)
    return rows


def find_header_indices(rows, keywords):
    """Find header row and return keyword → column_index mapping."""
    kw_re = {kw: re.compile(re.escape(kw), re.I) for kw in keywords}
    for row in rows[:5]:
        found = {}
        for kw, pattern in kw_re.items():
            for i, cell in enumerate(row):
                if pattern.search(cell):
                    found[kw] = i
                    break
        if found:
            return found
    return {}


def parse_compensation_tables(soup):
    """
    Parse ALL Summary Compensation Tables from proxy HTML.
    Returns list of dicts with keys: name_cell, salary_k, stock_awards_m, total_m, notes
    """
    def safe_get(row, col_i, max_tries=2):
        if col_i is None or col_i < 0:
            return None
        for offset in range(max_tries):
            idx = col_i + offset
            if idx >= len(row):
                return None
            val = row[idx].strip()
            if not val:
                continue
            if re.match(r'^\d+$', val) and 2 <= int(val) <= 99:
                continue
            if re.match(r'^[\(\d\)]+$', val) and '(' in val:
                continue
            return val
        return None

    all_rows = []
    for tbl in soup.find_all('table'):
        rows = parse_table_rows(tbl)
        if len(rows) < 2:
            continue
        header_map = find_header_indices(rows, [
            'Salary', 'Bonus', 'Stock Awards', 'Option Awards',
            'Non-Equity', 'Total', 'Year', 'Name'
        ])
        has_equity = 'Stock Awards' in header_map or 'Option Awards' in header_map
        if 'Name' not in header_map or 'Salary' not in header_map or not has_equity:
            continue

        salary_col_i = header_map.get('Salary')
        stock_col_i = header_map.get('Stock Awards') or header_map.get('Option Awards')
        total_col_i = header_map.get('Total')
        name_col_i = header_map.get('Name', 0)

        for row in rows[1:]:
            if not row or len(row) <= (name_col_i or 0):
                continue
            name_cell = row[name_col_i or 0]
            if not name_cell or len(name_cell) < 3:
                continue
            # Skip rows that look like year-only rows
            if re.match(r'^\d{4}$', name_cell.strip()):
                continue

            entry = {'name_cell': name_cell}

            sal_text = safe_get(row, salary_col_i)
            if sal_text:
                v = clean_num(sal_text)
                if v is not None:
                    entry['salary_k'] = round(v / 1000, 3)

            stk_text = safe_get(row, stock_col_i)
            if stk_text:
                v = clean_num(stk_text)
                if v is not None and v > 0:
                    entry['stock_awards_m'] = round(v / 1_000_000, 2)

            tot_text = safe_get(row, total_col_i)
            if tot_text:
                v = clean_num(tot_text)
                if v is not None and v > 0:
                    entry['total_m'] = round(v / 1_000_000, 2)

            if len(entry) > 1:  # has at least name + one value
                sal = entry.get('salary_k')
                if sal is not None and sal < 0.002:
                    entry['notes'] = '$1 salary / no cash comp'
                all_rows.append(entry)

    return all_rows


def parse_ownership_tables(soup):
    """
    Parse ALL ownership tables from proxy HTML.
    Returns list of dicts with keys: name_cell, stake_pct, voting_pct
    """
    def get_pct(row, col_i, max_tries=2):
        for offset in range(max_tries):
            idx = col_i + offset
            if idx >= len(row):
                return None
            raw = row[idx].strip()
            if raw in ('*', '\u2014', '-', ''):
                return None
            try:
                num = float(raw.replace(',', ''))
                if num == int(num) and 2 <= num <= 99 and '%' not in raw:
                    continue
            except (ValueError, TypeError):
                pass
            v = clean_num(raw)
            if v is not None and 0 < v <= 100:
                return round(v, 2)
        return None

    all_rows = []
    for tbl in soup.find_all('table'):
        rows = parse_table_rows(tbl)
        if len(rows) < 2:
            continue
        header_map = find_header_indices(rows, [
            'Name', 'Shares', 'Percent', 'Percentage', '% of', 'Voting'
        ])
        name_col_i = header_map.get('Name', 0)
        pct_col_i = header_map.get('Percent') or header_map.get('Percentage') or header_map.get('% of')
        voting_col_i = header_map.get('Voting')

        if 'Name' not in header_map or 'Shares' not in header_map or pct_col_i is None:
            continue

        for row in rows[1:]:
            if not row or len(row) <= (name_col_i or 0):
                continue
            name_cell = row[name_col_i or 0]
            if not name_cell or len(name_cell) < 3:
                continue

            entry = {'name_cell': name_cell}

            pct = get_pct(row, pct_col_i)
            if pct is not None:
                entry['stake_pct'] = pct

            if voting_col_i is not None:
                vpct = get_pct(row, voting_col_i)
                if vpct is not None:
                    entry['voting_pct'] = vpct

            if len(entry) > 1:
                all_rows.append(entry)

    return all_rows


def download_proxy(cik):
    """
    Download and parse DEF 14A proxy for a CIK.
    Returns structured JSON dict or None.
    """
    accn, date = get_latest_def14a(cik)
    if not accn:
        return None

    html = get_proxy_html(cik, accn)
    if not html:
        return None

    try:
        soup = BeautifulSoup(html, 'lxml')
    except Exception:
        try:
            soup = BeautifulSoup(html, 'html.parser')
        except Exception:
            return None

    comp_rows = parse_compensation_tables(soup)
    ownership_rows = parse_ownership_tables(soup)

    return {
        'cik': cik,
        'accession_number': accn,
        'filing_date': date,
        'compensation_rows': comp_rows,
        'ownership_rows': ownership_rows,
    }


def download_all_proxy(ciks):
    """Download proxy data for all CIKs, skipping existing files."""
    os.makedirs(PROXY_DIR, exist_ok=True)
    total = len(ciks)
    downloaded = 0
    skipped = 0
    no_proxy = 0
    failed = 0

    for i, cik in enumerate(ciks):
        out_path = os.path.join(PROXY_DIR, f"{cik}.json")
        if os.path.exists(out_path):
            skipped += 1
            continue

        data = download_proxy(cik)

        if data:
            with open(out_path, 'w') as f:
                json.dump(data, f, indent=1)
            downloaded += 1
        elif data is None:
            no_proxy += 1
        else:
            failed += 1

        if (i + 1) % 25 == 0:
            print(f"  Proxy: {i+1}/{total} processed ({downloaded} new, {skipped} existing, {no_proxy} no filing)", flush=True)

    print(f"  Proxy done: {downloaded} downloaded, {skipped} existing, {no_proxy} no DEF 14A, {failed} failed")


def main():
    parser = argparse.ArgumentParser(description='Download SEC EDGAR source data')
    parser.add_argument('--xbrl-only', action='store_true', help='Only download XBRL data')
    parser.add_argument('--proxy-only', action='store_true', help='Only download proxy data')
    args = parser.parse_args()

    do_xbrl = not args.proxy_only
    do_proxy = not args.xbrl_only

    print("Loading ticker map...", flush=True)
    ciks = load_ciks()
    print(f"  {len(ciks)} unique CIKs to process")

    if do_xbrl:
        print("\nDownloading XBRL company facts...", flush=True)
        download_all_xbrl(ciks)

    if do_proxy:
        print("\nDownloading proxy filings...", flush=True)
        download_all_proxy(ciks)

    print("\nDone.")


if __name__ == '__main__':
    main()
