"""
edgar_data_transformation.py

Transforms raw SEC EDGAR source data into DDF intermediate files.

Input:
  source/edgar/xbrl/{cik}.json     — XBRL company facts
  source/edgar/proxy/{cik}.json    — Parsed DEF 14A proxy data
  source/edgar/ticker_map.csv      — person → ticker → CIK mapping

Output (intermediate/edgar/):
  ddf--entities--person.csv
    Person-level properties: ticker, cik, ipo_year, equity_stake_pct,
    voting_control_pct, total_comp_m, base_salary_k, stock_awards_m

  ddf--datapoints--revenue_m--by--person--time.csv
  ddf--datapoints--gross_margin_pct--by--person--time.csv
  ddf--datapoints--operating_margin_pct--by--person--time.csv
    Time-series financial data from 10-K annual filings.

Usage:
  cd etl/scripts
  python edgar_data_transformation.py
"""

import csv
import json
import os
import re

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
TICKER_MAP = os.path.join(BASE_DIR, 'source', 'edgar', 'ticker_map.csv')
XBRL_DIR = os.path.join(BASE_DIR, 'source', 'edgar', 'xbrl')
PROXY_DIR = os.path.join(BASE_DIR, 'source', 'edgar', 'proxy')
OUTPUT_DIR = os.path.join(BASE_DIR, 'intermediate', 'edgar')

REVENUE_TAGS = [
    'Revenues',
    'RevenueFromContractWithCustomerExcludingAssessedTax',
    'SalesRevenueNet',
    'SalesRevenueGoodsNet',
    'RevenuesNetOfInterestExpense',
    'NetRevenues',
    'TotalRevenues',
    'InterestAndDividendIncomeOperating',
    'RevenueFromContractWithCustomerIncludingAssessedTax',
]


def load_ticker_map():
    """
    Returns:
      cik_persons: {cik: [(person, ticker), ...]}
      person_tickers: {person: ticker}  (first ticker wins)
    """
    cik_persons = {}
    person_tickers = {}
    with open(TICKER_MAP, newline='') as f:
        for row in csv.DictReader(f):
            person, ticker, cik = row['person'], row['ticker'], row['cik']
            cik_persons.setdefault(cik, []).append((person, ticker))
            if person not in person_tickers:
                person_tickers[person] = ticker
    return cik_persons, person_tickers


# ── XBRL extraction ──────────────────────────────────────────────────────────

def extract_ipo_year(facts):
    """Extract IPO year from the earliest 10-K filing date."""
    us_gaap = facts.get('facts', {}).get('us-gaap', {})
    earliest_year = None
    # Check a common tag for earliest filing
    for tag in REVENUE_TAGS:
        if tag not in us_gaap:
            continue
        try:
            for unit_entries in us_gaap[tag].get('units', {}).values():
                for entry in unit_entries:
                    if entry.get('form') in ('10-K', '10-K/A') and entry.get('fp') == 'FY':
                        year = int(entry['end'][:4])
                        if earliest_year is None or year < earliest_year:
                            earliest_year = year
        except (KeyError, TypeError, ValueError):
            continue
    return earliest_year


def extract_financial_timeseries(facts):
    """
    Extract annual revenue, gross margin, and operating margin time series.
    Returns: {year: {revenue_m, gross_margin_pct, operating_margin_pct}}

    Matches all metrics to the same filing (by accession number, then end date).
    """
    us_gaap = facts.get('facts', {}).get('us-gaap', {})
    results = {}

    # Step 1: collect all annual revenue entries
    revenue_entries = []
    for tag in REVENUE_TAGS:
        if tag not in us_gaap:
            continue
        try:
            entries = [
                x for x in us_gaap[tag]['units']['USD']
                if x.get('form') in ('10-K', '10-K/A') and x.get('fp') == 'FY'
                and x.get('val', 0) > 0
            ]
            if entries:
                revenue_entries = entries
                break  # use the first matching tag
        except (KeyError, TypeError):
            continue

    if not revenue_entries:
        return {}

    # Deduplicate by end date (keep latest accession)
    by_end = {}
    for entry in revenue_entries:
        end = entry['end']
        if end not in by_end or entry.get('accn', '') >= by_end[end].get('accn', ''):
            by_end[end] = entry

    def get_same_period_val(tag, ref_accn, ref_end):
        """Get value from the same 10-K filing as the reference revenue."""
        if tag not in us_gaap:
            return None
        try:
            units = us_gaap[tag]['units']['USD']
            if ref_accn:
                matches = [x for x in units if x.get('accn') == ref_accn]
                if matches:
                    fy = [x for x in matches if x.get('fp') == 'FY']
                    return (fy or matches)[-1]['val']
            matches = [
                x for x in units
                if x.get('end') == ref_end and x.get('form') in ('10-K', '10-K/A')
            ]
            if matches:
                fy = [x for x in matches if x.get('fp') == 'FY']
                return (fy or matches)[-1]['val']
        except (KeyError, TypeError):
            pass
        return None

    for end_date, rev_entry in by_end.items():
        try:
            year = int(end_date[:4])
        except (ValueError, TypeError):
            continue

        revenue_val = rev_entry['val']
        ref_accn = rev_entry.get('accn')

        row = {'revenue_m': round(revenue_val / 1e6, 1)}

        gross_profit = get_same_period_val('GrossProfit', ref_accn, end_date)
        if gross_profit is not None:
            gm = gross_profit / revenue_val * 100
            if -100 <= gm <= 100:
                row['gross_margin_pct'] = round(gm, 1)

        op_income = get_same_period_val('OperatingIncomeLoss', ref_accn, end_date)
        if op_income is not None:
            om = op_income / revenue_val * 100
            if -10000 <= om <= 10000:
                row['operating_margin_pct'] = round(om, 1)

        results[year] = row

    return results


# ── Proxy extraction ─────────────────────────────────────────────────────────

def name_match_score(cell_text, target_name):
    """Score how well a table cell matches a target person's name."""
    cell = cell_text.lower().strip()
    parts = target_name.lower().split()
    if not parts:
        return 0
    last = parts[-1]
    if last not in cell:
        return 0
    first = parts[0]
    score = 1
    if first in cell or (len(first) > 1 and first[:3] in cell):
        score += 1
    return score


def extract_proxy_data(proxy_json, person_name):
    """
    Match a person's name against parsed proxy tables.
    Returns dict with: equity_stake_pct, voting_control_pct, total_comp_m,
                        base_salary_k, stock_awards_m
    """
    result = {}

    # Compensation
    comp_rows = proxy_json.get('compensation_rows', [])
    best_score = 0
    best_comp = None
    for row in comp_rows:
        name_cell = row.get('name_cell', '')
        score = name_match_score(name_cell, person_name)
        if score > best_score:
            best_score = score
            best_comp = row

    if best_comp and best_score > 0:
        if 'total_m' in best_comp:
            result['total_comp_m'] = best_comp['total_m']
        if 'salary_k' in best_comp:
            result['base_salary_k'] = best_comp['salary_k']
        if 'stock_awards_m' in best_comp:
            result['stock_awards_m'] = best_comp['stock_awards_m']

    # Ownership
    ownership_rows = proxy_json.get('ownership_rows', [])
    best_score = 0
    best_own = None
    for row in ownership_rows:
        name_cell = row.get('name_cell', '')
        score = name_match_score(name_cell, person_name)
        if score > best_score:
            best_score = score
            best_own = row

    if best_own and best_score > 0:
        if 'stake_pct' in best_own:
            result['equity_stake_pct'] = best_own['stake_pct']
        if 'voting_pct' in best_own:
            result['voting_control_pct'] = best_own['voting_pct']

    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Loading ticker map...", flush=True)
    cik_persons, person_tickers = load_ticker_map()
    all_ciks = sorted(cik_persons.keys())
    print(f"  {len(all_ciks)} CIKs, {len(person_tickers)} unique persons")

    # Build name lookup: person_slug → human name (for proxy matching)
    # Read from ticker_map + derive from slug
    def slug_to_name(slug):
        """Convert 'elon_musk' to 'Elon Musk'."""
        return ' '.join(w.capitalize() for w in slug.split('_'))

    # Collect data
    entity_rows = {}  # person → {ticker, cik, ipo_year, ...}
    revenue_rows = []  # (person, year, revenue_m)
    gm_rows = []  # (person, year, gross_margin_pct)
    om_rows = []  # (person, year, operating_margin_pct)

    xbrl_found = 0
    proxy_found = 0

    for cik in all_ciks:
        persons_for_cik = cik_persons[cik]

        # ── XBRL ──
        xbrl_path = os.path.join(XBRL_DIR, f"{cik}.json")
        timeseries = {}
        ipo_year = None
        if os.path.exists(xbrl_path):
            with open(xbrl_path) as f:
                facts = json.load(f)
            timeseries = extract_financial_timeseries(facts)
            ipo_year = extract_ipo_year(facts)
            if timeseries:
                xbrl_found += 1

        # ── Proxy ──
        proxy_path = os.path.join(PROXY_DIR, f"{cik}.json")
        proxy_json = None
        if os.path.exists(proxy_path):
            with open(proxy_path) as f:
                proxy_json = json.load(f)
            proxy_found += 1

        # ── Assign to persons ──
        for person, ticker in persons_for_cik:
            name = slug_to_name(person)

            # Entity data
            entity = {'ticker': ticker, 'cik': cik}
            if ipo_year:
                entity['ipo_year'] = ipo_year

            # Proxy data
            if proxy_json:
                proxy_data = extract_proxy_data(proxy_json, name)
                entity.update(proxy_data)

            entity_rows[person] = entity

            # Timeseries data
            for year, metrics in timeseries.items():
                if 'revenue_m' in metrics:
                    revenue_rows.append((person, year, metrics['revenue_m']))
                if 'gross_margin_pct' in metrics:
                    gm_rows.append((person, year, metrics['gross_margin_pct']))
                if 'operating_margin_pct' in metrics:
                    om_rows.append((person, year, metrics['operating_margin_pct']))

    # ── Write outputs ──
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Entities
    entity_cols = ['person', 'ticker', 'cik', 'ipo_year', 'equity_stake_pct',
                   'voting_control_pct', 'total_comp_m', 'base_salary_k', 'stock_awards_m']
    entity_path = os.path.join(OUTPUT_DIR, 'ddf--entities--person.csv')
    with open(entity_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=entity_cols, extrasaction='ignore')
        writer.writeheader()
        for person in sorted(entity_rows.keys()):
            row = {'person': person}
            row.update(entity_rows[person])
            writer.writerow(row)

    entities_with_data = sum(1 for e in entity_rows.values() if len(e) > 2)
    print(f"\nEntities: {len(entity_rows)} total, {entities_with_data} with proxy/financial data")

    # Fill rate stats
    for col in entity_cols[1:]:
        filled = sum(1 for e in entity_rows.values() if e.get(col) is not None and e.get(col) != '')
        print(f"  {col}: {filled}/{len(entity_rows)}")

    # Revenue datapoints
    revenue_path = os.path.join(OUTPUT_DIR, 'ddf--datapoints--revenue_m--by--person--time.csv')
    with open(revenue_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['person', 'time', 'revenue_m'])
        for row in sorted(revenue_rows):
            writer.writerow(row)
    print(f"\nRevenue datapoints: {len(revenue_rows)} rows ({len(set(r[0] for r in revenue_rows))} persons)")

    # Gross margin datapoints
    gm_path = os.path.join(OUTPUT_DIR, 'ddf--datapoints--gross_margin_pct--by--person--time.csv')
    with open(gm_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['person', 'time', 'gross_margin_pct'])
        for row in sorted(gm_rows):
            writer.writerow(row)
    print(f"Gross margin datapoints: {len(gm_rows)} rows ({len(set(r[0] for r in gm_rows))} persons)")

    # Operating margin datapoints
    om_path = os.path.join(OUTPUT_DIR, 'ddf--datapoints--operating_margin_pct--by--person--time.csv')
    with open(om_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['person', 'time', 'operating_margin_pct'])
        for row in sorted(om_rows):
            writer.writerow(row)
    print(f"Operating margin datapoints: {len(om_rows)} rows ({len(set(r[0] for r in om_rows))} persons)")

    print(f"\nXBRL files with data: {xbrl_found}/{len(all_ciks)}")
    print(f"Proxy files loaded: {proxy_found}/{len(all_ciks)}")
    print(f"\nOutput written to {OUTPUT_DIR}/")


if __name__ == '__main__':
    main()
