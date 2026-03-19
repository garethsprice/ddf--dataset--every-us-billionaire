"""
book_data_transformation.py

Transforms book_map.csv into DDF intermediate person-level properties.

Input:
  source/books/book_map.csv

Output (intermediate/books/):
  ddf--entities--person.csv
    Person-level book properties: autobiography, biography

Format:
  autobiography: "Title (Year)" or empty
  biography: semicolon-separated "Title (Author, Year)" entries or empty

Usage:
  cd etl/scripts
  python book_data_transformation.py
"""

import csv
import os
from collections import defaultdict

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
BOOK_MAP = os.path.join(BASE_DIR, 'source', 'books', 'book_map.csv')
OUTPUT_DIR = os.path.join(BASE_DIR, 'intermediate', 'books')


def main():
    print("Loading book map...", flush=True)

    # Collect books by person
    autobiographies = {}  # person → formatted string
    biographies = defaultdict(list)  # person → list of formatted strings

    with open(BOOK_MAP, newline='') as f:
        for row in csv.DictReader(f):
            person = row['person']
            book_type = row['book_type']
            title = row['title']
            author = row.get('author', '')
            year = row['year']

            if book_type == 'autobiography':
                autobiographies[person] = f"{title} ({year})"
            elif book_type == 'biography':
                biographies[person].append(f"{title} ({author}, {year})")

    # Merge into person-level rows
    all_persons = sorted(set(list(autobiographies.keys()) + list(biographies.keys())))

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'ddf--entities--person.csv')

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['person', 'autobiography', 'biography'])
        writer.writeheader()
        for person in all_persons:
            writer.writerow({
                'person': person,
                'autobiography': autobiographies.get(person, ''),
                'biography': '; '.join(biographies.get(person, [])),
            })

    auto_count = len(autobiographies)
    bio_count = sum(1 for v in biographies.values() if v)
    print(f"\nWrote {output_path}")
    print(f"  {len(all_persons)} persons total")
    print(f"  {auto_count} with autobiography, {bio_count} with biography")


if __name__ == '__main__':
    main()
