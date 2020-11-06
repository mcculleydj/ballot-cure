import csv
import pathlib
import codecs
import os
from dotenv import load_dotenv
from services import Postgres
from common import yes_no
from ingest_county import find_by_name_and_address

"""
Checks names in the Polk CSV against either production or dev.
 
Compares earlier Polk CSVs to later ones and looks for any names that were removed.
Essentially, this is designed to answer the question: should we rely on these CSVs to
cure voters as well as mark them as rejected?
"""


def clean_row(row):
    clean = {}
    for key, value in row.items():
        clean[key.strip()] = " ".join(value.strip().split()) if type(value) is str else value
    return clean


def main():
    name_map = {}
    paths = list(pathlib.Path('csvs/polk').glob('*.csv'))

    for csv_file in paths:
        name_map[csv_file] = set()
        with codecs.open(csv_file, encoding='utf-8', errors='ignore') as f:
            name_map[csv_file] = set()
            for row in csv.DictReader(f):
                clean = clean_row(row)
                name_map[csv_file].add((clean['First'], clean['Last']))

                with Postgres(**postgres_args) as cursor:
                    if find_by_name_and_address(cursor, clean):
                        print((clean['First'], clean['Last']), 'found...')

    for i, p1 in enumerate(paths):
        for j, p2 in enumerate(paths):
            if j <= i:
                continue
            print(f'Names removed between {p1} and {p2}:', name_map[p1] - name_map[p2])


if __name__ == '__main__':
    load_dotenv()

    if yes_no('Target production?'):
        postgres_args = {
            'host': os.getenv('POSTGRES_HOST'),
            'port': int(os.getenv('POSTGRES_PORT')),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'dbname': os.getenv('POSTGRES_DB'),
        }
    else:
        postgres_args = {
            'host': os.getenv('DEV_POSTGRES_HOST'),
            'port': int(os.getenv('DEV_POSTGRES_PORT')),
            'user': os.getenv('DEV_POSTGRES_USER'),
            'password': os.getenv('DEV_POSTGRES_PASSWORD'),
            'dbname': os.getenv('DEV_POSTGRES_DB'),
        }

    main()
