import csv
import argparse
import codecs
import pathlib
import os
import logging
from dotenv import load_dotenv
from constants import sos_csv_headers
from common import replace_bom
from services import Postgres


def create_chunks(iterable, n):
    chunks = []
    for i in range(0, len(iterable), n):
        chunks.append(iterable[i:i + n])
    return chunks


def mark_removed(cursor, row):
    logging.info(' | '.join([f'SoS-{args.day}.csv', 'REMOVE', 'registration_number', str(row['registration_number'])]))
    logs = row['logs'] + [' | '.join([f'SoS-{args.day}.csv', 'REMOVE'])]
    log = '\n'.join(logs)

    query = (
        'UPDATE voters '
        'SET logs = %s, log = %s, was_removed = true '
        'WHERE id = %s'
    )
    cursor.execute(query, (logs, log, row['id']))


def clean_rows(rows):
    headers = set(sos_csv_headers.keys())
    clean_rows_ = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if not k or k.strip() not in headers:
                continue
            # strip leading and trailing whitespace from all keys and values
            # reducing all inter-string whitespace to a single ' '
            clean[k.strip()] = ' '.join(v.strip().split()) if type(v) is str else v
        clean_rows_.append(clean)
    return clean_rows_


def main():
    # remove the leading BOM present in many Excel documents and CSVs exported from Excel
    replace_bom(f'csvs/sos/{args.day}.csv')

    stem = f'csvs/sos/{args.day}'
    voters = {}

    with codecs.open(f'{stem}.csv', encoding='utf-8', errors='ignore') as f:
        # load the entire file into memory and create a dictionary based on voter ID
        for row in csv.DictReader(f):
            if row.get('VOTER_ID'):
                # skip any row that does not provide a value for these fields
                if not all([row.get(pk) for pk in ['FIRST_NAME', 'LAST_NAME', 'RESIDENTIAL_ADDRESS_LINE_1']]):
                    continue
                try:
                    key = int(row.get('VOTER_ID'))
                    if key in voters:
                        voters[key] += [row]
                    else:
                        voters[key] = [row]
                except ValueError:
                    # skip any row that has a non-int for VOTER_ID
                    continue

    all_voter_ids = list(voters.keys())
    chunk_size = len(all_voter_ids) // number_of_chunks
    chunks = create_chunks(all_voter_ids, chunk_size + number_of_chunks)

    for i, chunk in enumerate(chunks):
        print(f'Writing {stem}_{i + 1}.csv...')
        with open(f'{stem}_{i + 1}.csv', 'w') as f:
            writer = csv.DictWriter(f, sos_csv_headers.keys())
            writer.writeheader()
            for voter_id in chunk:
                writer.writerows(clean_rows(voters[voter_id]))

    # get all voter IDs in the DB and check them against the CSV
    # we will rely on ingest_sos_chunk.py to set was_removed to false if a voter re-appears
    all_voter_ids_set = set(all_voter_ids)
    total = len(all_voter_ids_set)
    print('Checking for any removed voters...')
    i = 1
    with Postgres(**postgres_args) as cursor:
        query = 'SELECT id, registration_number, logs FROM voters'
        cursor.execute(query)
        for row in cursor.fetchall():
            print(f'Checking voter {i}...', end='\r')
            i += 1
            if dict(row)['registration_number'] not in all_voter_ids_set:
                mark_removed(cursor, dict(row))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='day', required=True)
    parser.add_argument('-n', dest='number_of_chunks', type=int, required=True)
    parser.add_argument('-p', dest='is_prod', action='store_true', default=False)
    args = parser.parse_args()
    number_of_chunks = args.number_of_chunks

    # ensure log dirs
    pathlib.Path('logs/').mkdir(exist_ok=True)
    pathlib.Path('dev_logs/').mkdir(exist_ok=True)

    load_dotenv()

    if args.is_prod:
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

    log_dir = 'logs' if args.is_prod else 'dev_logs'
    logging.basicConfig(filename=f'{log_dir}/SoS-{args.day}.log', format='%(asctime)s | %(message)s', level=logging.INFO)

    main()
