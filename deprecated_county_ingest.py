import csv
import argparse
import pathlib
import logging
import os
import traceback
import datetime
import redis
from dotenv import load_dotenv
from multiprocessing import Pool
from services import Postgres
from psycopg2.extensions import AsIs
from constants import county_csv_headers, date_keys
from common import replace_bom, yes_no, pk_string, find_by_name_and_address, find_by_registration_number

"""
This script is idempotent when run on a directory corresponding to a given day.
We can ingest 10-08, then add a new CSV to 10-08 and then run it again without a problem.
However, we should never run 10-07 after running 10-08 even for "new" 10-07 data.

We must distinguish between three dates here:
- date of information: the date the county pulled the data
- received date: the date we received the data
- ingest date: the date we ingested the data

There is not really a right answer to how we organize this, however if CountyCSV_A was
pulled before CountyCSV_B and we ingest A after B we will ruin the integrity of the data.
However, we organize the data it should be with this in mind.
"""


def find_and_compare(cursor, row, stem, day):
    existing_row = None

    # try to find by registration number
    if row.get('registration_number'):
        existing_row = find_by_registration_number(cursor, row.get('registration_number'))

    # fallback on name and address
    if not existing_row:
        existing_row = find_by_name_and_address(cursor, row)

    reject_date = None

    if existing_row:
        logs = existing_row['logs']
        reject_date = existing_row['reject_date']
        cure_date = existing_row['cure_date']

        # remove our columns prior to comparison
        del existing_row['created_at']
        del existing_row['updated_at']
        del existing_row['logs']
        del existing_row['log']
        del existing_row['reject_date']
        del existing_row['cure_date']
        del existing_row['county_data']

        # convert dates back to strings for comparison with CSV data
        for key in date_keys:
            if existing_row.get(key):
                existing_row[key] = existing_row[key].strftime('%-m/%-d/%Y')
            if row.get(key):
                row[key] = datetime.datetime.strptime(row[key], '%m/%d/%Y').strftime('%-m/%-d/%Y')

        has_changed = False

        # iterate over key-values and log any differences
        for k, v in row.items():
            if existing_row[k] != v:
                has_changed = True
                logging.info(' | '.join(['UPDATE', pk_string(row), k, f'{existing_row[k]} => {row[k]}']))
                logs.append(' | '.join([f'{stem}.csv', f'{day}', 'UPDATE', k, f'{existing_row[k]} => {row[k]}']))

                if k == 'ballot_status' and existing_row[k] is None:
                    reject_date = f'2020-{day}'
                    # if a ballot status goes from null => rejected => null => rejected
                    # we want to remove the cure date since a cure date cannot coexist with a non-null status
                    cure_date = None
                elif k == 'ballot_status' and row[k] is None:
                    cure_date = f'2020-{day}'

        return has_changed, existing_row['id'], logs, reject_date, cure_date

    # a ballot can appear for the first time and be marked as rejected on the same CSV
    if row['ballot_status']:
        reject_date = f'2020-{day}'

    # initial log entry
    logging.info(' | '.join(['INSERT', pk_string(row)]))
    logs = [' | '.join([f'{stem}.csv', f'{day}', 'INSERT'])]
    return None, None, logs, reject_date, None


def insert_row(cursor, row, stem, day):
    # check the CSV headers
    csv_key_set = set(county_csv_headers.keys())

    for key in row.keys():
        if key not in csv_key_set:
            raise Exception(f'Unexpected CSV header: {stem} | {day} | {key}')

    row_dict = {}

    for csv_key, sql_key in county_csv_headers.items():
        # TODO: add more rigorous vetting for CSV data once enums are properly defined
        if csv_key == 'COUNTY':
            # if a county does not appear in the data set use the name of the file instead
            # some county data has other counties listed, which is why we do not simply use the path stem
            row_dict[sql_key] = row.get(csv_key) or stem
        elif csv_key == 'PARTY':
            # consolidate all non Dems and Reps under the banner of Other
            if row.get(csv_key) != 'DEM' and row.get(csv_key) != 'REP':
                row_dict[sql_key] = 'OTH'
            else:
                row_dict[sql_key] = row.get(csv_key)
        elif row.get(csv_key) is not None and csv_key == 'IS_VOID':
            # int => bool
            row_dict[sql_key] = row.get(csv_key) == '1'
        elif row.get(csv_key) is not None and csv_key == 'REGN_NUM':
            # consider any non-integer string an error and replace with None
            try:
                row_dict[sql_key] = int(row.get(csv_key))
            except ValueError:
                row_dict[sql_key] = None
        elif not row.get(csv_key) or not row.get(csv_key).strip():
            # replace falsy values for text fields with null
            row_dict[sql_key] = None
        else:
            row_dict[sql_key] = row.get(csv_key)

    # has_changed: None => new, need to insert; False => existing, no changes; True => existing, need to update
    has_changed, psql_id, logs, reject_date, cure_date = find_and_compare(cursor, row_dict, stem, day)

    # assign our data
    row_dict['logs'] = logs
    row_dict['log'] = '\n'.join(logs)
    row_dict['reject_date'] = reject_date
    row_dict['cure_date'] = cure_date
    row_dict['county_data'] = True

    # upsert row
    columns = row_dict.keys()
    values = tuple(row_dict.values())

    if has_changed:
        # we're only ok with a full overwrite here because of we're logging all changes
        # we can accept that any recent data is more reliable and should replace existing values
        # while at the same time tracking all changes in case we need to see what happened
        query = (
            'UPDATE voters '
            'SET (%s) = %s '
            'WHERE id = %s'
        )
        cursor.execute(query, (AsIs(','.join(columns)), values, psql_id))

    elif has_changed is None:
        query = (
            'INSERT INTO voters (%s) '
            'VALUES %s'
        )
        cursor.execute(query, (AsIs(','.join(columns)), values))


def clean_row(row):
    # remove leading and trailing whitespace from all keys and values
    clean = {}
    for key, value in row.items():
        clean[key.strip()] = " ".join(value.strip().split()) if type(value) is str else value
    return clean


def ingest_csv(args_tuple):
    # "postgres_args_" to not shadow the name under __main__ and because multiprocessing cannot share global vars
    path, day, postgres_args_, is_prod = args_tuple
    log_dir = 'logs' if is_prod else 'dev_logs'
    logging.basicConfig(filename=f'{log_dir}/{path.stem}-{day}.log', format='%(asctime)s | %(message)s', level=logging.INFO)

    # remove the leading BOM present in many Excel documents and CSVs exported from Excel
    replace_bom(path)

    with Postgres(**postgres_args_) as cursor:
        print(f'Processing {path.name}...')
        with open(path) as f:
            for row in csv.DictReader(f):
                try:
                    insert_row(cursor, clean_row(row), path.stem, day)
                except Exception as e:
                    tb = traceback.TracebackException.from_exception(e)
                    logging.error(f'ERROR | {"".join(tb.format())} | {str(row)}')

                if redis_client.get('kill_ingest'):
                    print('Kill switch detected...')
                    break

        print(f'Done with {path.name}...')


def main():
    with Pool(args.workers) as pool:
        pool.map(ingest_csv, [(path, args.day, postgres_args, is_prod) for path in pathlib.Path('csvs').joinpath(args.day).glob('*.csv')])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='day', required=True)
    parser.add_argument('-w', dest='workers', type=int, default=1)
    args = parser.parse_args()

    # ensure log dirs
    pathlib.Path('logs/').mkdir(exist_ok=True)
    pathlib.Path('dev_logs/').mkdir(exist_ok=True)

    load_dotenv()

    is_prod = yes_no('Target production?')
    if is_prod:
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

    redis_client = redis.StrictRedis(host='localhost', decode_responses=True)

    main()
