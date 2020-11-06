import csv
import argparse
import logging
import os
import codecs
import pandas as pd
import pathlib
import sys
import traceback
from fuzzywuzzy import fuzz
from dotenv import load_dotenv
from services import Postgres
from common import replace_bom, yes_no, pk_string, get_voter
from constants import display_names


def insert_county_id(cursor, row, address_start, registration_number):
    query = (
        'INSERT INTO county_ids (last_name, first_name, middle_name, address_start, registration_number) '
        'VALUES (%s, %s, %s, %s, %s)'
    )
    cursor.execute(query, (row['Last'], row['First'], row['Middle'], address_start, registration_number))


def search_by_address(cursor, query_args):
    query = (
        'SELECT registration_number '
        'FROM county_ids '
        'WHERE last_name = %s '
        'AND first_name = %s '
        'AND address_start = %s'
    )
    cursor.execute(query, query_args)
    return cursor.fetchone()


def search_by_name(cursor, query_args, middle_name):
    query = (
        'SELECT registration_number '
        'FROM county_ids '
        'WHERE last_name = %s '
        'AND first_name = %s '
    )
    if middle_name:
        query += 'AND middle_name = %s '
        query_args += (middle_name,)
    else:
        query += 'AND middle_name IS NULL '
    cursor.execute(query, query_args)
    return cursor.fetchone()


def get_voter_registration_number(cursor, match):
    if match:
        query = (
            'SELECT * '
            'FROM voters '
            'WHERE registration_number = %s'
        )
        cursor.execute(query, (dict(match)['registration_number'],))
        return dict(cursor.fetchone())


def ask_jeeves(cursor, row, rejected_address_start):
    while True:
        registration_number = input('Please provide the correct registration number for this voter (s to skip): ')
        if registration_number == 's':
            return None
        try:
            insert_county_id(cursor, row, rejected_address_start, int(registration_number))
            return get_voter(cursor, int(registration_number))
        except ValueError:
            print('Not a number')
        except Exception as e:
            tb = traceback.TracebackException.from_exception(e)
            logging.error(' | '.join(['ERROR', str(row), ''.join(tb.format())]))
            print(''.join(tb.format()))


def get_matches(matches, cursor, row, address):
    if len(matches) > 1:
        print('Rejected ballot matched more than one row:', row)
        return ask_jeeves(cursor, row, address)
    elif len(matches) == 0:
        print('Rejected ballot did not match any row:', row)
        return ask_jeeves(cursor, row, address)

    return matches[0]


def find_by_registration_number(cursor, registration_number):
        query = (
            'SELECT * '
            'FROM voters '
            'WHERE registration_number = %s '
        )

        cursor.execute(query, (registration_number,))
        result = cursor.fetchone()
        return dict(result) if result else None


def find_by_name_and_address(cursor, row):
    if row['Address'].strip() == '':
        match = search_by_name(cursor, (row['Last'], row['First']), row['Middle'])
    else:
        rejected_address_start = ' '.join(row['Address'].lower().split()[:2])
        match = search_by_address(cursor, (row['Last'], row['First'], rejected_address_start))

    existing_row = get_voter_registration_number(cursor, match)

    if existing_row:
        return existing_row
    if row['Address'].strip() == '':
        query = (
            'SELECT * '
            'FROM voters '
            'WHERE last_name = %s '
            'AND first_name = %s '
            'AND county = %s'
        )

        if row['Middle']:
            query += 'AND middle_name = %s '
            query_args =  (row['Last'], row['First'], county, row['Middle'])
        else:
            query += 'AND middle_name IS NULL '
            query_args =  (row['Last'], row['First'], county)

        cursor.execute(query, query_args)
        matches = [dict(existing_row) for existing_row in cursor.fetchall()]
        return get_matches(matches, cursor, row, '')

    query = (
        'SELECT * '
        'FROM voters '
        'WHERE last_name = %s '
        'AND first_name = %s '
        'AND county = %s'
    )
    cursor.execute(query, (row['Last'], row['First'], county))
    existing_rows = [dict(existing_row) for existing_row in cursor.fetchall()]

    matches = []

    for existing_row in existing_rows:
        existing_resident_address_start = ' '.join(existing_row['resident_address'].lower().split()[:2])
        # Polk CSV sometimes uses mailing address if one exists
        existing_mailing_address_start = ''
        if existing_row.get('mailing_address'):
            existing_mailing_address_start = ' '.join(existing_row['mailing_address'].lower().split()[:2])
        if existing_resident_address_start == rejected_address_start or existing_mailing_address_start == rejected_address_start:
            matches.append(existing_row)

        # use fuzzy matching to try and match on address
        if len(matches) != 1:
            resident_address = existing_row['resident_address'].lower() if existing_row['resident_address'] else ''
            mailing_address = existing_row['mailing_address'].lower() if existing_row['mailing_address'] else ''
            rejected_address = row['Address'].lower()
            if fuzz.partial_ratio(resident_address, rejected_address) >= 90 or fuzz.partial_ratio(mailing_address, rejected_address) >= 90:
                matches.append(existing_row)

    return get_matches(matches, cursor, row, rejected_address_start)


def set_rejected(cursor, row):
    if county == 'Des Moines':
        existing_row = find_by_registration_number(cursor, row['registration_number'])
    else:
        existing_row = find_by_name_and_address(cursor, row)

    if not existing_row:
        return -1

    logs = existing_row['logs']

    if not existing_row['ballot_status']:
        reject_date = f'2020-{args.day}'
        # if a ballot status goes from null => rejected => null => rejected
        # we want to remove the cure date since a cure date cannot coexist with a non-null ballot status
        cure_date = None
        logging.info(' | '.join(['UPDATE', pk_string(existing_row), display_names['ballot_status'], f'None => {row["situation"]}']))
        logs.append(' | '.join([f'{county}-{args.day}.csv', 'UPDATE', display_names['ballot_status'], f'None => {row["situation"]}']))
        log = '\n'.join(logs)

        # if the SoS file does not specify a receive method assume 'Mail'
        if not existing_row['absentee_receive_method']:
            query = (
                'UPDATE voters '
                'SET reject_date = %s, cure_date = %s, number_of_rejections = %s, was_ever_rejected = %s, currently_rejected = %s, reject_reason = %s, ballot_status = %s, logs = %s, log = %s, absentee_receive_method = %s '
                'WHERE id = %s'
            )
            query_args = (reject_date, cure_date, 1, True, True, row['situation'], row['situation'], logs, log, 'Mail', existing_row['id'])
        else:
            query = (
                'UPDATE voters '
                'SET reject_date = %s, cure_date = %s, number_of_rejections = %s, was_ever_rejected = %s, currently_rejected = %s, reject_reason = %s, ballot_status = %s, logs = %s, log = %s '
                'WHERE id = %s'
            )
            query_args = (reject_date, cure_date, 1, True, True, row['situation'], row['situation'], logs, log, existing_row['id'])
        cursor.execute(query, query_args)

    return existing_row['registration_number']


def get_rejected_voter_ids(cursor):
    query = (
        'SELECT registration_number '
        'FROM voters '
        'WHERE ballot_status IS NOT NULL '
        'AND county = %s'
    )
    cursor.execute(query, (county,))
    return {dict(row)['registration_number'] for row in cursor.fetchall()}


def cure(cursor, cured_voter_ids):
    find_query = (
        'SELECT * '
        'FROM voters '
        'WHERE registration_number = %s'
    )
    update_query = (
        'UPDATE voters '
        'SET cure_date = %s, currently_rejected = %s, ballot_status = %s, logs = %s, log = %s '
        'WHERE registration_number = %s'
    )
    for voter_id in cured_voter_ids:
        cursor.execute(find_query, (voter_id,))
        cured_voter = dict(cursor.fetchone())

        logs = cured_voter.get('logs')
        logging.info(' | '.join(['UPDATE', pk_string(cured_voter), display_names['ballot_status'], f'{cured_voter.get("ballot_status")} => None']))
        logs.append(' | '.join([f'{county}-{args.day}.csv', 'UPDATE', display_names['ballot_status'], f'{cured_voter.get("ballot_status")} => None']))
        log = '\n'.join(logs)

        cursor.execute(update_query, (f'2020-{args.day}', False, None, logs, log, voter_id))


def check_headers_and_pks(row):
    # check the CSV headers
    if county == 'Polk':
        headers = {'Last', 'First', 'Middle', 'Address', 'Zip', 'State', 'CITY', 'Date', 'situation'}
    elif county == 'Cerro Gordo':
        headers = {'Last', 'First', 'Middle', 'request #', 'fax/email', 'original rec\'d', 'situation', 'Address', 'City State Zip'}

    for key in row.keys():
        if key not in headers:
            sys.exit('Unexpected key: ' + key)

    if not all([row[pk].strip() for pk in pks]):
        sys.exit('Missing required key: ' + str(row))


def clean_row(row):
    clean = {}
    for key, value in row.items():
        clean[key.strip()] = " ".join(value.strip().split()) if type(value) is str else value

    if county == 'Polk':
        clean['situation'] = 'Deficient Affidavit/ Incomplete'
        clean['Middle'] = None
    elif county == 'Cerro Gordo':
        names = row['First'].split(' ')
        clean['First'] = names[0].upper()
        clean['Middle'] = names[1].upper() if len(names) >= 2 else None
        clean['Last'] = clean['Last'].upper()

        if 'defective' in row['situation'].lower() or 'envelope 'in row['situation'].lower():
            clean['situation'] = 'Defective Affidavit/Envelope'
        else:
            clean['situation'] = 'Deficient Affidavit/ Incomplete'

    return clean


def handle_des_moines(path):
    df = pd.read_csv(path)
    registration_numbers = df.iloc[:, 0]
    if registration_numbers.dtypes != int:
        sys.exit('First column was not registration numbers')

    rows = []
    for registration_number in registration_numbers.tolist():
        row = dict()
        row['registration_number'] = registration_number
        row['situation'] = 'Defective Affidavit/Envelope'
        rows.append(row)

    return rows


def main():
    # remove the leading BOM present in many Excel documents and CSVs exported from Excel
    replace_bom(path)

    if county == 'Des Moines':
        rows = handle_des_moines(path)
    else:
        rows = []
        with codecs.open(path, encoding='utf-8', errors='ignore') as f:
            for row in csv.DictReader(f):
                clean = clean_row(row)
                check_headers_and_pks(clean)
                rows.append(clean)

    with Postgres(**postgres_args) as cursor:
        rejected_voter_ids = get_rejected_voter_ids(cursor)

        # update logs and DB with rejected voters
        for row in rows:
            voter_id = set_rejected(cursor, row)
            # -1 => problem trying to match this record
            if voter_id < 0:
                continue
            if voter_id in rejected_voter_ids:
                # remove any voter that is still rejected from this set
                # what remains are the voters that should be marked as cured
                rejected_voter_ids.remove(voter_id)
        cure(cursor, rejected_voter_ids)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='day', required=True)
    parser.add_argument('-c', dest='county', choices=['Cerro Gordo', 'Des Moines', 'Polk'], required=True)
    args = parser.parse_args()

    # ensure log dirs
    pathlib.Path('logs/').mkdir(exist_ok=True)
    pathlib.Path('dev_logs/').mkdir(exist_ok=True)

    county = args.county
    path = pathlib.Path(f'csvs/{county.lower().replace(" ", "_")}/{args.day}.csv')

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

    log_dir = 'logs' if is_prod else 'dev_logs'
    logging.basicConfig(filename=f'{log_dir}/{county}-{args.day}.log', format='%(asctime)s | %(message)s', level=logging.INFO)

    pks = ['Last', 'First']

    main()
