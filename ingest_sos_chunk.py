import csv
import argparse
import traceback
import logging
import os
import codecs
import pathlib
import datetime
import redis
from dotenv import load_dotenv
from services import Postgres
from psycopg2.extensions import AsIs
from constants import sos_csv_headers, code_county_map, date_keys, counties_not_reporting, display_names
from common import pk_string, get_voter

"""
For the same reasons described in ingest_county.py
the SoS files should be ingested in chronological order.
"""


def preprend_logs(voter, active_row, number_of_ballots, void_count, additional_rows, removed_rows):
    logs = []

    if additional_rows:
        # less one if previous had no active row
        previous_count = voter['number_of_rows'] - (1 if not voter['is_void'] else 0)
        # less one if UPDATE VOID line will cover one of the inserts
        add_insert_void_count = void_count - previous_count - (1 if not voter['is_void'] else 0)

        # handle additional void rows to log
        for i in range(add_insert_void_count):
            logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', 'INSERT VOID', pk_string(active_row)]))
            logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', 'INSERT VOID']))

        # if voter was and still is active add an UPDATE VOID log entry to explain that one or more new ballots were added
        # cannot rely on the is_void false => is_void true, since both the previous and current ballot are false
        if not voter['is_void'] and not active_row['is_void']:
            logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', 'UPDATE VOID', pk_string(active_row), 'new ballot(s)']))
            logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', 'UPDATE VOID', 'new ballot(s) added']))

    elif removed_rows:
        # add remove row log entries
        for i in range(removed_rows):
            logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', 'REMOVE ROW', pk_string(active_row)]))
            logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', 'REMOVE ROW']))

    return logs


def compare_and_log(number_of_ballots, existing_row, new_row, additional_rows, additional_logs):
    log_type = 'UPDATE NEW' if additional_rows else 'UPDATE'

    # Case 3E
    if new_row['is_void'] and log_type == 'UPDATE NEW':
        log_type += ' VOID'

    logs = existing_row['logs'] + additional_logs

    # remove our columns prior to comparison
    del existing_row['created_at']
    del existing_row['updated_at']
    del existing_row['logs']
    del existing_row['log']
    del existing_row['reject_date']
    del existing_row['cure_date']
    del existing_row['county_data']
    del existing_row['number_of_rows']
    del existing_row['has_voided_ballot']
    del existing_row['was_removed']
    del existing_row['number_of_rejections']
    del existing_row['was_ever_rejected']
    del existing_row['currently_rejected']
    del existing_row['reject_reason']

    # convert dates back to strings for comparison with CSV data
    for key in date_keys:
        if existing_row.get(key):
            existing_row[key] = existing_row[key].strftime('%-m/%-d/%Y')
        if new_row.get(key):
            new_row[key] = datetime.datetime.strptime(new_row[key], '%m/%d/%Y').strftime('%-m/%-d/%Y')

    has_changed = False

    # iterate over key-values and log any differences
    for k, v in new_row.items():
        if existing_row[k] != v:
            has_changed = True

            if k == 'ballot_status' and new_row['county'] not in counties_not_reporting and not new_row['is_void']:
                if existing_row[k] is None:
                    logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', f'{log_type} REJECT', pk_string(new_row), display_names[k], f'{existing_row[k]} => {new_row[k]}']))
                    logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', f'{log_type} REJECT', display_names[k], f'{existing_row[k]} => {new_row[k]}']))
                else:
                    logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', f'{log_type}', pk_string(new_row), display_names[k], f'{existing_row[k]} => {new_row[k]}']))
                    logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', f'{log_type}', display_names[k], f'{existing_row[k]} => {new_row[k]}']))

            if k == 'is_void' and new_row[k]:
                logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', f'{log_type} VOID', pk_string(new_row), display_names[k], f'{existing_row[k]} => {new_row[k]}']))
                logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', f'{log_type} VOID', display_names[k], f'{existing_row[k]} => {new_row[k]}']))
            elif k != 'ballot_status':
                logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', f'{log_type}', pk_string(new_row), display_names[k], f'{existing_row[k]} => {new_row[k]}']))
                logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', f'{log_type}', display_names[k], f'{existing_row[k]} => {new_row[k]}']))

    return has_changed, logs


def construct_mailing_address(row):
    mailing_address = ' '.join([
        row['MAIL_ADDRESS'].strip(),
        row['MAIL_CITY'].strip(),
        row['MAIL_STATE'].strip(),
        row['MAIL_ZIP'].strip(),
        row['MAIL_ZIP_PLUS'].strip()
    ]).strip()
    return mailing_address if mailing_address else None


def convert_party(party):
    if party == 'Democrat':
        return 'DEM'
    elif party == 'Republican':
        return 'REP'
    else:
        return 'OTH'


def construct_psql_rows(rows):
    psql_rows = []

    for row in rows:
        psql_row = {
            'mailing_address': construct_mailing_address(row)
        }

        for csv_key, sql_key in sos_csv_headers.items():
            # TODO: add more rigorous vetting for CSV data once enums are properly defined
            if csv_key == 'COUNTY_CODE':
                # county code => county name
                county_code = row.get(csv_key)
                if len(county_code) == 1:
                    county_code = '0' + county_code
                psql_row['county'] = code_county_map[county_code]['name']
            elif csv_key == 'POLITICAL_PARTY':
                # Democrat => DEM, etc.
                psql_row[sql_key] = convert_party(row.get(csv_key))
            elif csv_key == 'BALLOT_STATUS':
                # ensure that ballot status contains something meaningful else set to None
                if row.get(csv_key) and 'Affidavit' in row.get(csv_key):
                    psql_row[sql_key] = row.get(csv_key)
                else:
                    psql_row[sql_key] = None
            elif csv_key == 'IS_VOID':
                # int => bool
                psql_row[sql_key] = row.get(csv_key) == '1'
            elif csv_key == 'VOTER_ID':
                # type checking already occurs in process_sos_csv.py
                psql_row[sql_key] = int(row.get(csv_key))
            elif sql_key is None:
                continue
            elif row.get(csv_key) is None or not row.get(csv_key).strip():
                # null or whitespace only => None
                psql_row[sql_key] = None
            else:
                psql_row[sql_key] = row.get(csv_key)

        psql_rows.append(psql_row)

    return psql_rows


def convert_date(row):
    if row.get('receive_date'):
        return datetime.datetime.strptime(row['receive_date'], '%m/%d/%Y')
    return None


def handle_multiple_active_rows(psql_rows):
    # get the row with the most recent receive date and ignore all other active rows
    index_date_tuples = [(i, convert_date(row)) for i, row in enumerate(psql_rows) if convert_date(row)]
    if len(index_date_tuples) == 0:
        return [psql_rows[-1]]
    sorted_tuples = sorted(index_date_tuples, key=lambda x: x[1], reverse=True)
    return [psql_rows[sorted_tuples[0][0]]]


def active_void(psql_rows):
    """
    Case A => active has length 1, void has length N
        return the active row and void rows
    Case B => active has length 0, void has length N
        a single void row will be removed from the end of the void list and added to the active list
        as the representative row in the DB
    Case C => active has length N > 1, void has length N
        a row will be selected as the active row in handle_multiple_active_rows, all other active rows are ignored
    """
    active_rows = []
    void_rows = []
    for psql_row in psql_rows:
        if psql_row['is_void']:
            void_rows.append(psql_row)
        else:
            active_rows.append(psql_row)

    active_count = len(active_rows)
    void_count = len(void_rows)

    if len(active_rows) < 1:
        # if no rows are active choose the last one for the DB
        active_rows = [void_rows[len(void_rows) - 1]]
        void_rows = void_rows[:-1]
    elif len(active_rows) > 1:
        active_rows = handle_multiple_active_rows(active_rows)

    return active_rows[0], void_rows, active_count, void_count


def insert_voter(cursor, psql_rows):
    active_row, void_rows, _, _ = active_void(psql_rows)

    # even if there are 3 active ballots this number will be one
    # number of ballots should be number of recorded voids + a single active ballot if one exists
    # multiple active ballots is an anomaly
    number_of_ballots = 1 + len(void_rows)

    qualifiers = ''

    # a ballot can appear for the first time and be marked as rejected on the same CSV
    if active_row['ballot_status']:
        qualifiers += ' REJECT'

    if active_row['is_void']:
        qualifiers += ' VOID'

    # initial log entry
    logs = []
    for void_row in void_rows:
        logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', 'INSERT VOID', pk_string(void_row)]))
        logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', 'INSERT VOID']))

    logging.info(' | '.join([f'Ballot Count: {number_of_ballots}', 'INSERT' + qualifiers, pk_string(active_row)]))
    logs.append(' | '.join([f'SoS-{args.day}.csv', f'Ballot Count: {number_of_ballots}', 'INSERT' + qualifiers]))
    active_row['logs'] = logs
    active_row['log'] = '\n'.join(logs)
    active_row['number_of_rows'] = number_of_ballots

    if len(void_rows) > 0 or active_row['is_void']:
        active_row['has_voided_ballot'] = True

    # TODO: remove this if we decide we no longer want to use county data for anything more than ballot status
    active_row['county_data'] = False

    columns = active_row.keys()
    values = tuple(active_row.values())

    query = (
        'INSERT INTO voters (%s) '
        'VALUES %s'
    )
    cursor.execute(query, (AsIs(','.join(columns)), values))


def update_voter(cursor, psql_rows, voter, additional_rows=False, removed_rows=0):
    active_row, void_rows, active_count, void_count = active_void(psql_rows)

    # even if there are 3 active ballots this number will be 1
    # number of ballots should be number of recorded voids + a single active ballot if one exists
    # multiple active ballots is an anomaly that we hide from the data
    # if all rows are void, one is still plucked from the void list and set as active in active_void()
    number_of_ballots = 1 + len(void_rows)

    additional_logs = preprend_logs(voter, active_row, number_of_ballots, void_count, additional_rows, removed_rows)

    # handle updates that are independent of has_changed
    if (additional_rows or removed_rows) and voter['number_of_rows'] != number_of_ballots:
        query = (
            'UPDATE voters '
            'SET number_of_rows = %s '
            'WHERE id = %s'
        )
        cursor.execute(query, (number_of_ballots, voter['id']))

    if (len(void_rows) > 0 or active_row['is_void']) and not voter['has_voided_ballot']:
        query = (
            'UPDATE voters '
            'SET has_voided_ballot = %s '
            'WHERE id = %s'
        )
        cursor.execute(query, (True, voter['id']))

    if voter['was_removed']:
        query = (
            'UPDATE voters '
            'SET was_removed = %s '
            'WHERE id = %s'
        )
        cursor.execute(query, (False, voter['id']))

    if voter['is_void'] and active_count > 0 and not additional_rows:
        raise Exception(f'Voter lost void status without adding a row: {voter["registration_number"]}')

    if not voter['is_void'] or additional_rows:
        has_changed, updated_logs = compare_and_log(number_of_ballots, voter, active_row, additional_rows, additional_logs)

        if has_changed:
            active_row['logs'] = updated_logs
            active_row['log'] = '\n'.join(updated_logs)

            # TODO: remove this if we decide we no longer want to use county data for anything more than ballot status
            active_row['county_data'] = False

            if active_row['county'] in counties_not_reporting:
                columns = [key for key in active_row.keys() if key != 'ballot_status']
                values = tuple([active_row[column] for column in columns])
            else:
                columns = active_row.keys()
                values = tuple(active_row.values())

            # we're only ok with a full overwrite of CSV fields because we're logging all changes
            # we can accept that any recent data is more reliable and should replace existing values
            # while at the same time tracking all changes in the log in case we need data provenance later
            query = (
                'UPDATE voters '
                'SET (%s) = %s '
                'WHERE id = %s'
            )
            cursor.execute(query, (AsIs(','.join(columns)), values, voter['id']))


def upsert_voter(cursor, voter_id, rows):
    voter = get_voter(cursor, voter_id)

    # TODO: remove this once if we decide we no longer need a reference to county data
    if voter and voter['county_data']:
        return

    psql_rows = construct_psql_rows(rows)

    # Case 1: new voter
    if not voter:
        insert_voter(cursor, psql_rows)

    # Case 2: voter has the same number of rows as before
    elif voter['number_of_rows'] == len(rows):
        update_voter(cursor, psql_rows, voter)

    # Case 3: voter has more rows than before
    elif voter['number_of_rows'] < len(rows):
        update_voter(cursor, psql_rows, voter, additional_rows=True)

    # Case 4: voter has fewer rows than before (rare)
    elif voter['number_of_rows'] > len(rows):
        update_voter(cursor, psql_rows, voter, removed_rows=voter['number_of_rows'] - len(rows))

    return psql_rows[-1]['county']


def update_rejection_data(cursor, voter_id, rejection_data):
    query = (
        'UPDATE voters '
        'SET reject_date = %s, number_of_rejections = %s, was_ever_rejected = %s, currently_rejected = %s, reject_reason = %s, cure_date = NULL '
        'WHERE registration_number = %s'
    )
    cursor.execute(query, (
        rejection_data['reject_date'],
        rejection_data['number_of_rejections'],
        rejection_data['was_ever_rejected'],
        rejection_data['currently_rejected'],
        rejection_data['reject_reason'],
        voter_id,
    ))
    logging.info(' | '.join([f'UPDATE REJECTION DATA', str(voter_id), str(rejection_data)]))


def update_rejection_reason(cursor, voter_id, reason):
    query = (
        'UPDATE voters '
        'SET reject_reason = %s '
        'WHERE registration_number = %s'
    )
    cursor.execute(query, (reason, voter_id))
    logging.info(' | '.join([f'UPDATE REJECTION REASON', str(voter_id), reason]))


def cure_voter(cursor, voter_id):
    query = (
        'UPDATE voters '
        'SET cure_date = %s, currently_rejected = %s '
        'WHERE registration_number = %s'
    )
    cursor.execute(query, (f'2020-{args.day}', False, voter_id))
    logging.info(' | '.join([f'UPDATE CURE DATE', str(voter_id), f'2020-{args.day}']))


def set_currently_rejected(cursor, voter_id, value):
    query = (
        'UPDATE voters '
        'SET currently_rejected = %s '
        'WHERE registration_number = %s'
    )
    cursor.execute(query, (value, voter_id))
    logging.info(' | '.join([f'UPDATE CURRENTLY REJECTED', str(voter_id), str(value)]))


def reject_and_cure(cursor, voter_id, rows):
    voter = get_voter(cursor, voter_id)

    if not voter:
        return

    number_of_rejected_rows = 0
    rejection_data = {
        'reject_date': f'2020-{args.day}',
        'was_ever_rejected': True,
        'currently_rejected': False,
        'reject_reason': None
    }
    active_ballot = None

    for row in rows:
        if row.get('IS_VOID') == '0':
            active_ballot = row

        if row.get('BALLOT_STATUS') and 'Affidavit' in row.get('BALLOT_STATUS'):
            number_of_rejected_rows += 1

            # if this is an active ballot set currently rejected
            if row.get('IS_VOID') == '0':
                rejection_data['currently_rejected'] = True

            # if either voter reject reason or rejection data reject reason is already Both set Both and move on
            if voter.get('reject_reason') and (voter['reject_reason'] == 'Both' or rejection_data['reject_reason'] == 'Both'):
                rejection_data['reject_reason'] = 'Both'

            # if voter reject reason is different than this row set Both
            elif voter.get('reject_reason') and voter['reject_reason'] != row.get('BALLOT_STATUS'):
                rejection_data['reject_reason'] = 'Both'

            # ensure that another row in this list hasn't already set a reason other than this row's reason
            elif rejection_data['reject_reason'] and rejection_data['reject_reason'] != row.get('BALLOT_STATUS'):
                rejection_data['reject_reason'] = 'Both'

            # no voter reject reason exists or it already matches this row set this reason
            else:
                rejection_data['reject_reason'] = row.get('BALLOT_STATUS')

    # TODO: we know of a few cases where rejected rows in 10-{N} are removed in 10-{N+1}
    #       in those cases we will NOT be updating the DB with the latest rejection data
    #       but that voter will have previously been marked as rejected and can still be cured

    # if there is NEW rejection data in the SoS perform the DB update
    # new rows in the CSV with a rejected ballot status
    if number_of_rejected_rows > voter['number_of_rejections']:
        rejection_data['number_of_rejections'] = number_of_rejected_rows
        update_rejection_data(cursor, voter_id, rejection_data)
    else:
        # if not updating rejection data perform any other updates as necessary
        if number_of_rejected_rows > 0 and rejection_data['reject_reason'] and voter['reject_reason'] != rejection_data['reject_reason']:
            update_rejection_reason(cursor, voter_id, rejection_data['reject_reason'])

        # if the number of rejected rows gets out of sync with the CSV a voter can end up with null ballot status
        # and currently_rejected = true, which is not correct
        if voter['ballot_status'] is None and voter['currently_rejected']:
            set_currently_rejected(cursor, voter_id, False)

    # no new rejection data and has an active ballot and is not already marked as cured
    if number_of_rejected_rows <= voter['number_of_rejections'] and active_ballot and voter['cure_date'] is None:
        # this is our definition of cured: has been rejected at least once, has an active ballot that is received and not rejected
        active_ballot_not_rejected = active_ballot['BALLOT_STATUS'] is None or 'Affidavit' not in active_ballot['BALLOT_STATUS']
        active_ballot_received = active_ballot['RECEIVED_DATE'] is not None and active_ballot['RECEIVED_DATE'].strip() != ''
        if voter['was_ever_rejected'] and active_ballot_received and active_ballot_not_rejected:
            cure_voter(cursor, voter_id)


def main():
    voters = {}

    with codecs.open(f'csvs/sos/{args.day}_{args.chunk}.csv', encoding='utf-8', errors='ignore') as f:
        for row in csv.DictReader(f):
            if row['VOTER_ID'] in voters:
                voters[row['VOTER_ID']] += [row]
            else:
                voters[row['VOTER_ID']] = [row]

    with Postgres(**postgres_args) as cursor:
        i = 1
        total = len(voters)
        for voter_id, rows in voters.items():
            try:
                print(f'Processing voter {i} of {total}...', end='\r')
                i += 1
                county = upsert_voter(cursor, voter_id, rows)
                if county and county not in counties_not_reporting:
                    reject_and_cure(cursor, voter_id, rows)
            except Exception as e:
                tb = traceback.TracebackException.from_exception(e)
                print(''.join(tb.format()))
                logging.error(f'ERROR | {"".join(tb.format())} | {str(row)}')

            if redis_client.get('kill_ingest'):
                print('Kill switch detected...')
                break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='day', required=True)
    parser.add_argument('-c', dest='chunk', type=int, required=True)
    parser.add_argument('-p', dest='is_prod', action='store_true', default=False)
    args = parser.parse_args()

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

    redis_client = redis.StrictRedis(host='localhost', decode_responses=True)

    main()
