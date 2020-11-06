import csv
import os
import codecs
import argparse
import civis
from dotenv import load_dotenv
from services import Postgres
from common import replace_bom, yes_no
from constants import van_to_clarity


def generate_base(for_clarity=False):
    """
    List of currently rejected voters from the cure universe without a phone number in VAN.
    """
    if for_clarity:
        query = (
            'SELECT '
            '   myv_van_id as van_id, '
            '   voters.first_name, '
            '   voters.last_name, '
            '   voters.resident_address, '
            '   voters.city, '
            '   voters.state, '
            '   voters.zip, '
            '   voters.mailing_address '
        )
    else:
        query = 'SELECT voters.registration_number as sos_id, myv_van_id as van_id, voters.first_name, voters.last_name '

    query = query + (
        'FROM voters, survey_responses, voter_demographics '
        'WHERE voters.registration_number = survey_responses.registration_number '
        'AND survey_responses.registration_number = voter_demographics.registration_number '
        'AND best_number IS NULL '
        'AND best_number_type IS NULL '
        'AND cell IS NULL '
        'AND landline IS NULL '
        'AND voters.ballot_status IS NOT NULL'
    )
    with Postgres(**postgres_args) as cursor:
        cursor.execute(query)
        return {dict(row)['van_id']: dict(row) for row in cursor.fetchall()}


def process_clarity_csv():
    with Postgres(**postgres_args) as cursor:
        cursor.execute('SELECT * FROM wrong_numbers')
        wrong_numbers = {(dict(row)['van_id'], dict(row)['number']) for row in cursor.fetchall()}
        cursor.execute('SELECT * FROM right_numbers')
        right_numbers = {(dict(row)['van_id'], dict(row)['number']) for row in cursor.fetchall()}

    replace_bom('from_clarity.csv')

    clarity_dict = {}

    with codecs.open('from_clarity.csv', encoding='utf-8', errors='ignore') as f:
        for row in csv.DictReader(f):
            clarity_phone = row['ts_phone'] if (int(row['van_id']), row['ts_phone']) not in wrong_numbers and row['ts_phone'] != '\\N' else None
            clarity_cell = row['ts_wireless'] if (int(row['van_id']), row['ts_wireless']) not in wrong_numbers and row['ts_wireless'] != '\\N' else None
            clarity_dict[int(row['van_id'])] = {
                'clarity_phone': clarity_phone,
                'clarity_cell': clarity_cell,
                'clarity_phone_type': row['ts_phonetype'] if clarity_phone else None,
                'phone_verified': row['ts_phone'] in right_numbers,
                'cell_verified': row['ts_wireless'] in right_numbers,
            }
        return clarity_dict


def upload_numbers(upload_type):
    query = (
        f'INSERT INTO {upload_type}_numbers (van_id, number, source)'
        'VALUES (%s, %s, %s) '
        'ON CONFLICT DO NOTHING'
    )
    with Postgres(**postgres_args) as cursor:
        with open(f'phones/{upload_type}_numbers.csv') as f:
            for row in csv.DictReader(f):
                cursor.execute(query, (row['van_id'], row['number'], row['source']))


def generate_list():
    if os.path.exists('phone_list.csv'):
        os.remove('phone_list.csv')

    base_dict = generate_base()
    clarity_dict = process_clarity_csv()

    with open('phones/phone_list.csv', 'w') as f:
        headers = [
            'sos_id',
            'van_id',
            'first_name',
            'last_name',
            'clarity_phone',
            'clarity_cell',
            'clarity_phone_type',
            'phone_verified',
            'cell_verified'
        ]
        writer = csv.DictWriter(f, headers)
        writer.writeheader()
        for van_id, clarity_values in clarity_dict.items():
            # skip clarity numbers we no longer care about
            if van_id not in base_dict:
                continue
            else:
                base_dict[van_id] = {**base_dict[van_id], **clarity_values}
        for van_id in base_dict.keys():
            if van_id not in clarity_dict:
                base_dict[van_id] = {
                    **base_dict[van_id],
                    'clarity_phone': None,
                    'clarity_cell': None,
                    'clarity_phone_type': None,
                    'phone_verified': None,
                    'cell_verified': None
                }
        rows = []
        for key in base_dict.keys():
            rows.append({'van_id': key, **base_dict[key]})
        writer.writerows(rows)


def create_for_clarity():
    if os.path.exists('for_clarity.csv'):
        os.remove('for_clarity.csv')

    replace_bom('phones/not_yet_contacted.csv')

    with codecs.open('phones/not_yet_contacted.csv') as f:
        rows = [row for row in csv.DictReader(f) if not row.get('Pref Phone ') or row.get('Pref Phone ').strip() == '']

    for_clarity_rows = [{van_to_clarity[k]: v for k, v in row.items() if k in van_to_clarity} for row in rows]

    with open('phones/for_clarity.csv', 'w') as f:
        writer = csv.DictWriter(f, van_to_clarity.values())
        writer.writeheader()
        writer.writerows(for_clarity_rows)


def get_cure_universe(currently_rejected=False, no_contact=False, has_number=False, has_cell=False, missing_number=False):
    query = (
        'SELECT '
        '  survey_responses.myv_van_id as van_id, '
        '  voters.first_name as first, '
        '  voters.last_name as last, '
        '  voters.county as county, '
        '  voters.ballot_status as status, '
        '  voter_demographics.best_number as best, '
        '  voter_demographics.cell '
        'FROM voters, survey_responses, voter_demographics '
        'WHERE voters.registration_number = survey_responses.registration_number '
        'AND dscc_support_score >= 50'
    )

    if currently_rejected:
        query += 'AND voters.ballot_status IS NOT NULL '

    if no_contact:
        query += (
            'AND survey_responses.registration_number = voter_demographics.registration_number '
            'AND auditor_contact_date IS NULL '
            'AND has_plan_date IS NULL '
            'AND plan_date IS NULL '
        )

    if has_cell:
        query += (
            'AND ( '
            '  cell IS NOT NULL OR '
            '  (best_number IS NOT NULL AND best_number_type = \'C\') '
            ')'
        )
    elif has_number:
        query += 'AND best_number IS NOT NULL'
    elif missing_number:
        query += 'AND best_number IS NULL'

    with Postgres(**postgres_args) as cursor:
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]


def get_opt_outs():
    van_ids = set()
    with open('phones/opt_out.csv') as f:
        for row in csv.DictReader(f):
            van_ids.add(int(row['van_id']))
    return van_ids


def get_van_numbers():
    numbers = {}
    with open('phones/not_yet_contacted.csv') as f:
        for row in csv.DictReader(f):
            if row.get('Pref Phone '):
                numbers[int(row['Voter File VANID'])] = row.get('Pref Phone ')
    return numbers


def get_clarity_numbers():
    numbers = {}
    with open('phones/from_clarity.csv') as f:
        for row in csv.DictReader(f):
            clarity_phone = row['ts_phone'] if row['ts_phone'] != '\\N' and row['ts_phonetype'] == 'Wireless' else None
            clarity_cell = row['ts_wireless'] if row['ts_wireless'] != '\\N' else None
            numbers[int(row['van_id'])] = clarity_cell or clarity_phone
    return numbers


def get_voter_info(cursor, registration_number):
    query = (
        'SELECT first_name as first, last_name as last, county, ballot_status as status '
        'FROM voters '
        'WHERE registration_number = %s'
    )
    cursor.execute(query, (registration_number,))
    result = cursor.fetchone()
    if not result:
        return None
    return dict(result)


def get_best_cell_for_van_ids(van_ids):
    query = (
        'SELECT myv_van_id as van_id, sos_id, best_number, cell '
        'FROM my_state.person as p, states_shared_pipeline.myv_001_best_phones as ph '
        'WHERE p.national_myv_van_id = ph.national_myv_van_id '
        'AND (best_number_type = \'C\' OR cell IS NOT NULL) '
        f'AND p.myv_van_id IN {tuple(van_ids)}'
    )

    if os.path.exists('phones/cell_phones.csv'):
        os.remove('phones/cell_phones.csv')

    fut = civis.io.civis_to_csv(
        filename='phones/cell_phones.csv',
        sql=query,
        database='Dover',
    )
    fut.result()


def get_sos_ids_for_van_ids(van_ids):
    query = (
        'SELECT myv_van_id as van_id, sos_id '
        'FROM my_state.person as p '
        f'WHERE p.myv_van_id IN {tuple(van_ids)}'
    )

    if os.path.exists('phones/sos_ids.csv'):
        os.remove('phones/sos_ids.csv')

    fut = civis.io.civis_to_csv(
        filename='phones/sos_ids.csv',
        sql=query,
        database='Dover',
    )
    fut.result()


def follow_up_texts():
    if os.path.exists(f'phones/deficient_follow_up_{args.day}.csv'):
        os.remove(f'phones/deficient_follow_up_{args.day}.csv')

    if os.path.exists(f'phones/defective_follow_up_{args.day}.csv'):
        os.remove(f'phones/defective_follow_up_{args.day}.csv')

    van_ids = set()
    van_phones = {}
    with open('phones/contacted_currently_rejected.csv') as f:
        for row in csv.DictReader(f):
            van_ids.add(row['Voter File VANID'])
            van_phones[row['Voter File VANID']] = row['Pref Phone ']
    get_best_cell_for_van_ids(van_ids)
    get_sos_ids_for_van_ids(van_ids)

    sos_ids = {}
    with open('phones/sos_ids.csv') as f:
        for row in csv.DictReader(f):
            sos_ids[row['van_id']] = int(row['sos_id'])

    cells = {}
    with open('phones/cell_phones.csv') as f:
        for row in csv.DictReader(f):
            cells[row['van_id']] = row['cell'] or row['best_number']

    rows_to_write = []
    with Postgres(**postgres_args) as cursor:
        for van_id in van_ids:

            if van_id not in cells:
                cell = van_phones[van_id]
                if not cell:
                    continue
            else:
                cell = cells[van_id]

            voter_info = get_voter_info(cursor, sos_ids[van_id])
            if not voter_info:
                continue

            rows_to_write.append({
                'van_id': van_id,
                'cell': cell,
                **voter_info
            })

    deficient_targets = [row for row in rows_to_write if row['status'] and 'Deficient' in row['status']]
    defective_targets = [row for row in rows_to_write if row['status'] and 'Defective' in row['status']]

    for target in deficient_targets:
        del target['status']

    for target in defective_targets:
        del target['status']

    with open(f'phones/deficient_follow_up_{args.day}.csv', 'w') as f:
        headers = [
            'van_id',
            'first',
            'last',
            'county',
            'cell'
        ]
        writer = csv.DictWriter(f, headers)
        writer.writeheader()
        writer.writerows(deficient_targets)

    with open(f'phones/defective_follow_up_{args.day}.csv', 'w') as f:
        headers = [
            'van_id',
            'first',
            'last',
            'county',
            'cell'
        ]
        writer = csv.DictWriter(f, headers)
        writer.writeheader()
        writer.writerows(defective_targets)


def generate_text_universe():
    if os.path.exists(f'defective_{args.day}.csv'):
        os.remove(f'defective_{args.day}.csv')

    if os.path.exists(f'deficient_{args.day}.csv'):
        os.remove(f'deficient_{args.day}.csv')

    targets_w_cells = get_cure_universe(currently_rejected=True, no_contact=True, has_cell=True)
    targets_wo_numbers = get_cure_universe(currently_rejected=True, no_contact=True, missing_number=True)

    for target in targets_w_cells:
        if target.get('cell'):
            del target['best']
        else:
            target['cell'] = target['best']
            del target['best']

    for target in targets_wo_numbers:
        del target['best']

    # try the latest rejected VAN file
    van_numbers = get_van_numbers()
    for target in targets_wo_numbers:
        if target['van_id'] in van_numbers:
            # assume cell, there is no way to know so send the text
            target['cell'] = van_numbers[target['van_id']]

    # try the latest Clarity file
    clarity_numbers = get_clarity_numbers()
    for target in targets_wo_numbers:
        if 'cell' in target:
            continue
        if target['van_id'] in clarity_numbers:
            target['cell'] = clarity_numbers[target['van_id']]

    # screen out opt outs or previously contacted voters (via text -- not yet uploaded to VAN)
    targets_w_cells += [target for target in targets_wo_numbers if target['cell']]
    opt_outs = get_opt_outs()
    targets_w_cells = [target for target in targets_w_cells if target['van_id'] not in opt_outs]

    # remove anyone not in the rejected non-contacted cure universe
    van_ids = set()
    with open('phones/not_yet_contacted.csv') as f:
        for row in csv.DictReader(f):
            van_ids.add(int(row['Voter File VANID']))
    targets_w_cells = [target for target in targets_w_cells if target['van_id'] in van_ids]

    deficient_targets = [target for target in targets_w_cells if target['status'] == 'Deficient Affidavit/ Incomplete']
    defective_targets = [target for target in targets_w_cells if target['status'] == 'Defective Affidavit/Envelope']

    for target in deficient_targets:
        del target['status']

    for target in defective_targets:
        del target['status']

    with open(f'phones/deficient_{args.day}.csv', 'w') as f:
        headers = [
            'van_id',
            'first',
            'last',
            'county',
            'cell'
        ]
        writer = csv.DictWriter(f, headers)
        writer.writeheader()
        writer.writerows(deficient_targets)

    with open(f'phones/defective_{args.day}.csv', 'w') as f:
        headers = [
            'van_id',
            'first',
            'last',
            'county',
            'cell'
        ]
        writer = csv.DictWriter(f, headers)
        writer.writeheader()
        writer.writerows(defective_targets)


def main():
    if args.upload_wrong_numbers:
        upload_numbers('wrong')

    if args.upload_right_numbers:
        upload_numbers('right')

    if args.generate_list:
        generate_list()

    if args.for_clarity:
        create_for_clarity()

    if args.generate_text_universe:
        generate_text_universe()

    if args.follow_up_texts:
        follow_up_texts()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-w', dest='upload_wrong_numbers', action='store_true', default=False)
    parser.add_argument('-r', dest='upload_right_numbers', action='store_true', default=False)
    parser.add_argument('-o', dest='generate_list', action='store_true', default=False)
    parser.add_argument('-c', dest='for_clarity', action='store_true', default=False)
    parser.add_argument('-t', dest='generate_text_universe', action='store_true', default=False)
    parser.add_argument('-f', dest='follow_up_texts', action='store_true', default=False)
    parser.add_argument('-d', dest='day')
    args = parser.parse_args()

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

    main()
