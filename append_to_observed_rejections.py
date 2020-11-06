import csv
import os
import civis
import pandas as pd
from dotenv import load_dotenv
from fuzzywuzzy import fuzz
from services import Postgres


def get_contact_info(sos_ids, ballot_info):
    if os.path.exists('appends.csv'):
        os.remove('appends.csv')

    query = (
        'SELECT '
        '   myv_van_id as van_id, '
        '   sos_id, '
        '   last_name, '
        '   first_name, '
        '   county_name, '
        '   party_name_dnc, '
        '   best_number, '
        '   cell, '
        '   landline, '
        '   voting_street_address, '
        '   voting_street_address_2, '
        '   voting_city, '
        '   voting_zip, '
        '   mailing_street_address, '
        '   mailing_street_address_2, '
        '   mailing_city, '
        '   mailing_zip, '
        '   mailing_state '
        'FROM my_state.person as p '
        'LEFT JOIN states_shared_pipeline.myv_001_best_phones as ph '
        'ON p.national_myv_van_id = ph.national_myv_van_id '
        f'WHERE p.sos_id IN {str(tuple(sos_ids)).replace(",", "") if len(tuple(sos_ids)) == 1 else tuple(sos_ids)} '
    )

    fut = civis.io.civis_to_csv(
        filename='appends.csv',
        sql=query,
        database='Dover',
    )
    fut.result()

    df = pd.read_csv('appends.csv')[['van_id', 'sos_id']]
    rejections = []

    for sos_id in df['sos_id'].tolist():
        rejections.append(ballot_info[str(sos_id)][0])
    df['ballot_status'] = rejections
    del df['sos_id']
    df.to_csv('to_civis.csv', index=False)

    path = 'to_civis.csv'
    fut = civis.io.csv_to_civis(
        filename=path,
        database='Dover',
        table='states_ia_projects.ia_observed_rejections',
        existing_table_rows='drop'
    )
    fut.result()
    os.remove(path)

    append_voters_to_cure(pd.read_csv('appends.csv'))

    with open('appends.csv') as f:
        return [row for row in csv.DictReader(f)]


def append_voters_to_cure(df):
    drop_rows = ('DELETE FROM observed_rejections')

    insert_row = (
        'INSERT INTO observed_rejections (registration_number, county, last_name, first_name, party, cell, landline, address, city) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s,  %s, %s) '
        'ON CONFLICT DO NOTHING'
    )

    with Postgres(**postgres_args) as cursor:
        cursor.execute(drop_rows)
        for _, row in df.iterrows():
            args = (row['sos_id'], row['county_name'], row['last_name'], row['first_name'], row['party_name_dnc'],
                    str(row['cell']), str(row['landline']), row['voting_street_address'], row['voting_city'])
            cursor.execute(insert_row, args)


def get_matches(cursor, voter):
    query = (
        'SELECT * '
        'FROM voters '
        'WHERE registration_number = %s '
    )
    cursor.execute(query, (voter['sos_id'],))
    result = cursor.fetchone()
    if result:
        return [dict(result)]

    query = (
        'SELECT * '
        'FROM voters '
        'WHERE county = %s '
        'AND absentee_sequence_number = %s'
    )
    cursor.execute(query, (voter['county'].title(), voter['sequence_number']))
    result = cursor.fetchone()
    if result:
        return [dict(result)]

    query = (
        'SELECT * '
        'FROM voters '
        'WHERE first_name = %s '
        'AND last_name = %s '
    )

    args = (voter['first'].upper(), voter['last'].upper())
    if voter['county']:
        query += 'AND county = %s '
        args += (voter['county'].title(),)

    cursor.execute(query, args)
    return [dict(row) for row in cursor.fetchall()]


def search_by_address(potential_matches, address):
    if address.strip() == '':
        return []

    matches = []
    address_start = ' '.join(address.lower().split()[:2])

    for match in potential_matches:
        existing_resident_address_start = ' '.join(match['resident_address'].lower().split()[:2])
        existing_mailing_address_start = ''
        if match['mailing_address']:
            existing_mailing_address_start = ' '.join(match['mailing_address'].lower().split()[:2])
        if existing_resident_address_start == address_start or existing_mailing_address_start == address_start:
            matches.append(match)

        # use fuzzy matching to try and match on address
        if len(matches) != 1:
            resident_address =  match['resident_address'].lower() if match['resident_address'] else ''
            mailing_address = match['mailing_address'].lower() if match['mailing_address'] else ''
            rejected_address = address.lower()
            if fuzz.partial_ratio(resident_address, rejected_address) >= 90 or fuzz.partial_ratio(mailing_address, rejected_address) >= 90:
                matches.append(match)

    return matches


def get_rejection_reason(reason):
    if not reason:
        return None
    if reason == "The voter's affidavit lacks the voter's signature":
        return "Deficient"
    if reason in ["Void", "The applicant voted in person", "Other (enter information in the next question)"]:
        return "Other"
    return "Defective"


def append_unknown_voters(unknown_voters):
    drop_rows = ('DELETE FROM unknown_voters')

    insert_row = (
        f'INSERT INTO unknown_voters (county, last_name, first_name, address, city, note, registration_number)'
        'VALUES (%s, %s, %s, %s, %s, %s, %s) '
    )

    with Postgres(**postgres_args) as cursor:
        cursor.execute(drop_rows)
        for voter in unknown_voters:
            args = (voter['county'], voter['last'], voter['first'], voter['address'], voter['city'], voter['note'], voter['sos_id'])
            cursor.execute(insert_row, args)


def main():
    if os.path.exists('from_drive_append.csv'):
        os.remove('from_drive_append.csv')

    print('Processing absentee ballot intake file...')

    projections = []
    original_rows = []
    with open('from_drive.csv') as f:
        for row in csv.DictReader(f):
            headers = list(row.keys())
            if not row['If rejected, what is the basis for rejection?']:
                continue
            original_rows.append(row)

            projections.append({
                'first': row['Voter\'s First Name'].split(' ')[0],
                'last': row['Voter\'s Last Name'].split(' ')[0],
                'county': row['County (ENTER COUNTY UNLESS IT IS AN LBJ ISSUE WITHOUT COUNTY)'],
                'sos_id': row['Voter ID'] if row['Voter ID'] else -1,
                'sequence_number': str(row['Sequence Number']) if row['Sequence Number'] else '-1',
                'address': row['Voter\'s Address (1)'],
                'city': row['Voter\'s City'],
                'rejection_reason': get_rejection_reason(row['If rejected, what is the basis for rejection?']),
            })

    sos_ids = set()
    ballot_info = dict() # maps to tuple (rejection_reason)
    unknown_voters = list()

    with Postgres(**postgres_args) as cursor:
        for projection in projections:
            matches = get_matches(cursor, projection)
            if len(matches) > 1:
                matches = search_by_address(matches, projection['address'])
            if len(matches) == 1 and not matches[0]['reject_date']:
                sos_ids.add(matches[0]['registration_number'])
                projection['sos_id'] = str(matches[0]['registration_number'])
                ballot_info[projection['sos_id']] = (projection['rejection_reason'],)
            else:
                note = []
                sos_id = None
                for match in matches:
                    if match['reject_date']:
                        note.append(f'We believe this voter (ID: {match["registration_number"]}) was rejected on {match["reject_date"]}')
                        sos_id = match['registration_number']
                projection['note'] = '; '.join(note)
                projection['sos_id'] = sos_id
                unknown_voters.append(projection)

    appends = get_contact_info(sos_ids, ballot_info)
    headers += list(appends[0].keys()) + ['rejection_reason']

    appends_dict = {}
    for a in appends:
        appends_dict[a['sos_id']] = a

    with open('from_drive_append.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for i, projection in enumerate(projections):
            row = {
                **original_rows[i],
                'van_id': None,
                'sos_id': None,
                'last_name': None,
                'first_name': None,
                'county_name': None,
                'party_name_dnc': None,
                'best_number': None,
                'cell': None,
                'landline': None,
                'voting_street_address': None,
                'voting_street_address_2': None,
                'voting_city': None,
                'voting_zip': None,
                'mailing_street_address': None,
                'mailing_street_address_2': None,
                'mailing_city': None,
                'mailing_zip': None,
                'mailing_state': None,
            }
            if projection.get('sos_id') in appends_dict:
                row = {
                    **row,
                    **appends_dict[projection['sos_id']],
                    'rejection_reason': projection['rejection_reason'],
                }
            writer.writerow(row)

    append_unknown_voters(unknown_voters)


if __name__ == '__main__':
    load_dotenv()

    postgres_args = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': int(os.getenv('POSTGRES_PORT')),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'dbname': os.getenv('POSTGRES_DB'),
    }

    main()
