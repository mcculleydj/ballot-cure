import os
import civis
import csv
from psycopg2.extensions import AsIs
from common import yes_no
from services import Postgres
from dotenv import load_dotenv
from constants import voter_demographics_keys, consolidated_demographics_keys, survey_responses_keys, qid_question_map, rid_response_map


def transform_survey_data(path):
    voters = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            if voters.get(row['registration_number']):
                question = qid_question_map[row['survey_question_id']]
                response = rid_response_map[row['survey_response_id']]
                voters[row['registration_number']][question] = response
                voters[row['registration_number']][f'{question}_date'] = row['date_canvassed']
            else:
                question = qid_question_map[row['survey_question_id']]
                response = rid_response_map[row['survey_response_id']]
                voters[row['registration_number']] = {
                    'registration_number': row['registration_number'],
                    'myv_van_id': row['myv_van_id'],
                    question: response,
                    f'{question}_date': row['date_canvassed'],
                    'number_attempts': row['number_attempts'],
                    'phone_attempts': row['phone_attempts'],
                    'text_attempts': row['text_attempts'],
                    'number_canvasses': row['number_canvasses'],
                    'phone_canvasses': row['phone_canvasses'],
                    'text_canvasses': row['text_canvasses'],
                    'most_recent_contact_attempt': None if row['most_recent_contact_attempt'] == '' else row['most_recent_contact_attempt']
                }
                for _, v in qid_question_map.items():
                    if v == question:
                        continue
                    voters[row['registration_number']][v] = None
                    voters[row['registration_number']][f'{v}_date'] = None
    return voters


def check_for_van_numbers():
    numbers = {}
    with open('phones/for_dashboard_van_phones.csv') as f:
        for row in csv.DictReader(f):
            if row.get('Pref Phone '):
                numbers[int(row['Voter File VANID'])] = row.get('Pref Phone ')

    with Postgres(**postgres_args) as cursor:
        for van_id, number in numbers.items():
            query = (
                'UPDATE voter_demographics '
                'SET van_phone = %s '
                'WHERE van_id = %s'
            )
            cursor.execute(query, (number, van_id))


def main():
    voter_data = ('ia_sos_county_all_rejected', 'voter_demographics', voter_demographics_keys)
    demographics_data = ('ia_sos_all_demographic_data', 'consolidated_demographics', consolidated_demographics_keys)
    survey_data = ('ia_sos_rejected_van_data', 'survey_responses', survey_responses_keys)

    for civis_table, sql_table, keys in [voter_data, demographics_data, survey_data]:
        path = f'from_civis_{civis_table}.csv'

        if os.path.exists(path):
            os.remove(path)

        query = (
            'SELECT * '
            f'FROM states_ia_projects.{civis_table}'
        )

        fut = civis.io.civis_to_csv(
            filename=path,
            sql=query,
            database='Dover',
        )
        fut.result()

        with Postgres(**postgres_args) as cursor:
            # delete existing rows
            cursor.execute(f'DELETE FROM {sql_table}')

            if sql_table == 'survey_responses':
                for voter_id, row in transform_survey_data(path).items():
                    query = (
                        f'INSERT INTO {sql_table} (%s) '
                        'VALUES %s'
                    )
                    columns = tuple(survey_responses_keys)
                    values = tuple([row[k] for k in survey_responses_keys])
                    cursor.execute(query, (AsIs(','.join(columns)), values))
            else:
                with open(path) as f:
                    for row in csv.DictReader(f):
                        query = (
                            f'INSERT INTO {sql_table} (%s) '
                            'VALUES %s'
                        )
                        columns = tuple(row.keys())
                        values = tuple([row[k] if row[k] != '' else None for k in keys])
                        cursor.execute(query, (AsIs(','.join(columns)), values))

        os.remove(path)

    check_for_van_numbers()


if __name__ == '__main__':
    load_dotenv()

    prod = yes_no('Target production?')
    if prod:
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
