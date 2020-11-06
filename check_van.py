import csv
import argparse
import os
import civis
import subprocess
from dotenv import load_dotenv
from services import Postgres


def currently_rejected_case_one():
    query = (
        'SELECT * '
        'FROM voters '
        'WHERE county IN (\'Polk\', \'Cerro Gordo\', \'Des Moines\') '
        'AND ballot_status IS NOT NULL'
    )
    with Postgres(**postgres_args) as cursor:
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]


def all_rejected_van():
    if os.path.exists('all_rejected.csv'):
        os.remove('all_rejected.csv')

    query = (
        'select person.myv_van_id as van_id, sos_id '
        'from my_state.person '
        'left join my_state_van.coord20_myv_001_responses '
        'on person.myv_van_id = coord20_myv_001_responses.myv_van_id '
        'where survey_question_id = 427730 '
        'and sq_most_recent_response = 1 '
        'and survey_response_id in (1746381, 1746382, 1746383) '
        'and county_name in (\'Polk\', \'Cerro Gordo\', \'Des Moines\') '
        'and person.state_code = \'IA\''
    )

    fut = civis.io.civis_to_csv(
        filename='all_rejected.csv',
        sql=query,
        database='Dover',
    )
    fut.result()

    with open('all_rejected.csv') as f:
        return [row for row in csv.DictReader(f)]


def get_voters_set_as_rejected(date):
    if os.path.exists(f'{date}_rejected.csv'):
        os.remove(f'{date}_rejected.csv')

    query = (
        'select person.myv_van_id as van_id, sos_id '
        'from my_state.person '
        'left join my_state_van.coord20_myv_001_responses '
        'on person.myv_van_id = coord20_myv_001_responses.myv_van_id '
        'where survey_question_id = 427730 '
        f'and date_canvassed = \'2020-{date}\' '
        'and sq_most_recent_response = 1 '
        'and survey_response_id in (1746381, 1746382, 1746383)'
    )

    fut = civis.io.civis_to_csv(
        filename=f'{date}_rejected.csv',
        sql=query,
        database='Dover',
    )
    fut.result()

    with open(f'{date}_rejected.csv') as f:
        return [row for row in csv.DictReader(f)]


def get_voters_set_as_cured_new_ballot(date):
    if os.path.exsts(f'{date}_cured.csv'):
        os.remove(f'{date}_cured.csv')

    query = (
        'select person.myv_van_id as van_id, sos_id '
        'from my_state.person '
        'left join my_state_van.coord20_myv_001_responses '
        'on person.myv_van_id = coord20_myv_001_responses.myv_van_id '
        'where survey_question_id = 427730 '
        f'and date_canvassed = \'2020-{date}\' '
        'and sq_most_recent_response = 1 '
        'and survey_response_id = 1746384'
    )

    fut = civis.io.civis_to_csv(
        filename=f'{date}_cured.csv',
        sql=query,
        database='Dover',
    )
    fut.result()

    with open(f'{date}_cured.csv') as f:
        return [row for row in csv.DictReader(f)]


def compare_rejected(date):
    query = (
        'select * from voters where reject_date = %s'
    )
    with Postgres(**postgres_args) as cursor:
        cursor.execute(query, (f'2020-{date}',))
        rejected_rows = [dict(row) for row in cursor.fetchall()]

    rejected_row_dict = {}
    for row in rejected_rows:
        rejected_row_dict[row['registration_number']] = row

    psql_sos_ids = {row['registration_number'] for row in rejected_rows}

    civis_sos_ids = set()

    with open(f'{date}_rejected.csv') as f:
        for row in csv.DictReader(f):
            civis_sos_ids.add(int(row['sos_id']))

    not_in_civis = psql_sos_ids - civis_sos_ids
    not_in_psql = civis_sos_ids - psql_sos_ids

    print('in psql not in civis', not_in_civis)
    print('in civis not in psql', civis_sos_ids - psql_sos_ids)

    # for vid in not_in_civis:
    #     if rejected_row_dict[vid]['county'] != 'Polk' and rejected_row_dict[vid]['party'] == 'DEM':
    #         p = subprocess.run(['grep', f'{vid}', '/Users/dingo/dev/IACC_2020/cure/csvs/sos/10-31.csv'], capture_output=True)
    #         print('\n', p.stdout, '\n')

    for vid in not_in_psql:
        p = subprocess.run(['grep', f'{vid}', '/Users/dingo/dev/IACC_2020/cure/csvs/sos/10-31.csv'], capture_output=True)
        print('\n', p.stdout, '\n')


def main():
    # get_voters_set_as_rejected(args.date)
    # compare_rejected(args.date)

    psql_rows = currently_rejected_case_one()
    civis_rows = all_rejected_van()

    reg_nums = {str(row['registration_number']) for row in psql_rows}
    sos_ids = {row['sos_id'] for row in civis_rows}

    sql_not_civis = reg_nums - sos_ids
    civis_not_sql = sos_ids - reg_nums

    print('in psql not in civis', reg_nums - sos_ids)
    print('in civis not in psql', sos_ids - reg_nums)
    print()
    print()
    print(len(sql_not_civis))
    print(len(civis_not_sql))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='date', required=True)
    args = parser.parse_args()

    load_dotenv()

    postgres_args = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': int(os.getenv('POSTGRES_PORT')),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'dbname': os.getenv('POSTGRES_DB'),
    }

    main()
