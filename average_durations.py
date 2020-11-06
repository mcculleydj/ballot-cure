import os
from dotenv import load_dotenv
from services import Postgres
from common import yes_no
from constants import county_names

"""
Calculates average durations between key events and aggregates them at the county level.
"""


def build_county_dict(results=False):
    counties = {}
    for county_name in county_names:
        if results:
            counties[county_name] = {
                'time_to_return': None,
                'time_to_reject': None,
                'time_to_cure': None
            }
        else:
            counties[county_name] = {
                'time_to_return': {
                    'sum': 0,
                    'count': 0
                },
                'time_to_reject': {
                    'sum': 0,
                    'count': 0
                },
                'time_to_cure': {
                    'sum': 0,
                    'count': 0
                },
            }
    return counties


def average_time_to_return(cursor):
    query = (
        'SELECT * '
        'FROM voters '
        'WHERE sent_date IS NOT NULL '
        'AND receive_date IS NOT NULL '
        'AND absentee_issue_method = \'Mailing\''
    )
    cursor.execute(query)
    i = 1
    for row in cursor.fetchall():
        print(f'Processing average time to return {i}...', end='\r')
        i += 1
        d = dict(row)
        d['sent_date'] = d['sent_date'].replace(year=2020)
        d['receive_date'] = d['receive_date'].replace(year=2020)
        counties_dict[row['county']]['time_to_return']['count'] += 1
        counties_dict[row['county']]['time_to_return']['sum'] += (d['receive_date'] - d['sent_date']).days
    for county_name in county_names:
        if counties_dict[county_name]['time_to_return']['count'] > 0:
            results_dict[county_name]['time_to_return'] = counties_dict[county_name]['time_to_return']['sum'] / counties_dict[county_name]['time_to_return']['count']
        else:
            results_dict[county_name]['time_to_return'] = None
    print()


def average_time_to_reject(cursor):
    query = (
        'SELECT * '
        'FROM voters '
        'WHERE reject_date IS NOT NULL '
        'AND receive_date IS NOT NULL '
        'AND absentee_issue_method = \'Mailing\''
    )
    cursor.execute(query)
    i = 1
    for row in cursor.fetchall():
        print(f'Processing average time to reject {i}...', end='\r')
        i += 1
        d = dict(row)
        d['reject_date'] = d['reject_date'].replace(year=2020)
        d['receive_date'] = d['receive_date'].replace(year=2020)
        # only consider non-negative durations (negative results are due to new ballots)
        if (d['reject_date'] - d['receive_date']).days >= 0:
            counties_dict[row['county']]['time_to_reject']['count'] += 1
            counties_dict[row['county']]['time_to_reject']['sum'] += (d['reject_date'] - d['receive_date']).days
    for county_name in county_names:
        if counties_dict[county_name]['time_to_reject']['count'] > 0:
            results_dict[county_name]['time_to_reject'] = counties_dict[county_name]['time_to_reject']['sum'] / counties_dict[county_name]['time_to_reject']['count']
        else:
            results_dict[county_name]['time_to_reject'] = None
    print()


def average_time_to_cure(cursor):
    query = (
        'SELECT * '
        'FROM voters '
        'WHERE reject_date IS NOT NULL '
        'AND cure_date IS NOT NULL '
        'AND absentee_issue_method = \'Mailing\''
    )
    cursor.execute(query)
    i = 1
    for row in cursor.fetchall():
        print(f'Processing average time to cure {i}...', end='\r')
        i += 1
        d = dict(row)
        d['cure_date'] = d['cure_date'].replace(year=2020)
        d['reject_date'] = d['reject_date'].replace(year=2020)
        counties_dict[row['county']]['time_to_cure']['count'] += 1
        counties_dict[row['county']]['time_to_cure']['sum'] += (d['cure_date'] - d['reject_date']).days
    for county_name in county_names:
        if counties_dict[county_name]['time_to_cure']['count'] > 0:
            results_dict[county_name]['time_to_cure'] = counties_dict[county_name]['time_to_cure']['sum'] / counties_dict[county_name]['time_to_cure']['count']
        else:
            results_dict[county_name]['time_to_cure'] = None
    print()


def insert_averages(cursor):
    query = (
        'INSERT INTO average_durations '
        '(county, time_to_return, time_to_reject, time_to_cure) '
        'VALUES (%s, %s, %s, %s)'
    )
    for county, values in results_dict.items():
        cursor.execute(query, (
            county,
            values['time_to_return'],
            values['time_to_reject'],
            values['time_to_cure']
        ))


def main():
    with Postgres(**postgres_args) as cursor:
        cursor.execute('DELETE FROM average_durations')
        average_time_to_return(cursor)
        average_time_to_reject(cursor)
        average_time_to_cure(cursor)
        insert_averages(cursor)


if __name__ == '__main__':
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

    counties_dict = build_county_dict()
    results_dict = build_county_dict(results=True)

    main()
