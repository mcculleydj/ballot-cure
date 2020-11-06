
import civis
import os
import pandas as pd
from common import yes_no
from dotenv import load_dotenv
from services import Postgres


def main():

    path = 'from_civis_ia_observed_rejection_data_to_van.csv'
    if os.path.exists(path):
        os.remove(path)

    query = (
        'SELECT * '
        'FROM states_ia_projects.ia_observed_rejection_data_to_van'
    )

    fut = civis.io.civis_to_csv(
        filename=path,
        sql=query,
        database='Dover',
    )
    fut.result()

    df = pd.read_csv(path)

    new_van_ids_to_cure = pd.read_csv(path)['van_id'].tolist()
    #
    df = pd.read_csv('appends.csv')
    to_cure = df[df['van_id'].isin(new_van_ids_to_cure)]
    # # cell and landline will display in the dashboard with a .0 after the number without the following line (eg. 2121234567.0)
    # to_cure = to_cure.astype({'cell': 'Int64', 'landline': 'Int64'})
    # to_cure.to_csv('observed_rejections_to_cure.csv', mode='a', header=False, index=False) # append new rejections to existing csv


    insert_row = (
        'INSERT INTO observed_rejections (registration_number, county, last_name, first_name, party, cell, landline, address, city) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s,  %s, %s) '
        'ON CONFLICT DO NOTHING'
    )

    with Postgres(**postgres_args) as cursor:
        for _, row in df.iterrows():
            args = (row['sos_id'], row['county_name'], row['last_name'], row['first_name'], row['party_name_dnc'],
                    str(int(row['cell'])), str(int(row['landline'])), row['voting_street_address'], row['voting_city'])
            cursor.execute(insert_row, args)




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
