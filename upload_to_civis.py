import os
import civis
from common import yes_no
from services import Postgres
from dotenv import load_dotenv


def main():
    with Postgres(**postgres_args) as cursor:
        with open('projection_schema.sql') as f:
            cursor.execute(f.read())

    if prod:
        query = (
            'SELECT * '
            'FROM civis_projection'
        )
        file_output_query = f'COPY ({query}) TO STDOUT WITH CSV HEADER'

        path = 'to_civis.csv'
        with Postgres(**postgres_args) as cursor:
            with open(path, 'w') as f:
                cursor.copy_expert(file_output_query, f)

        fut = civis.io.csv_to_civis(
            filename=path,
            database='Dover',
            table='states_ia_projects.ia_sos_county_all_voters',
            existing_table_rows='drop'
        )
        fut.result()
        os.remove(path)


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
