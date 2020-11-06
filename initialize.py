import os
from dotenv import load_dotenv
from pathlib import Path
from services import Postgres
from common import yes_no


def main():
    log_dir = 'logs' if is_prod else 'dev_logs'
    for path in Path(log_dir).glob('*.log'):
        path.unlink()

    with Postgres(**postgres_args) as cursor:
        with open('schema.sql') as f:
            cursor.execute(f.read())


if __name__ == '__main__':
    load_dotenv()

    is_prod = yes_no('Target production?')
    if is_prod:
        # postgres_args = {
        #     'host': os.getenv('POSTGRES_HOST'),
        #     'port': int(os.getenv('POSTGRES_PORT')),
        #     'user': os.getenv('POSTGRES_USER'),
        #     'password': os.getenv('POSTGRES_PASSWORD'),
        #     'dbname': os.getenv('POSTGRES_DB'),
        # }

        # if yes_no('Initialize the production database and logs?'):
        #     main()
        pass
    else:
        postgres_args = {
            'host': os.getenv('DEV_POSTGRES_HOST'),
            'port': int(os.getenv('DEV_POSTGRES_PORT')),
            'user': os.getenv('DEV_POSTGRES_USER'),
            'password': os.getenv('DEV_POSTGRES_PASSWORD'),
            'dbname': os.getenv('DEV_POSTGRES_DB'),
        }

        if yes_no('Initialize the dev database and logs?'):
            main()
