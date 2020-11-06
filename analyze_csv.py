import csv
import codecs
import os
from dotenv import load_dotenv
from services import Postgres
"""
Warehouse for various sanity check methods and CSV analyzers.
"""

# count unique voter IDs
# voter_ids = set()
# with codecs.open('csvs/sos/10-15_6.csv', encoding='utf-8', errors='ignore') as f:
#     for row in csv.DictReader(f):
#         if row.get('VOTER_ID'):
#             voter_ids.add(row.get('VOTER_ID'))
# print('Number of unique voter IDs:', len(voter_ids))


# compare DB with Eva's CSVs of defective and deficient voters
load_dotenv()

postgres_args = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT')),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'dbname': os.getenv('POSTGRES_DB'),
}

with Postgres(**postgres_args) as cursor:
    query = (
        'SELECT registration_number '
        'FROM voters '
        'WHERE currently_rejected = true '
        'AND county != \'Polk\' AND county != \'Cerro Gordo\''
        'AND ballot_status like \'%Defective%\''
    )
    cursor.execute(query)
    db_vids = {dict(row)['registration_number'] for row in cursor.fetchall()}

csv_vids = set()

with codecs.open('defective.csv', encoding='utf-8', errors='ignore') as f:
    for row in csv.DictReader(f):
        if row.get('StateFileID'):
            csv_vids.add(int(row.get('StateFileID')))


print('in csv not in db')
print(csv_vids - db_vids)

print('in db not in csv')
print(db_vids - csv_vids)

with Postgres(**postgres_args) as cursor:
    for vid in csv_vids - db_vids:
        query = (
            'SELECT updated_at, ballot_status '
            'FROM voters '
            'WHERE registration_number = %s'
        )
        cursor.execute(query, (vid, ))
        row = dict(cursor.fetchone())
        print(row['updated_at'], row['ballot_status'])


