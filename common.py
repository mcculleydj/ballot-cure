import codecs
from distutils.util import strtobool
from constants import primary_sql_keys

"""
Common utility methods.
"""


def replace_bom(path):
    with codecs.open(path, encoding='utf-8', errors='ignore') as f:
        clean = f.read().replace('\ufeff', '')
    with open(path, 'w') as f:
        f.write(clean)


def yes_no(question, default='no'):
    if default is None:
        prompt = " [y/n] "
    elif default == 'yes':
        prompt = " [Y/n] "
    elif default == 'no':
        prompt = " [y/N] "
    else:
        raise ValueError(f"Unknown setting '{default}' for default.")

    while True:
        try:
            resp = input(question + prompt).strip().lower()
            if default is not None and resp == '':
                return default == 'yes'
            else:
                return strtobool(resp)
        except ValueError:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def pk_string(row):
    registration_number = f'{row["registration_number"]} | ' if row.get('registration_number') else ''
    return registration_number + ', '.join([row[k] or '' for k in primary_sql_keys])


def get_voter(cursor, voter_id):
    query = (
        'SELECT * '
        'FROM voters '
        'WHERE registration_number = %s'
    )
    cursor.execute(query, (voter_id,))
    result = cursor.fetchone()
    return dict(result) if result else None


def find_by_name_and_address(cursor, row):
    query = (
        'SELECT * '
        'FROM voters '
        'WHERE last_name = %s '
        'AND first_name = %s '
        'AND resident_address = %s'
    )

    query_args = (row['last_name'], row['first_name'], row['resident_address'])

    if row['middle_name']:
        query += ' AND middle_name = %s'
        query_args += (row['middle_name'],)
    else:
        query += ' AND middle_name IS NULL'

    if row['name_suffix']:
        query += ' AND name_suffix = %s'
        query_args += (row['name_suffix'],)
    else:
        query += ' AND name_suffix IS NULL'

    cursor.execute(query, query_args)
    existing_row = cursor.fetchone()
    return dict(existing_row) if existing_row else None


def find_by_registration_number(cursor, registration_number):
    query = (
        'SELECT * '
        'FROM voters '
        'WHERE registration_number = %s'
    )
    cursor.execute(query, (registration_number,))
    existing_row = cursor.fetchone()
    return dict(existing_row) if existing_row else None


