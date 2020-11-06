import os
import civis
import csv
import sys
from dotenv import load_dotenv


def main():
    path = 'to_civis.csv'

    if os.path.exists(path):
        os.remove(path)

    rows = []
    with open('from_drive_append.csv') as f:
        for row in csv.DictReader(f):
            if not row['van_id'].strip() or not row['If rejected, what is the basis for rejection?'].strip():
                sys.exit('Table is missing values')
            rows.append({
                'van_id': row['van_id'],
                'ballot_status': row['If rejected, what is the basis for rejection?']
            })

    with open(path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=['van_id', 'ballot_status'])
        writer.writeheader()
        writer.writerows(rows)

    fut = civis.io.csv_to_civis(
        filename=path,
        database='Dover',
        table='states_ia_projects.ia_observed_rejections',
        existing_table_rows='drop'
    )
    fut.result()

    os.remove(path)


if __name__ == '__main__':
    load_dotenv()

    main()
