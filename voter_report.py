import civis
import os
import csv
import argparse
from dotenv import load_dotenv


def main():
    raw_path = 'voter_report_raw.csv'
    finished_path = 'voter_report.csv'

    if os.path.exists(raw_path):
        os.remove(raw_path)

    if os.path.exists(finished_path):
        os.remove(finished_path)

    query = (
        'select '
        '  person.myv_van_id, '
        '  person.sos_id, '
        '  person.first_name, '
        '  person.last_name, '
        '  person.county_name, '
        '  biden.bfp_support_score_targeting as biden_support_score, '
        '  dscc.support_score as dscc_support_score, '
        '  survey.survey_question_id, '
        '  survey.survey_question_name_truncated, '
        '  survey.survey_response_name_truncated, '
        '  survey.sq_most_recent_response '
        'from '
        '  my_state.person '
        'left join '
        '  my_state.all_scores as biden '
        'on '
        '  person.person_id = biden.person_id '
        'left join '
        '  states_ia_projects.civis_scores_sep2020 as dscc '
        'on '
        '  person.person_id = dscc.person_id '
        'left join '
        '  my_state_van.coord20_myv_001_responses as survey '
        'on '
        '  person.myv_van_id = survey.myv_van_id '
        f'where sos_id in {str(sos_ids).replace(",", "") if len(sos_ids) == 1 else sos_ids}'
    )

    fut = civis.io.civis_to_csv(
        filename=raw_path,
        sql=query,
        database='Dover',
    )
    fut.result()

    voters = {}

    relevant_survey_questions = {
        '405714',
        '405716',
        '408235',
        '427730',
        '427745',
        '427948',
        '427736',
        '431668'
    }

    with open(raw_path) as f:
        for row in csv.DictReader(f):
            if not voters.get(row['sos_id']):
                voters[row['sos_id']] = {
                    'van_id': row['myv_van_id'],
                    'sos_id': row['sos_id'],
                    'first': row['first_name'],
                    'last': row['last_name'],
                    'county': row['county_name'],
                    'biden_score': row['biden_support_score'],
                    'greenfield_score': row['dscc_support_score']
                }
            if row['sq_most_recent_response'] == '1' and row['survey_question_id'] in relevant_survey_questions:
                voters[row['sos_id']][row['survey_question_name_truncated']] = row['survey_response_name_truncated']

    for sos_id in sos_ids:
        if sos_id not in voters:
            print(f'\n {sos_id} NOT IN CIVIS DATABASE! \n')

    fieldnames = [
        'van_id',
        'sos_id',
        'first',
        'last',
        'county',
        'biden_score',
        'greenfield_score',
        '***IA Dem ID',
        '***IA POTUS ID',
        '***IA US Senate ID',
        'IA[Int] BallotReject',
        'Review Notice Receiv',
        'IA Cure Method',
        'IA Cure Plan',
        'IA CurePlan Followup',
    ]

    rows = list(voters.values())

    with open(finished_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ids', dest='sos_ids', nargs="+")
    args = parser.parse_args()
    sos_ids = tuple(args.sos_ids)

    load_dotenv()
    main()
