import csv
import argparse
import codecs
from constants import code_county_map


"""
Simple script to analyze the daily SoS CSV.
"""


def main():
    counties_reporting = set()
    all_counties = {k for k, _ in code_county_map.items()}

    req_tues = {}

    totals = {
        'dems': 0,
        'reps': 0,
        'oths': 0,
        'dems_rec': 0,
        'reps_rec': 0,
        'oths_rec': 0,
        'dems_rej': 0,
        'reps_rej': 0,
        'oths_rej': 0,
        'missing_pk': 0,
    }

    by_county = {}
    for k, v in code_county_map.items():
        by_county[k] = {
            'name': v['name'],
            'code': k,
            'dems': 0,
            'reps': 0,
            'oths': 0,
            'dems_rec': 0,
            'reps_rec': 0,
            'oths_rec': 0,
            'dems_rej': 0,
            'reps_rej': 0,
            'oths_rej': 0
        }

    pks = ['FIRST_NAME', 'LAST_NAME', 'RESIDENTIAL_ADDRESS_LINE_1']

    # shockingly these files are not well-encoded
    with codecs.open(f'csvs/sos/{args.day}.csv', encoding='utf-8', errors='ignore') as f:
        for row in csv.DictReader(f):
            if not all([row[pk] for pk in pks]):
                totals['missing_pk'] += 1
                continue
            county_code = row['COUNTY_CODE']

            if len(county_code) == 1:
                county_code = '0' + county_code

            if row['POLITICAL_PARTY'] == 'Democrat':
                key = 'dems'
            elif row['POLITICAL_PARTY'] == 'Republican':
                key = 'reps'
            else:
                key = 'oths'

            if row['REQUEST_DATE'] == '10/27/2020':
                req_tues[row['VOTER_ID']] = {
                    'ABSENTEE_ISSUE_METHOD': row['ABSENTEE_ISSUE_METHOD'],
                    'POLITICAL_PARTY': row['POLITICAL_PARTY'],
                    'DATE_OF_BIRTH': row['DATE_OF_BIRTH']
                }

            totals[key] += 1
            by_county[county_code][key] += 1
            if row['RECEIVED_DATE']:
                totals[f'{key}_rec'] += 1
                by_county[county_code][f'{key}_rec'] += 1
                if row['BALLOT_STATUS'] and 'Affidavit' in row['BALLOT_STATUS']:
                    totals[f'{key}_rej'] += 1
                    by_county[county_code][f'{key}_rej'] += 1
                    counties_reporting.add(county_code)

    # TODO: replace this console output with something more useful

    print('TOTALS')
    print('  Democrats')
    print('    ', totals['dems'], 'tracked;', totals['dems_rec'], 'received;', f'{round(100 * totals["dems_rec"] / totals["dems"], 2)}% return pct;', totals['dems_rej'], 'rejected;', f'{round(100 * totals["dems_rej"] / totals["dems_rec"], 2)}% rejection rate')
    print('  Republicans')
    print('    ', totals['reps'], 'tracked;', totals['reps_rec'], 'received;', f'{round(100 * totals["reps_rec"] / totals["reps"], 2)}% return pct;', totals['reps_rej'], 'rejected;', f'{round(100 * totals["reps_rej"] / totals["reps_rec"], 2)}% rejection rate')
    print('  Others')
    print('    ', totals['oths'], 'tracked;', totals['oths_rec'], 'received;', f'{round(100 * totals["oths_rec"] / totals["oths"], 2)}% return pct;', totals['oths_rej'], 'rejected;', f'{round(100 * totals["oths_rej"] / totals["oths_rec"], 2)}% rejection rate')

    for v in by_county.values():
        v['dems_rtn_pct'] = round(100 * v['dems_rec'] / v['dems'], 2)
        v['reps_rtn_pct'] = round(100 * v['reps_rec'] / v['reps'], 2)
        v['oths_rtn_pct'] = round(100 * v['oths_rec'] / v['oths'], 2)
        v['dems_rej_pct'] = round(100 * v['dems_rej'] / v['dems_rec'], 2)
        v['reps_rej_pct'] = round(100 * v['reps_rej'] / v['reps_rec'], 2)
        v['oths_rej_pct'] = round(100 * v['oths_rej'] / v['oths_rec'], 2)

    print('\nTOP COUNTIES')
    highest_dem_rej_counties = sorted(by_county.values(), key=lambda x: x['dems_rej'], reverse=True)[:5]
    highest_dem_rej_rate_counties = sorted(by_county.values(), key=lambda x: x['dems_rej_pct'], reverse=True)[:5]

    print('  Highest number of rejected Democratic ballots')
    for county in highest_dem_rej_counties:
        print('\n   ', county['name'])
        print('      Dem Ballots:', county['dems'])
        print('      Dem Received:', county['dems_rec'])
        print('      Dem Received Percentage:', county['dems_rtn_pct'])
        print('      Dem Rejected:', county['dems_rej'])
        print('      Dem Rejected Percentage:', county['dems_rej_pct'])
        print('      Rep Ballots:', county['reps'])
        print('      Rep Received:', county['reps_rec'])
        print('      Rep Received Percentage:', county['reps_rtn_pct'])
        print('      Rep Rejected:', county['reps_rej'])
        print('      Rep Rejected Percentage:', county['reps_rej_pct'])
        print('      Oth Ballots:', county['oths'])
        print('      Oth Received:', county['oths_rec'])
        print('      Oth Received Percentage:', county['oths_rtn_pct'])
        print('      Oth Rejected:', county['oths_rej'])
        print('      Oth Rejected Percentage:', county['oths_rej_pct'])

    print('\n  Highest rate of rejection for Democratic ballots')
    for county in highest_dem_rej_rate_counties:
        print('\n   ', county['name'])
        print('      Dem Ballots:', county['dems'])
        print('      Dem Received:', county['dems_rec'])
        print('      Dem Rejected:', county['dems_rej'])
        print('      Dem Rejected Percentage:', county['dems_rej_pct'])
        print('      Rep Ballots:', county['reps'])
        print('      Rep Received:', county['reps_rec'])
        print('      Rep Rejected:', county['reps_rej'])
        print('      Rep Rejected Percentage:', county['reps_rej_pct'])
        print('      Oth Ballots:', county['oths'])
        print('      Oth Received:', county['oths_rec'])
        print('      Oth Rejected:', county['oths_rej'])
        print('      Oth Rejected Percentage:', county['oths_rej_pct'])

    print('\nMissing first, last, or address:', totals['missing_pk'])

    counties_not_reporting = all_counties - counties_reporting
    print(len(all_counties - counties_reporting), 'counties not reporting any rejections to the SoS:', )
    counties = []
    for county_code in counties_not_reporting:
        total_received = by_county[county_code]['dems_rec'] + by_county[county_code]['reps_rec'] + by_county[county_code]['oths_rec']
        counties.append((code_county_map[county_code]["name"], total_received))
    counties = sorted(counties, key=lambda x: x[1], reverse=True)
    for county in counties:
        print(f'{county[0]}: {county[1]} ballots received') 

    yob_count = {}
    party_count = {}
    method_count = {}

    for k, v in req_tues.items():
        if yob_count.get(v['DATE_OF_BIRTH'].split('/')[2]):
            yob_count[v['DATE_OF_BIRTH'].split('/')[2]] += 1
        else:
            yob_count[v['DATE_OF_BIRTH'].split('/')[2]] = 1

        if party_count.get(v['POLITICAL_PARTY']):
            party_count[v['POLITICAL_PARTY']] += 1
        else:
            party_count[v['POLITICAL_PARTY']] = 1

        if method_count.get(v['ABSENTEE_ISSUE_METHOD']):
            method_count[v['ABSENTEE_ISSUE_METHOD']] += 1
        else:
            method_count[v['ABSENTEE_ISSUE_METHOD']] = 1

    ts = []
    for k, v in yob_count.items():
        ts.append((k, v))

    print(sorted(ts, key=lambda x: x[1], reverse=True))

    print(yob_count)
    print(party_count)
    print(method_count)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='day', required=True)
    args = parser.parse_args()
    main()
