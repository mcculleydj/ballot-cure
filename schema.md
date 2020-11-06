# TODO

- text => enums (do this in code not in PSQL, whenever there is a discrete list of possibilities)
- breakdown SoS schema
- answer 2 remaining questions on county schema

# CSV Schemas

## iVoters Schema (standard county CSV)

- LAST_NAME
- FIRST_NAME
- MIDDLE_NAME
- NAME_SUFFIX
- DATE_OF_BIRTH
- PHONE_NUMBER
- REGN_NUM
- PRECINCT
- SPLIT
- VOTERSTATUS
- PARTY
- RES_ADD
- CITY
- COUNTY
- STATE
- ZIP
- MAILING_ADDRESS
- ABSENTEE_ADDRESS
- BALLOTSTATUS
- REQUEST_DATE
- SENT_DATE
- RECEIVE_DATE
- IS_VOID
- COMMENTS
- ABSENTEE_SEQUENCE_NUMBER
- ABSENTEE_ISSUE_METHOD
- ABSENTEE_RECEIVE_METHOD

## Secretary of State Schema

- COUNTY_CODE
- ELECTION_DATE
- PRECINCT_CODE
- PRECINCT_NAME
- STATE_HOUSE_NAME
- STATE_SENATE_NAME
- CONGRESSIONAL
- POLITICAL_PARTY
- IS_VOID
- REQUEST_DATE
- SENT_DATE
- RECEIVED_DATE
- VOTER_ID
- STATUS
- FIRST_NAME
- MIDDLE_NAME
- LAST_NAME
- NAME_SUFFIX
- RESIDENTIAL_ADDRESS_LINE_1
- CT_CITY
- CT_ST_STATE
- ZIP_ZIP_CODE
- ZIP_PLUS
- HOME_PHONE
- MODIFIED_DATE
- IS_STANDARD
- ABSENTEE_SEQUENCE_NUMBER
- ABSENTEE_ISSUE_METHOD
- RECEIVE_METHOD
- DATE_OF_BIRTH
- MAIL_ADDRESS
- MAIL_CITY
- MAIL_STATE
- MAIL_ZIP
- MAIL_ZIP_PLUS
- FPCA
- BALLOT_STATUS
- COMMENTS
- SPLIT

# Voters Schema

## our columns

- id | serial | PSQL ID
- created_at | timestamptz | when this ballot was inserted
- updated_at | timestamptz | when this ballot was last updated
- logs | text[] | track changes to this voter as an array for backend
- log | text | track changes to this voter as a string for Data Studio
- reject_date | date | directory date (e.g. 10-10) that the voter's ballot first appears as rejected 
- cure_date | date | directory date that the voter's ballot goes from rejected => null; a non-null status implies a null cure date
- county_data | boolean | whether or not this row came from a county CSV
- number_of_rows | integer | the number of rows matching this voter in the latest CSV
- has_voided_ballot | boolean | true if this voter ever had a row where is_void was true
- was_removed | boolean | true if this voter appeared in an earlier CSV but does not appear in the latest CSV

## columns coming from the CSV

### PII
- last_name | text | voter's last name
- first_name | text | voter's first name
- middle_name | text | voter's middle name
- name_suffix | text | voter's name suffix
- date_of_birth | date | voter's DOB
- phone_number | text | voter's phone number

### Voter
- registration_number | integer | SoS voter ID
- precinct | text | based on resident address and defines your polling location
- split | text | qualifier on the precinct column a voter may reside in a split precinct
- voter_status | text | registration status _confirm: "active", "inactive", "cancelled", "incomplete", and "pending"_
- party | text | voter's registered party affiliation

### Address
- resident_address | text | voter's resident address
- city | text | voter's resident address city
- county | text | voter's resident address county
- state | text | voter's resident address state (should always be IA _confirm this_)
- zip | text | voter's resident address zip code
- mailing_address | text | voter's mailing address if different from resident address (e.g. like a P.O Box)
- absentee_address | text | where this ballot should be sent for this election

### Ballot
- ballot_status | text | deficient (rejected signature), defective (rejected for any other reason) or null
- request_date | date | date county received a request for an absentee ballot
- sent_date | date | date county sent voter their absentee ballot
- receive_date | date | date county received submitted absentee ballot from voter
- is_void | bool | ??? this the ballot or th request or both
- comments | text | notes from county / election officials
- absentee_sequence_number | text | ??? because the SoS requires all absentee ballots to be reported this is a state-wide ballot ID
- absentee_issue_method | text | method the county provided the ballot to the voter: email (overseas only), in person, or mail
- absentee_receive_method | text | method the voter provided the ballot back to the county: satellite, counter / in person, email, or mail

# Polk IDs

- last_name | text | last name of the voter as it appears in the Polk CSV
- first_name | text | first name of the voter as it appears in the Polk CSV
- address_start | text | the first two whitespace separated strings in the address made lowercase (e.g. 123 MAIN => 123 main)
- registration_number | integer unique | SoS voter ID
- voter_psql_id | integer | foreign key (in name only) matching the id of the voter in the voters table

# Voter Demographics

- registration_number | integer | SoS voter ID
- ballot_status | text |  deficient (rejected signature), defective (rejected for any other reason) or null
- county | text | voter's resident address county
- reject_date | date | directory date (e.g. 10-10) that the voter's ballot first appears as rejected
- cure_date | date |
- age | INTEGER,
- ethnicity | TEXT,
- biden_support_score | DOUBLE PRECISION,
- biden_turnout_score | DOUBLE PRECISION,
- dscc_support_score | DOUBLE PRECISION,
- dscc_turnout_score | DOUBLE PRECISION
