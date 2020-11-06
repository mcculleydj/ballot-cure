DROP TABLE IF EXISTS voters;
DROP TABLE IF EXISTS county_ids;
DROP TABLE IF EXISTS average_durations;
DROP TABLE IF EXISTS voter_demographics;
DROP TABLE IF EXISTS consolidated_demographics;
DROP TABLE IF EXISTS survey_responses;
DROP TABLE IF EXISTS wrong_numbers;
DROP TABLE IF EXISTS right_numbers;

CREATE TABLE voters (
    -- our data:
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    logs TEXT[],
    log TEXT,
    county_data BOOLEAN DEFAULT false,
    number_of_rows INTEGER,
    has_voided_ballot BOOLEAN DEFAULT false,
    was_removed BOOLEAN DEFAULT false,
    reject_date DATE,
    cure_date DATE,
    number_of_rejections INTEGER DEFAULT 0,
    was_ever_rejected BOOLEAN DEFAULT false,
    currently_rejected BOOLEAN DEFAULT false,
    reject_reason TEXT,

    -- their data:
    last_name  TEXT,
    first_name TEXT,
    middle_name TEXT,
    name_suffix TEXT,
    county TEXT,
    registration_number INTEGER UNIQUE,
    date_of_birth DATE,
    resident_address TEXT,
    mailing_address TEXT,
    absentee_address TEXT,
    phone_number TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    voter_status TEXT,
    party TEXT,
    precinct TEXT,
    ballot_status TEXT,
    request_date DATE,
    sent_date DATE,
    receive_date DATE,
    is_void BOOLEAN,
    comments TEXT,
    absentee_sequence_number TEXT,
    absentee_issue_method TEXT,
    absentee_receive_method TEXT,
    split TEXT
);

CREATE TABLE county_ids (
	last_name TEXT,
	first_name TEXT,
	middle_name TEXT,
	address_start TEXT,
	registration_number INTEGER UNIQUE
);

CREATE TABLE average_durations (
    county TEXT,
    time_to_return REAL,
    time_to_reject REAL,
    time_to_cure REAL
);

CREATE TABLE voter_demographics (
    registration_number INTEGER UNIQUE,
    party TEXT,
    ballot_status TEXT,
    county TEXT,
    reject_date DATE,
    cure_date DATE,
    age INTEGER,
    ethnicity TEXT,
    biden_support_score DOUBLE PRECISION,
    biden_turnout_score DOUBLE PRECISION,
    dscc_support_score DOUBLE PRECISION,
    dscc_turnout_score DOUBLE PRECISION,
    biden_support_bucket TEXT,
    dscc_support_bucket TEXT,
    voting_street_address TEXT,
    voting_street_address_2 TEXT,
    voting_city TEXT,
    voting_zip TEXT,
    voting_zip4 TEXT,
    voting_address_latitude TEXT,
    voting_address_longitude TEXT,
    voting_address_geocode_level TEXT,
    voting_address_type TEXT,
    voting_address_multi_tenant TEXT,
    voting_address_timezone TEXT,
    mailing_address_id TEXT,
    mailing_street_address TEXT,
    mailing_street_address_2 TEXT,
    mailing_city TEXT,
    mailing_zip TEXT,
    mailing_zip4 TEXT,
    mailing_state TEXT,
    mailing_address_type TEXT,
    best_number TEXT,
    best_number_type TEXT,
    best_number_wrong_number_score TEXT,
    best_number_quality_score TEXT,
    best_number_2 TEXT,
    best_number_2_type TEXT,
    best_number_2_wrong_number_score TEXT,
    best_number_2_quality_score TEXT,
    best_number_3 TEXT,
    best_number_3_type TEXT,
    best_number_3_wrong_number_score TEXT,
    best_number_3_quality_score TEXT,
    cell TEXT,
    cell_wrong_number_score TEXT,
    cell_quality_score TEXT,
    cell_2 TEXT,
    cell_2_wrong_number_score TEXT,
    cell_2_quality_score TEXT,
    cell_3 TEXT,
    cell_3_wrong_number_score TEXT,
    cell_3_quality_score TEXT,
    landline TEXT,
    landline_wrong_number_score TEXT,
    landline_quality_score TEXT,
    landline_2 TEXT,
    landline_2_wrong_number_score TEXT,
    landline_2_quality_score TEXT,
    landline_3 TEXT,
    landline_3_wrong_number_score TEXT,
    landline_3_quality_score TEXT,
    van_phone TEXT,
    van_id INTEGER
);

CREATE TABLE consolidated_demographics (
    county TEXT,
    demographic TEXT,
    total_votes INTEGER,
    category TEXT
);

CREATE TABLE survey_responses (
    registration_number INTEGER UNIQUE,
    myv_van_id INTEGER,
    van_status TEXT,
    van_status_date DATE,
    auditor_contact TEXT,
    auditor_contact_date DATE,
    has_plan TEXT,
    has_plan_date DATE,
    plan TEXT,
    plan_date DATE,
    follow_up TEXT,
    follow_up_date DATE,
    number_attempts INTEGER,
    phone_attempts INTEGER,
    text_attempts INTEGER,
    number_canvasses INTEGER,
    phone_canvasses INTEGER,
    text_canvasses INTEGER,
    most_recent_contact_attempt DATE
);

CREATE TABLE wrong_numbers (
  van_id INTEGER,
  number TEXT,
  source TEXT,
  PRIMARY KEY (van_id, number)
);

CREATE TABLE right_numbers (
  van_id INTEGER,
  number TEXT,
  source TEXT,
  PRIMARY KEY (van_id, number)
);

CREATE TABLE unknown_voters (
	county TEXT,
	last_name TEXT,
	first_name TEXT,
	address TEXT,
	city TEXT,
	note TEXT,
	registration_number INTEGER
);

CREATE TABLE observed_rejections (
	registration_number INTEGER UNIQUE,
	county TEXT,
	last_name TEXT,
	first_name TEXT,
	party TEXT,
	cell TEXT,
	landline TEXT,
	address TEXT,
	city TEXT
);


CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON voters
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();
