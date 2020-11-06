DROP TABLE IF EXISTS civis_projection;

CREATE TABLE civis_projection (
    registration_number SERIAL PRIMARY KEY,
    party TEXT,
    ballot_status TEXT,
    county TEXT,
    reject_date DATE,
    cure_date DATE
);


INSERT INTO civis_projection (registration_number, party, ballot_status, county, reject_date, cure_date)
SELECT registration_number, party, ballot_status, county, reject_date, cure_date
FROM voters;

