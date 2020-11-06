### Ingest Scripts

Process county and SoS data for the purposes of helping cure ballots. This was built targeting a GCP PSQL instance. You will need your own PSQL instance to run this without modifying the code. In the csv directory create at least one sub-directory named for a date in this format MM-DD (e.g. 10-06). In that sub-directory place at least one CSV named for a county (e.g. Polk.csv).

- service.py: defines the Postgres context manager class, you will need to supply your own values for host, dbname, etc. either in the constructor call or as hardcoded defaults in this file
- constants.py: exists to share common constants between other scripts and keep them out of the way
- initialize.py: removes all logs then drops and recreates the voters table (executes schema.sql)
- ingest_county.py: target a directory that is named following this format MM-DD containing one or more county CSV files to ingest this content into the database
  - relies on logging both to the DB and to flat files to track changes over time to voter records
  - CSVs should be ingested in chronological order from oldest to most recent
- digest_sos.py: process the SoS daily CSV (does not yet interact with the persistent layer) to output the top 5 counties by number rejected and rejection rate. easily extended to answer specific questions, e.g. how many counties are reporting at least one rejected ballot?
- schema.md: a description of the fields in the county CSVs, SoS CSV, and the schema defined in schema.sql

### Table Parser

a demonstration using AWS (textract) to convert images of tabular data into a CSV. to run this you will need to supply your own AWS credentials and a test file converted into images. e.g. break Howard 10.08.pdf into 5 images files and use those image files as inputs to table_parser.py. early tests are promising
that we can use this to convert scanned PDFs into usable data. 