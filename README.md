# sql2pgsql
Short python script using sqlalchemy to copy the contents of ms sql tables to pgsql

Tables must be created in PG before running the script


# transfer_data.py
Accepts command line connection details and table name
Truncates the PG table
inserts new data (column names must match but it allows for PG being lowercase and MS SQL not)
delete spatial index if exists
create spatial index

sqlAlchemy seem to read geometry (not supported) columns type as null, iadded it into the data types declation, if your data has other unsupported data types this prob wont work

# run_imports.py
uses conenction details from vars and a same folder txt file of table names to process multiple tables through the transfer_data script
