# sql2pgsql
Short python script using sqlalchemy to copy the contents of ms sql tables to pgsql

Tables must be created in PG before running the script


# transfer_data.py
This script is designed to transfer data from a SQL Server database to a PostgreSQL database. It performs the following tasks:
- Connects to both the SQL Server and PostgreSQL databases.
- Fetches data from a specified table in the SQL Server database.
- Truncates the specified table in the PostgreSQL database.
- Inserts the fetched data into the PostgreSQL table.
- Adds spatial indexes to geometry columns in the PostgreSQL table for improved query performance.

## Requirements
- Python 3.x
- SQLAlchemy
- psycopg2
- geoalchemy2

## Usage
1. Install the required Python packages:
```bash
pip install SQLAlchemy psycopg2 geoalchemy2
3. Modify the script according to your database configurations and column mappings.
4. Run the script with the following command:

```bash
python3 transfer_data_script.py --ms-server <SQL_SERVER_HOSTNAME> --ms-database <SQL_SERVER_DATABASE_NAME> --ms-username <SQL_SERVER_USERNAME> --ms-password <SQL_SERVER_PASSWORD> --ms-table <SQL_SERVER_TABLE_NAME> --pg-host <POSTGRESQL_HOSTNAME> --pg-port <POSTGRESQL_PORT> --pg-database <POSTGRESQL_DATABASE_NAME> --pg-username <POSTGRESQL_USERNAME> --pg-password <POSTGRESQL_PASSWORD> --pg-table <POSTGRESQL_TABLE_NAME>



SqlAlchemy seems to read geometry (not supported) columns type as null, it was added into the data types declation as NULL: GEOMETRY, if your data has other unsupported data types this prob wont work correctly

# run_imports.py
## Overview
This Python script automates the process of transferring data from a Microsoft SQL Server (MS SQL) database to a PostgreSQL database. It reads table names from a file (`table_names.txt`) and runs the `transfer_data.py` script for each table.

## Requirements
- Python 3.x

## Usage
1. Modify the script to provide your database connection details:

```python
ms_server = "your_ms_server"
ms_database = "your_ms_database"
ms_username = "your_ms_username"
ms_password = "your_ms_password"
pg_host = "your_pg_host"
pg_port = "your_pg_port"
pg_database = "your_pg_database"
pg_username = "your_pg_username"
pg_password = "your_pg_password"

1. Ensure that the transfer_data.py script is present in the same directory as this script.
2. Create a file named table_names.txt and populate it with the names of the tables you want to transfer data for, each on a separate line.
3. Run the script with the following command:

```bash
python3 run_imports.py

## Script Details
The script reads table names from table_names.txt and executes transfer_data.py for each table, transferring data from MS SQL to PostgreSQL.
Each transfer operation is executed as a subprocess, capturing stdout and stderr to provide feedback on the process.

## Note
Ensure that both MS SQL and PostgreSQL databases are accessible from the machine running the script.
Double-check the table names in table_names.txt to ensure they match the tables in your MS SQL database.
