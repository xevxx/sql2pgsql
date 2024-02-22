import subprocess
import sys
import urllib.parse
# Define connection details
ms_server = "your_ms_server"
ms_database = "your_ms_database"
ms_username = "your_ms_username"
ms_password = "your_ms_password"
pg_host = "your_pg_host"
pg_port = "your pg port"
pg_database = "your_pg_database"
pg_username = "your_pg_username"
pg_password = "your_pg_password"

ms_password = urllib.parse.quote_plus(ms_password)
pg_password = urllib.parse.quote_plus(pg_password)

# Function to run transfer_data.py for a given table name
def transfer_data_for_table(table_name):
    command = ["python3", "./transfer_data.py", "--ms-server", ms_server, "--ms-database", ms_database, "--ms-username", ms_username, "--ms-password", ms_password, "--ms-table", table_name, "--pg-host", pg_host, "--pg-port", pg_port, "--pg-database", pg_database, "--pg-username", pg_username, "--pg-password", pg_password, "--pg-table", table_name.lower()]
    # Print the command to the terminal log
    print("Executing command:", " ".join(command))
    # Execute the command
     # Open subprocess to capture stdout and stderr
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True) as process:
        # Read and print stdout and stderr line by line
        for line in process.stdout:
            print(line.strip())
        for line in process.stderr:
            print(line.strip(), file=sys.stderr)

    # Check the return code of the subprocess
    if process.returncode != 0:
        print(f"An error occurred while transferring data for table {table_name}.")
        # Handle the error accordingly
    else:
        print(f"Data transfer for table {table_name} completed successfully.")

# Read table names from table_names.txt and run transfer_data.py for each table
with open("table_names.txt", "r") as file:
    for line in file:
        table_name = line.strip()
        transfer_data_for_table(table_name)
