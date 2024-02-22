import argparse
import logging
from sqlalchemy import create_engine, Column, MetaData, Table, text
from sqlalchemy.engine import reflection
from geoalchemy2 import Geometry,WKTElement
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set the logging level to DEBUG
logger = logging.getLogger(__name__)  # Create a logger for the current module

# Define type mapping dictionary for converting SQL Server types to PostgreSQL types
type_mapping = {
    'NVARCHAR': 'VARCHAR',
    'DATETIME': 'TIMESTAMP',
    'BIT': 'BOOLEAN',
    'NULL': 'GEOMETRY'
    # Add more mappings as needed
}



def get_column_info(engine, table_name):
    insp = reflection.Inspector.from_engine(engine)
    columns = insp.get_columns(table_name)
    column_info = []

    for col in columns:
        col_name = col['name']

        logging.debug(f"coltype: {col['type']}")
        col_type_str = str(col['type']).split(' COLLATE ')[0]
        col_type = type_mapping.get(col_type_str.upper(),col_type_str)

        # Check if the column is of type 'geometry'
        if col_type is not None and str(col_type).lower() == 'geometry':
            # Query for SRID of the geometry column
            query = f"SELECT TOP (1) {col_name}.STSrid FROM {table_name}"
            with engine.begin() as conn:
                srid_result = conn.execute(query).fetchone()
            if srid_result:
                srid = srid_result[0]
            else:
                srid = None
            column_info.append((col_name, col_type, srid))
        else:
            column_info.append((col_name, col_type, None))

    return column_info




def transfer_data(ms_sql_engine, pg_engine, ms_sql_table, pg_table):
    ms_sql_conn = ms_sql_engine.connect()
    pg_conn = pg_engine.connect()

    

    logger.info("Connected to SQL Server and PostgreSQL")

    # Get column info from SQL Server
    column_info = get_column_info(ms_sql_engine, ms_sql_table)

    #logger.info("Table created in PostgreSQL")

    # Get column names
    pg_columns = [col[0] for col in column_info]

    try:
        # Execute query to fetch data from MS SQL Server
        # ms_sql_result = ms_sql_conn.execute(f'SELECT {", ".join(pg_columns)} FROM {ms_sql_table}')
        logger.debug(f"SQLQUERYGEN: SELECT {', '.join([f'{col[0]}.STAsText() AS {col[0]}' if col[1].upper() == 'GEOMETRY' else col[0] for col in column_info])} FROM {ms_sql_table}")
        ms_sql_result = ms_sql_conn.execute(f"SELECT {', '.join([f'{col[0]}.STAsText() AS {col[0]}' if col[1].upper() == 'GEOMETRY' else col[0] for col in column_info])} FROM {ms_sql_table}")
        

        logger.info("Data fetched from SQL Server")

        # Fetch data and insert into PostgreSQL
        pg_table_obj = Table(pg_table, MetaData(), autoload_with=pg_engine)
        
        pg_conn.execute(pg_table_obj.delete())
        logger.info("Destination table truncated")

        pg_rows= []
        for row in ms_sql_result:
            pg_row = {}
            for col_name, col_type, epsg in column_info:
                # Include the primary key field directly from ms_sql_result
                if col_name in pg_columns and pg_table_obj.columns[col_name.lower()].primary_key:
                    # Explicitly set the primary key value from the source
                    pg_row[col_name.lower()] = row[col_name]
                elif col_type.lower() == 'geometry':
                    # Convert WKT to Geometry type for PostgreSQL
                    pg_row[col_name.lower()] = WKTElement(row[col_name], srid=epsg)  # Assuming SRID 4326, adjust as needed
                else:
                    pg_row[col_name.lower()] = row[col_name]

            pg_rows.append(pg_row)

        # Insert data into PostgreSQL using bulk insert
        if pg_rows:
            pg_conn.execute(pg_table_obj.insert(), pg_rows)

        logger.info("Data inserted into PostgreSQL")

        # Drop existing indexes before adding new spatial indexes
        for col_name, _, _ in column_info:
            index_name = f"{pg_table}_{col_name}_spatial_index"
            drop_index_stmt = f"DROP INDEX IF EXISTS {index_name};"
            pg_conn.execute(text(drop_index_stmt))

            logger.info(f"Dropped existing index {index_name}")

        # Add spatial indexes after inserting data
        for col_name, col_type, _ in column_info:
            if col_type.lower() == 'geometry':
                index_name = f"{pg_table}_{col_name}_spatial_index"

                drop_index_stmt = f"DROP INDEX IF EXISTS {index_name};"
                pg_conn.execute(text(drop_index_stmt))
                logger.info(f"Dropped existing index {index_name}")

                index_stmt = f"CREATE INDEX {index_name} ON {pg_table} USING GIST ({col_name});"
                pg_conn.execute(text(index_stmt))
                logger.info(f"Spatial index added on column {col_name}")


    except Exception as e:
        logger.error(f"An error occurred: {e}")
        # Rollback the transaction in case of error
        pg_conn.rollback()
    finally:
        # Close connections
        ms_sql_conn.close()
        pg_conn.close()

        logger.info("Connections closed")

def main():
    parser = argparse.ArgumentParser(description='Transfer data from SQL Server to PostgreSQL.')
    parser.add_argument('--ms-server', type=str, help='SQL Server hostname', required=True)
    parser.add_argument('--ms-database', type=str, help='SQL Server database name', required=True)
    parser.add_argument('--ms-username', type=str, help='SQL Server username', required=True)
    parser.add_argument('--ms-password', type=str, help='SQL Server password', required=True)
    parser.add_argument('--ms-table', type=str, help='SQL Server table name', required=True)
    parser.add_argument('--pg-host', type=str, help='PostgreSQL hostname', required=True)
    parser.add_argument('--pg-port', type=str, help='PostgreSQL port', required=True)
    parser.add_argument('--pg-database', type=str, help='PostgreSQL database name', required=True)
    parser.add_argument('--pg-username', type=str, help='PostgreSQL username', required=True)
    parser.add_argument('--pg-password', type=str, help='PostgreSQL password', required=True)
    parser.add_argument('--pg-table', type=str, help='PostgreSQL table name', required=True)
    args = parser.parse_args()

    # Create SQL Server engine
    ms_sql_engine = create_engine(f'mssql+pyodbc://{args.ms_username}:{args.ms_password}@{args.ms_server}/{args.ms_database}?driver=ODBC+Driver+17+for+SQL+Server&timeout=30')

    # Create PostgreSQL engine
    pg_engine = create_engine(f'postgresql+psycopg2://{args.pg_username}:{args.pg_password}@{args.pg_host}:{args.pg_port}/{args.pg_database}')

    # Transfer data
    transfer_data(ms_sql_engine, pg_engine, args.ms_table, args.pg_table)

if __name__ == "__main__":
    main()
