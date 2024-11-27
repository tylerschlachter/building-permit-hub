import pandas as pd
from sqlalchemy import create_engine
import logging
# import log_config
from dotenv import load_dotenv
import os


def write_to_db(csv_db_mapping):
    # Load environment variables from .env file
    # load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
    load_dotenv(".env")

    # Database connection string
    db_connection_string = f"postgresql://{os.getenv('RDS_USER')}:{os.getenv(
        'RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('DB_NAME')}"

    logging.info("Starting to write CSV data to database tables")
    engine = create_engine(db_connection_string)

    for csv_source, db_table_name in csv_db_mapping.items():
        try:
            logging.info(f"Reading CSV file: {csv_source}")
            data = pd.read_csv(csv_source, encoding='latin1')
            # Convert column names to lowercase
            data.columns = [col.lower() for col in data.columns]

            logging.info(f"Writing data to table: {db_table_name}")
            data.to_sql(db_table_name, engine,
                        if_exists='replace', index=False)
            logging.info(f"Successfully wrote data from {
                         csv_source} to {db_table_name}")
        except Exception as e:
            logging.error(f"Error writing data from {
                          csv_source} to {db_table_name}: {e}")

# Example usage
# csv_db_mapping = {
#     'data/md_permits_monthly_County.csv': 'md_permits_county',
#     'data/md_permits_monthly_Place.csv': 'md_permits_place',
#     'data/permits_monthly_Metro.csv': 'permits_metro'
# }

# write_to_db(csv_db_mapping)
