import psycopg2
import logging
# import log_config
from dotenv import load_dotenv
import os

# Load environment variables from .env file
# load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def create_tables(dry_run=False):
    # load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
    load_dotenv(".env")
    logging.info("Running create_tables function")
    commands = (
        """
        CREATE TABLE IF NOT EXISTS md_permits_county (
            id SERIAL PRIMARY KEY,
            bldgs_1_unit INTEGER,
            bldgs_1_unit_rep FLOAT,
            bldgs_2_units INTEGER,
            bldgs_2_units_rep FLOAT,
            bldgs_3_4_units INTEGER,
            bldgs_3_4_units_rep FLOAT,
            bldgs_5_units INTEGER,
            bldgs_5_units_rep FLOAT,
            cbsa_code FLOAT,
            cbsa_name VARCHAR(255),
            census_place_code FLOAT,
            central_city FLOAT,
            county_code FLOAT,
            county_name VARCHAR(255),
            csa_code FLOAT,
            division_code FLOAT,
            division_name VARCHAR(255),
            file_name VARCHAR(255),
            fips_county_5_digits FLOAT,
            fips_mcd_code FLOAT,
            fips_place_code FLOAT,
            footnote_code FLOAT,
            id_6_digit FLOAT,
            location_type VARCHAR(255),
            moncov FLOAT,
            month INTEGER,
            number_of_months_rep FLOAT,
            period VARCHAR(255),
            place_name VARCHAR(255),
            unique_place_id FLOAT,
            pop FLOAT,
            region_code FLOAT,
            region_name VARCHAR(255),
            source_code FLOAT,
            state_code FLOAT,
            state_name VARCHAR(255),
            survey_date VARCHAR(255),
            total_bldgs INTEGER,
            total_bldgs_rep FLOAT,
            total_units INTEGER,
            total_units_rep FLOAT,
            total_value FLOAT,
            total_value_rep FLOAT,
            units_1_unit INTEGER,
            units_1_unit_rep FLOAT,
            units_2_units INTEGER,
            units_2_units_rep FLOAT,
            units_3_4_units INTEGER,
            units_3_4_units_rep FLOAT,
            units_5_units INTEGER,
            units_5_units_rep FLOAT,
            value_1_unit FLOAT,
            value_1_unit_rep FLOAT,
            value_2_units FLOAT,
            value_2_units_rep FLOAT,
            value_3_4_units FLOAT,
            value_3_4_units_rep FLOAT,
            value_5_units FLOAT,
            value_5_units_rep FLOAT,
            year INTEGER,
            zip_code FLOAT,
            location_name VARCHAR(255),
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS md_permits_place (
            id SERIAL PRIMARY KEY,
            bldgs_1_unit INTEGER,
            bldgs_1_unit_rep FLOAT,
            bldgs_2_units INTEGER,
            bldgs_2_units_rep FLOAT,
            bldgs_3_4_units INTEGER,
            bldgs_3_4_units_rep FLOAT,
            bldgs_5_units INTEGER,
            bldgs_5_units_rep FLOAT,
            cbsa_code FLOAT,
            cbsa_name VARCHAR(255),
            census_place_code FLOAT,
            central_city FLOAT,
            county_code FLOAT,
            county_name VARCHAR(255),
            csa_code FLOAT,
            division_code FLOAT,
            division_name VARCHAR(255),
            file_name VARCHAR(255),
            fips_county_5_digits FLOAT,
            fips_mcd_code FLOAT,
            fips_place_code FLOAT,
            footnote_code FLOAT,
            id_6_digit FLOAT,
            location_type VARCHAR(255),
            moncov FLOAT,
            month INTEGER,
            number_of_months_rep FLOAT,
            period VARCHAR(255),
            place_name VARCHAR(255),
            unique_place_id FLOAT,
            pop FLOAT,
            region_code FLOAT,
            region_name VARCHAR(255),
            source_code FLOAT,
            state_code FLOAT,
            state_name VARCHAR(255),
            survey_date VARCHAR(255),
            total_bldgs INTEGER,
            total_bldgs_rep FLOAT,
            total_units INTEGER,
            total_units_rep FLOAT,
            total_value FLOAT,
            total_value_rep FLOAT,
            units_1_unit INTEGER,
            units_1_unit_rep FLOAT,
            units_2_units INTEGER,
            units_2_units_rep FLOAT,
            units_3_4_units INTEGER,
            units_3_4_units_rep FLOAT,
            units_5_units INTEGER,
            units_5_units_rep FLOAT,
            value_1_unit FLOAT,
            value_1_unit_rep FLOAT,
            value_2_units FLOAT,
            value_2_units_rep FLOAT,
            value_3_4_units FLOAT,
            value_3_4_units_rep FLOAT,
            value_5_units FLOAT,
            value_5_units_rep FLOAT,
            year INTEGER,
            zip_code FLOAT,
            location_name VARCHAR(255),
            date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS permits_metro (
            id SERIAL PRIMARY KEY,
            bldgs_1_unit INTEGER,
            bldgs_1_unit_rep FLOAT,
            bldgs_2_units INTEGER,
            bldgs_2_units_rep FLOAT,
            bldgs_3_4_units INTEGER,
            bldgs_3_4_units_rep FLOAT,
            bldgs_5_units INTEGER,
            bldgs_5_units_rep FLOAT,
            cbsa_code FLOAT,
            cbsa_name VARCHAR(255),
            census_place_code FLOAT,
            central_city FLOAT,
            county_code FLOAT,
            county_name VARCHAR(255),
            csa_code FLOAT,
            division_code FLOAT,
            division_name VARCHAR(255),
            file_name VARCHAR(255),
            fips_county_5_digits FLOAT,
            fips_mcd_code FLOAT,
            fips_place_code FLOAT,
            footnote_code FLOAT,
            id_6_digit FLOAT,
            location_type VARCHAR(255),
            moncov VARCHAR(255),
            month INTEGER,
            number_of_months_rep FLOAT,
            period VARCHAR(255),
            place_name VARCHAR(255),
            unique_place_id FLOAT,
            pop FLOAT,
            region_code FLOAT,
            region_name VARCHAR(255),
            source_code FLOAT,
            state_code FLOAT,
            state_name VARCHAR(255),
            survey_date VARCHAR(255),
            total_bldgs INTEGER,
            total_bldgs_rep FLOAT,
            total_units INTEGER,
            total_units_rep FLOAT,
            total_value FLOAT,
            total_value_rep FLOAT,
            units_1_unit INTEGER,
            units_1_unit_rep FLOAT,
            units_2_units INTEGER,
            units_2_units_rep FLOAT,
            units_3_4_units INTEGER,
            units_3_4_units_rep FLOAT,
            units_5_units INTEGER,
            units_5_units_rep FLOAT,
            value_1_unit FLOAT,
            value_1_unit_rep FLOAT,
            value_2_units FLOAT,
            value_2_units_rep FLOAT,
            value_3_4_units FLOAT,
            value_3_4_units_rep FLOAT,
            value_5_units FLOAT,
            value_5_units_rep FLOAT,
            year INTEGER,
            zip_code FLOAT,
            location_name VARCHAR(255),
            date DATE
        )
        """
    )
    try:
        # Connect to the PostgreSQL database using environment variables
        connection = psycopg2.connect(
            host=os.getenv("RDS_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("RDS_USER"),
            password=os.getenv("RDS_PASSWORD"),
            port=os.getenv("RDS_PORT")
        )
        cursor = connection.cursor()
        logging.info("Database connection successful")

        if dry_run:
            logging.info("Dry run mode: Printing SQL commands and checking table existence")
            for command in commands:
                print(command)
            
            # Check if tables exist
            table_names = ["md_permits_county", "md_permits_place", "permits_metro"]
            for table in table_names:
                cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}');")
                exists = cursor.fetchone()[0]
                if exists:
                    logging.info(f"Table '{table}' exists")
                else:
                    logging.info(f"Table '{table}' does not exist")
        else:
            # Create tables
            for command in commands:
                cursor.execute(command)
            # Commit the changes
            connection.commit()
            logging.info("Tables created successfully")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

# Call the function to create tables
# create_tables(dry_run=False)

