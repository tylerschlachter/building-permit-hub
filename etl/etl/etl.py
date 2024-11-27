# Import the functions from each script
from pull_census import download_and_extract_zip
from filter_and_write_to_csv import write_md_permits_to_csv, write_national_permits_to_csv
from sql_create import create_tables
from sql_write import write_to_db
import pandas as pd
import log_config


def main():
    """
    Run this from building-permit-hub/etl/etl
    """
    # Step 1: Pull census data
    url = "https://www2.census.gov/econ/bps/Master%20Data%20Set/"
    output_census_csv_filename = "../data/census_data_extract.csv"
    download_and_extract_zip(url, output_census_csv_filename)

    # Step 2: Filter and write to CSV
    permit_data = pd.read_csv(output_census_csv_filename, encoding='latin1')
    write_md_permits_to_csv(permit_data)
    write_national_permits_to_csv(permit_data)

    # Step 3: Create SQL tables
    create_tables(dry_run=False)

    # Step 4: Write data to SQL
    csv_db_mapping = {
        '../data/md_permits_monthly_County.csv': 'md_permits_county',
        '../data/md_permits_monthly_Place.csv': 'md_permits_place',
        '../data/permits_monthly_Metro.csv': 'permits_metro'
    }
    write_to_db(csv_db_mapping)


if __name__ == "__main__":
    main()
