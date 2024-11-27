import pandas as pd
import logging
# import log_config

def write_md_permits_to_csv(permit_data):
    logging.info("Starting to process Maryland permits data")
    locations = ['Place', 'County']
    
    # Filter for Maryland permits
    md_permits = permit_data[permit_data['STATE_NAME'].isin(['Maryland', 'MD'])]
    logging.info(f"Filtered Maryland permits: {len(md_permits)} rows")

    # Sort the data by 'YEAR', 'MONTH', and 'COUNTY_CODE'
    md_permits = md_permits.sort_values(by=['YEAR', 'MONTH', 'COUNTY_CODE'])
    logging.info("Sorted Maryland permits data")

    # Filter out rows where 'MONTH' is not in the range 1 to 12
    md_permits = md_permits[md_permits['MONTH'].between(1, 12)]
    logging.info(f"Filtered Maryland permits by month: {len(md_permits)} rows remaining")

    # Combine 'YEAR' and 'MONTH' columns into a 'date' column
    md_permits['date'] = pd.to_datetime(md_permits['YEAR'].astype(str) + '-' + md_permits['MONTH'].astype(str))
    logging.info("Added 'date' column to Maryland permits data")

    # Drop all rows where 'PERIOD' is not equal to 'Monthly'
    md_permits = md_permits[md_permits['PERIOD'] == 'Monthly']
    logging.info(f"Filtered Maryland permits by period: {len(md_permits)} rows remaining")

    # Save the filtered data to CSV files based on 'LOCATION_TYPE'
    for l in locations:
        location_data = md_permits[md_permits['LOCATION_TYPE'] == l]
        output_filename = f'../data/md_permits_monthly_{l}.csv'
        location_data.to_csv(output_filename, index=False)
        logging.info(f"Saved Maryland permits data for location type '{l}' to {output_filename}")

def write_national_permits_to_csv(permit_data):
    logging.info("Starting to process national permits data")
    locations = ['Metro']
    
    # Filter out rows where 'MONTH' is not in the range 1 to 12
    national_permits = permit_data[permit_data['MONTH'].between(1, 12)]
    logging.info(f"Filtered national permits by month: {len(national_permits)} rows remaining")

    # Combine 'YEAR' and 'MONTH' columns into a 'date' column
    national_permits['date'] = pd.to_datetime(national_permits['YEAR'].astype(str) + '-' + national_permits['MONTH'].astype(str))
    logging.info("Added 'date' column to national permits data")

    # Drop all rows where 'PERIOD' is not equal to 'Monthly'
    national_permits = national_permits[national_permits['PERIOD'] == 'Monthly']
    logging.info(f"Filtered national permits by period: {len(national_permits)} rows remaining")

    # Save the filtered data to CSV files based on 'LOCATION_TYPE'
    for l in locations:
        location_data = national_permits[national_permits['LOCATION_TYPE'] == l]
        if l == 'Metro':
            # Filter out rows where 'CBSA_NAME' does not contain 'MD'
            location_data = location_data[location_data['CBSA_NAME'].str.contains('MD', case=True, na=False)]
            logging.info(f"Filtered national permits for 'Metro' location type by 'CBSA_NAME': {len(location_data)} rows remaining")
        output_filename = f'../data/permits_monthly_{l}.csv'
        location_data.to_csv(output_filename, index=False)
        logging.info(f"Saved national permits data for location type '{l}' to {output_filename}")

# Example usage
# write_md_permits_to_csv(permit_data)
# write_national_permits_to_csv(permit_data)