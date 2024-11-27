import requests
from bs4 import BeautifulSoup
import zipfile
import io
import logging
# import log_config

def download_and_extract_zip(url, output_csv_filename):
    logging.info(f"Starting download from {url}")

    # Get the HTML content from the URL
    response = requests.get(url)
    response.raise_for_status()
    logging.info("Fetched HTML content from the URL")
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the .zip file link
    zip_link = None
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('.zip'):
            zip_link = url + href
            break
    
    if not zip_link:
        logging.error("No .zip file found at the specified URL")
        raise Exception("No .zip file found at the specified URL")
    
    logging.info(f"Found .zip file link: {zip_link}")
    
    # Download the .zip file
    zip_response = requests.get(zip_link)
    zip_response.raise_for_status()
    logging.info("Downloaded the .zip file")
    
    # Unzip the file and extract the CSV
    with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
        for file_name in z.namelist():
            if file_name.endswith('.csv'):
                with z.open(file_name) as csv_file:
                    with open(output_csv_filename, 'wb') as output_file:
                        output_file.write(csv_file.read())
                logging.info(f"Extracted {file_name} to {output_csv_filename}")
                return
    
    logging.error("No .csv file found in the .zip archive")
    raise Exception("No .csv file found in the .zip archive")

# Example usage
# url = "https://www2.census.gov/econ/bps/Master%20Data%20Set/"
# output_csv_filename = "data/census_data_extract.csv"
# download_and_extract_zip(url, output_csv_filename)

