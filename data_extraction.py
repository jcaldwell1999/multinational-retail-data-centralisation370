from database_utils import DatabaseConnector
import pandas as pd
from tabula import read_pdf
import requests
import time # To add delays
import boto3
from io import StringIO

class DataExtractor:
    """Utility class for extracting data from various sources."""

    def __init__(self, db_connector):
        """
        Initialize DataExtractor with a DatabaseConnector instance.
        
        :param db_connector: An instance of DatabaseConnector.
        """
        self.db_connector = db_connector  # Store DatabaseConnector instance

    def extract_table_names(self):
        """
        Retrieve all table names from the database.
        
        :return: List of table names.
        """
        return self.db_connector.list_db_tables()

    def read_rds_table(self, table_name):
        """
        Extracts the specified table from the RDS database.

        :param table_name: Name of the table to extract.
        :return: DataFrame containing the table data.
        """
        engine = self.db_connector.init_db_engine()
        with engine.connect() as connection:
            df = pd.read_sql(f"SELECT * FROM {table_name}", connection)
        return df
    
    def retrieve_pdf_data(self, pdf_url):
        """
        Extracts table data from a PDF file.
        
        :param pdf_url: The URL or local path of the PDF file.
        :return: DataFrame containing extracted table data.
        """
        print(f"Extracting data from PDF: {pdf_url}")

        # Extract all tables from the PDF as a list of DataFrames
        dfs = read_pdf(pdf_url, pages="all", multiple_tables=True)

        df = pd.concat(dfs, ignore_index=True)

        print("Extraction complete. Preview of data:")
        print(df.head())

        return df

    def list_number_of_stores(self, url, headers):
        """
        Retrieves the number of stores from the API.

        :param url: API endpoint for fetching the store count.
        :param headers: Dictionary containing API key.
        :return: Total number of stores as an integer.
        """
        response = requests.get(url, headers=headers) # Make GET request
        if response.status_code == 200: # Check if request was successful
            return response.json()['number_stores']
        else: 
            raise Exception(f"Failed to retrieve number of stores. Status code: {response.status_code}")
        
    def retrieve_stores_data(self, url, headers, store_count):
        """
        Extracts details for each store from the API.

        :param url: Store details API endpoint.
        :param headers: Dictionary containing the API key.
        :param store_count: Total number of stores to retrieve.
        :return: DataFrame containing all store details.
        """
        stores_data = []
        
        for store_number in range(1, store_count + 1): # Loop through store numbers
            response = requests.get(url.format(store_number=store_number), headers=headers)
            if response.status_code == 200:
                store_info = response.json()
                stores_data.append(store_info)
            else:
                print(f"Failed to retrieve store {store_number}. Skipping...")
            
            time.sleep(0.2) # Add delay to prevent API rate limits

        return pd.DataFrame(stores_data) # Convert list to DataFrame
    
    def extract_from_s3(self, s3_uri):
        """
        Extracts a CSV file from a public S3 bucket and returns a pandas DataFrame.

        :param s3_uri: S3 URI to the CSV file
        :return: pandas DataFrame with the product data
        """
        # Parse the bucket name and key from the s3_uri
        s3_uri = s3_uri.replace("s3://", "")
        bucket_name, *key_parts = s3_uri.split("/")
        key = "/".join(key_parts)

        # Create S3 Client
        s3 = boto3.client('s3')

        # Get the object from S3
        obj = s3.get_object(Bucket=bucket_name, Key=key)

        # Read the CSV content
        df = pd.read_csv(obj['Body'])

        return df
    
    def extract_json_data(self, url: str) -> pd.DataFrame:
        """
        Downloads and loads a JSON file from a URL into a DataFrame.
        """
        print(f"Extracting JSON data from {url}")
        response = requests.get(url)
        response.raise_for_status() # Raise error if the request fails.
        data = response.json()
        df = pd.DataFrame(data)
        print("JSON extraction complete. Preview:")
        print(df.head())
        return df