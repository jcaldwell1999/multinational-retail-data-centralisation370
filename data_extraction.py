from database_utils import DatabaseConnector
import pandas as pd

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
