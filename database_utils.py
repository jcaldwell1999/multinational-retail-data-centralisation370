import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

class DatabaseConnector:
    """Handles connection to the database."""

    def __init__(self):
        """
        Initialises the database connection using credentials.
        """
        self.credentials = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self, filename='db_creds.yaml'):
        """Reads database credentials from a YAML file and returns a dictionary."""
        with open(filename, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        """Initializes a database connection using SQLAlchemy and returns the engine."""
        creds = self.read_db_creds()
        engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@"
            f"{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self):
        """Lists all tables in the database."""
        engine = self.init_db_engine()
        with engine.connect() as connection:
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';"))
            tables = [row[0] for row in result]
        return tables

    def upload_to_db(self, df, table_name):
        """
        Uploads Pandas DataFrame to a specified table in the datbase.

        Args:
            df (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the table to store data in.
        """
        try: 
            engine = create_engine("postgresql://postgres:password@localhost:5432/sales_data")
            # Upload data to the database
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Successfully uploaded data to teh table {table_name}.")
        except Exception as e:
            print(f" Error uploading data: {e}")