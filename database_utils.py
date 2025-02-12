import yaml
from sqlalchemy import create_engine, text

class DatabaseConnector:
    """Handles connection to the database."""

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
