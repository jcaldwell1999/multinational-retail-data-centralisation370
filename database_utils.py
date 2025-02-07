import yaml
from sqlalchemy import create_engine

class DatabaseConnector:

    def read_db_creds(self, file_path='db_creds.yaml'):
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return engine