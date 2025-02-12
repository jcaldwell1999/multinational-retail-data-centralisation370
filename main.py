from database_utils import DatabaseConnector
from data_extraction import DataExtractor

# Initialize DatabaseConnector
db_connector = DatabaseConnector()

# Initialize DataExtractor and pass the db_connector instance
data_extractor = DataExtractor(db_connector)

# List Tables
tables = data_extractor.extract_table_names()
print("Tables in database:", tables)

# Read user the table
user_table_name = "legacy_users"  # Change this if the table name is different
df_users = data_extractor.read_rds_table(user_table_name)

print("User table preview:")
print(df_users.head())  # Show first few rows
