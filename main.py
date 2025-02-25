from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaner

# Initialize Classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor(db_connector)
data_cleaner = DataCleaner()

# Extract user data
print("Extracting user data from database...")
user_df = data_extractor.read_rds_table("legacy_users")
print("User data extracted.")

# Check missing values prior to cleaning:
print("\n Cleaning user data...")
cleaned_user_df = data_cleaner.clean_user_data(user_df)

# Preview of cleaned data
print("\n Cleaned Data Preview:")
print(cleaned_user_df.head(10))

# Upload cleaned data to the database
print("\n Uploading cleaned user data to the database...")
db_connector.upload_to_db(cleaned_user_df, "dim_users")
print("Data uploaded successfully.")

"""
# Check missing values prior to cleaning
print("\n Checking missing values before cleaning:")
print(user_df.isnull().sum())

# Clean user data
print("\n Cleaning user data...")
cleaned_user_df = data_cleaner.clean_user_data(user_df)
print("Data cleaned successfully.")

# Check missing values after cleaning
print("\n Checking missing values after cleaning:")
print(cleaned_user_df.isnull().sum())

# Preview of cleaned data
print("\n Cleaned User Data Preview:")
print(cleaned_user_df.head())
"""