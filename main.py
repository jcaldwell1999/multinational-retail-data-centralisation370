from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaner
from pandasgui import show

# Initialize Classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor(db_connector)
data_cleaner = DataCleaner()

# Extract user data
print("Extracting user data from database...")
user_df = data_extractor.read_rds_table("legacy_users")
print("User data extracted.")
#print(user_df.columns)
#if 'index' in user_df.columns:
#    user_df = user_df.rename(columns={'index': 'index_col'})
#user_df.to_csv('dim_users.csv', encoding='utf-8')
#show(user_df)

# Check missing values prior to cleaning:
print("\n Cleaning user data...")
cleaned_user_df = data_cleaner.clean_user_data(user_df)
print("Entry count of cleaned data: ", len(cleaned_user_df.index))
# Preview of cleaned data
print("\n Cleaned Data Preview:")
print(cleaned_user_df.head(10))

# Upload cleaned data to the database
print("\n Uploading cleaned user data to the database...")
db_connector.upload_to_db(cleaned_user_df, "dim_users")
print("Data uploaded successfully.")
