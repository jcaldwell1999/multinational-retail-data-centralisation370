from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaner
from pandasgui import show

# Initialize Classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor(db_connector)
data_cleaner = DataCleaner()

# Card Data Processing
pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

# Step 1: Extract Data from PDF
print("\nExtracting card details from PDF...")
card_df = data_extractor.retrieve_pdf_data(pdf_url)

# Checking before cleaning
print("\nChecking for duplicate rows in card data: ")
duplicates = card_df.duplicated().sum()
print(f"Duplicated rows found: {duplicates}")

print("\nDuplicate rows preview")
print(card_df[card_df.duplicated()].head(10))


# Step 2: Clean Extracted Data
print("\nCleaning Extracted card data...")
cleaned_card_df = data_cleaner.clean_card_data(card_df)

# Checking after cleaning
print("\nChecking for duplicate rows in card data: ")
duplicates = cleaned_card_df.duplicated().sum()
print(f"Duplicated rows found: {duplicates}")

print("\nDuplicate rows preview")
print(cleaned_card_df[cleaned_card_df.duplicated()].head(10))

show(cleaned_card_df)

# Step 3: Upload Cleaned Data to PostreSQL
print("\nUploading cleaned card data to database...")
db_connector.upload_to_db(cleaned_card_df, "dim_card_details")

print("Card data processing complete.")




# Commented out: User data processing
"""# Extract user data
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
print("Data uploaded successfully.")"""
