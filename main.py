from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaner
from pandasgui import show

# Initialize Classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor(db_connector)
data_cleaner = DataCleaner()

orders_df = data_extractor.read_rds_table("orders_table")
print("Max card_number length:", orders_df["card_number"].astype(str).str.len().max())
print("Max store_code length:", orders_df["store_code"].astype(str).str.len().max())
print("Max product_code length:", orders_df["product_code"].astype(str).str.len().max())


"""json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

# Milestone 2: Task 8: Retrieve and clean the date events data.
print("Extracting date times data...")
data_df = data_extractor.extract_json_data(json_url)

# Clean date times data
cleaned_data_df = data_cleaner.clean_date_data(data_df)
show(cleaned_data_df)

db_connector.upload_to_db(cleaned_data_df, "dim_date_times")"""

# Extraction, cleaning and uploading - Orders Table
"""print("Listing tables:...")
tables = db_connector.list_db_tables()
print(tables)
orders_df = data_extractor.read_rds_table("orders_table")
print(orders_df.head())
print("Cleaning orders table data...")
cleaned_orders_df = data_cleaner.clean_orders_data(orders_df)
print(f"Cleaned orders shape: {cleaned_orders_df.shape}")

db_connector.upload_to_db(cleaned_orders_df, "orders_table")"""

# Product Data extracting, cleaning and uploading
"""# S3 URL for product CSV
s3_url = "s3://data-handling-public/products.csv"

# Extract product data from S3
print("Extracting product data from S3...")
product_df = data_extractor.extract_from_s3(s3_url)
print("Product data extracted!")

# Preview extracted data
print(product_df.head())

# Cleaning data
print("Cleaning Product Data...")
cleaned_product_df = data_cleaner.clean_products_data(product_df)
show(cleaned_product_df)
print(f"Final shape: {cleaned_product_df.shape}")

# Upload to db
db_connector.upload_to_db(cleaned_product_df, "dim_products")"""

# Store details processing
"""# Initialize API Details
api_url_count = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
api_url_store = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

# Get number of stores
store_count = data_extractor.list_number_of_stores(api_url_count, headers)
print(f"Total number of stores: {store_count}")

# Extract all store data
store_df = data_extractor.retrieve_stores_data(api_url_store, headers, store_count)
print("Extracted store data preview: ")
print(store_df.head())

# Checking missing values before cleaning
print("\nChecking missing values before cleaning:")
print(store_df.isnull().sum())  # See how many NaNs exist

#if 'index' in store_df.columns:
#   store_df = store_df.rename(columns={'index': 'index_col'})

#show(store_df)

# Clean store data
cleaned_store_df = data_cleaner.clean_store_data(store_df)

print("\nCheckin missing values after cleaning: ")
print(cleaned_store_df.isnull().sum())

db_connector.upload_to_db(cleaned_store_df, "dim_store_details")
print("Store data uploaded successfully")
"""

"""if 'index' in cleaned_store_df.columns:
   cleaned_store_df = cleaned_store_df.rename(columns={'index': 'index_col'})

show(cleaned_store_df.iloc[:])"""


# Commented out card processing
"""# Card Data Processing
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
"""
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
