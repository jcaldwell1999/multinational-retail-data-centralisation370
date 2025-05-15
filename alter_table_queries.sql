"""
Task 1:
orders_table atlerations
"""
-- Alter orders table
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- Check values in orders table
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'orders_table';

"""
Task 2:
dim_users alterations
"""

-- Alter dim_users table
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

-- Check values in dim_users table
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'dim_users';


"""
Task 3:
dim_store_details alterations
"""
-- Check values in dim_store_details table
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'dim_store_details';

-- View first 5 dim_store_details
SELECT * FROM dim_store_details LIMIT 5;

-- Drop the bad latitude column
ALTER TABLE dim_store_details
DROP COLUMN IF EXISTS lat;
-- Already removed when cleaning

-- Replace N/A with NULL
UPDATE dim_store_details
SET locality = NULL
WHERE locality = 'N/A';

-- Alter Data Types
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE NUMERIC USING latitude::NUMERIC,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

-- Checking J78 value
SELECT * FROM dim_store_details
WHERE staff_numbers = 'J78';
-- Updating values with numbers in them
UPDATE dim_store_details
SET staff_numbers = NULL
WHERE staff_numbers !~ '^\d+$';

-- Checking tables
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';


"""
Task 4:
Changes to dim_products table
"""
-- Preview of table
SELECT * FROM dim_products LIMIT 5;

-- Table data types
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'dim_products';

-- Strip '£' from product_price
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- Alter product price datatype 
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE NUMERIC USING product_price::NUMERIC;

-- Add weight_class column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

-- Fill weight_class based on weight
UPDATE dim_products
SET weight_class = CASE
	WHEN weight < 2 THEN 'Light'
	WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
	WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
	WHEN weight >= 140 THEN 'Truck_Required'
END;

-- Checking weight
SELECT *
FROM dim_products
LIMIT 20;

-- Fixing dim_products index name
ALTER TABLE dim_products
RENAME COLUMN "Unnamed: 0" TO index;

"""
Task 5:
Update dim_products table
"""
-- Rename 'removed' to 'still_available'
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Table data types
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'dim_products';

-- Alter table
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE NUMERIC,
	ALTER COLUMN weight TYPE NUMERIC,
	ALTER COLUMN "EAN" TYPE VARCHAR(20),
	ALTER COLUMN product_code TYPE VARCHAR(15),
	ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
	ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
	ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
	ALTER COLUMN weight_class TYPE VARCHAR(20);

-- Check max lengths
SELECT MAX(CHAR_LENGTH("EAN")) AS ean_max, MAX(CHAR_LENGTH(product_code)) AS product_code_max, MAX(CHAR_LENGTH(weight_class)) AS weight_class_max
FROM dim_products;

"""
Task 6:
Update dim_date_times table
"""
-- Check table datatypes
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'dim_date_times';

-- Alter table
ALTER TABLE dim_date_times
	ALTER COLUMN "month" TYPE VARCHAR(2),
	ALTER COLUMN "year" TYPE VARCHAR(4),
	ALTER COLUMN "day" TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Check max lengths
SELECT MAX(CHAR_LENGTH("month")) AS month_max, MAX(CHAR_LENGTH("year")) AS year_max, MAX(CHAR_LENGTH("day")) AS day_max, MAX(CHAR_LENGTH(time_period)) AS time_period_max
FROM dim_date_times;

"""
Task 7:
Update dim_card_details table
"""

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN expiry_date TYPE VARCHAR(19),
	ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Check dim_card_details table datatypes
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'dim_card_details';

-- Check dim_card_details lengths
SELECT MAX(CHAR_LENGTH(card_number)) AS card_number_max, MAX(CHAR_LENGTH(expiry_date)) AS expiry_date_max
FROM dim_card_details;

SELECT * FROM dim_card_details LIMIT 20;
