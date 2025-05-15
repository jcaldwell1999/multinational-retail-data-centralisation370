"""
Task 8:
Setting Primary Keys
"""

-- Checking primary key for dim_date_times
SELECT COUNT(*) AS total_rows,
	COUNT(DISTINCT date_uuid) AS unique_values,
	COUNT(*) - COUNT(date_uuid) AS null_values
FROM dim_date_times;

-- user_uuid FROM dim_users
-- card_number FROM dim_card_details
-- store_code FROM dim_store_details
-- product_code FROM dim_products

-- Checking primary key for dim_card_details
SELECT COUNT(*) AS total_rows,
	COUNT(DISTINCT card_number) AS unique_values,
	COUNT(*) - COUNT(card_number) AS null_values
FROM dim_card_details;

-- Checking primary key for dim_store_details
SELECT COUNT(*) AS total_rows,
	COUNT(DISTINCT store_code) AS unique_values,
	COUNT(*) - COUNT(store_code) AS null_values
FROM dim_store_details;

-- Checking primary key for dim_products
SELECT COUNT(*) AS total_rows,
	COUNT(DISTINCT product_code) AS unique_values,
	COUNT(*) - COUNT(product_code) AS null_values
FROM dim_products;

-- dim_date_times
SELECT * FROM dim_date_times;

-- Checking primary key of dim_date_times
SELECT COUNT(*) AS total_rows,
	COUNT(DISTINCT date_uuid) AS unique_values,
	COUNT (*) - COUNT(date_uuid)
FROM dim_date_times;

-- Orders table - No PK needed, maps directly to other tables
SELECT * FROM orders_table;

-- Add primary keys to dimension tables:
ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
ALTER TABLE dim_products ADD PRIMARY KEY (product_code);

SELECT * FROM orders_table;

-- Setting orders_table foreign keys
ALTER TABLE orders_table	
	ADD CONSTRAINT fk_date_uuid
		FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_user_uuid
		FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
		
ALTER TABLE orders_table
	ADD CONSTRAINT fk_card_number
		FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
		
ALTER TABLE orders_table
	ADD CONSTRAINT fk_store_code
		FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

-- Removing from orders_table where store_code doesn't match dim_store_details
DELETE FROM orders_table
WHERE store_code NOT IN (
	SELECT store_code FROM dim_store_details
);

SELECT * FROM dim_store_details;
SELECT * FROM orders_table LIMIT 50000;
		
ALTER TABLE orders_table
	ADD CONSTRAINT fk_product_code
		FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

-- Removing from orders_table where product_code isn't in dim_products

DELETE FROM orders_table
WHERE product_code NOT IN (
	SELECT product_code FROM dim_products
);
