--1. Change order table columns into the correct data types

-- Find the longest card number length
SELECT MAX(LENGTH(card_number::TEXT)) FROM order_table
SET LIMIT 1; --19

-- Find the longest store code length
SELECT MAX(LENGTH(store_code::TEXT)) FROM order_table
SET LIMIT 1; --12

-- Find the longest product code length
SELECT MAX(LENGTH(product_code::TEXT)) FROM order_table
SET LIMIT 1; --11

-- Alter column data types
ALTER TABLE order_table
ALTER COLUMN date_uuid TYPE UUID
USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;


--2. Change dim user table columns into the correct data types

-- Find the longest country code length
SELECT MAX(LENGTH(country_code::text)) FROM dim_user
SET LIMIT 1; --2

-- Alter column data types
ALTER TABLE dim_user
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE
USING date_of_birth::DATE,
ALTER COLUMN country_code TYPE VARCHAR(3),
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE;

ALTER TABLE dim_user
ALTER COLUMN user_uuid TYPE TEXT;

--3: Merge Lat columns and change dim store details table columns into the correct data types

-- Merge and drop unwanted column
UPDATE dim_store_details
SET latitude = COALESCE(latitude || lat, latitude);

ALTER TABLE dim_store_details
DROP COLUMN lat;

-- Find the longest store code length
SELECT MAX(LENGTH(store_code::TEXT)) FROM dim_store_details
SET LIMIT 1; --12

-- Find the longest country code length
SELECT MAX(LENGTH(country_code::TEXT)) FROM dim_store_details
SET LIMIT 1; --2

-- Used to find columns with N/A VALUES
SELECT * FROM dim_store_details
WHERE address = 'N/A';

-- Updates N/A values into NULL
UPDATE dim_store_details
SET latitude = NULL
WHERE latitude = 'N/A';

UPDATE dim_store_details
SET longitude = NULL
WHERE longitude = 'N/A';

UPDATE dim_store_details
SET address = NULL
WHERE address = 'N/A';

UPDATE dim_store_details
SET locality = NULL
WHERE locality = 'N/A';

-- Alter column data types
ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255),
ALTER COLUMN longitude TYPE FLOAT
USING longitude::FLOAT,
ALTER COLUMN latitude TYPE FLOAT
USING latitude::FLOAT;


--4. Remove £ sign and add a new column

-- Removing £ sign in product_price
UPDATE dim_product
SET product_price = REPLACE(product_price, '£', '');

-- Adding a weight_class column
ALTER TABLE dim_product
ADD weight_class VARCHAR(14);

-- Categorized weight class based on weight
UPDATE dim_product
SET weight_class = CASE
WHEN weight < 2 then 'Light'
WHEN weight >= 2 AND weight < 40 then 'Mid_Sized'
WHEN weight >= 40 AND weight < 140 then 'Heavy'
WHEN weight >= 140 then 'Truck_Required'
ELSE NULL
END;


--5. Change dim product table columns into the correct data types

-- Rename removed column into still_available
ALTER TABLE dim_product
RENAME COLUMN removed TO still_available;

-- Find the longest product code length
SELECT MAX(LENGTH(product_code)) FROM dim_product
SET LIMIT 1; --11

-- Find the longest EAN length
SELECT MAX(LENGTH("EAN")) FROM dim_product
SET LIMIT 1; --17

-- Alter column data types
ALTER TABLE dim_product
ALTER COLUMN product_price TYPE FLOAT
USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID
USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOLEAN
USING CASE still_available
WHEN 'Still_avaliable' THEN TRUE 
WHEN 'Removed' THEN FALSE
ELSE NULL
END;


--6. Change dim date time table columns into the correct data types

-- Find the longest month length
SELECT MAX(LENGTH(month::TEXT)) FROM dim_date_times
SET LIMIT 1; --2

-- Find the longest year length
SELECT MAX(LENGTH(year::TEXT)) FROM dim_date_times
SET LIMIT 1; --4

-- Find the longest day length
SELECT MAX(LENGTH(day::TEXT)) FROM dim_date_times
SET LIMIT 1; --2

-- Find the longest time_period length
SELECT MAX(LENGTH(time_period::TEXT)) FROM dim_date_times
SET LIMIT 1; --10

-- Alter column data types
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID
USING date_uuid::uuid;


--7. Change dim card details table column into the correct data types

-- Find the longest card_number length
SELECT MAX(LENGTH(card_number)) FROM dim_card_details
SET LIMIT 1; --19

-- Find the longest expiry_date length
SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details
SET LIMIT 1; --5

-- Alter column data types
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE;


--8. Create primary key in dim tables
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_user
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_product
ADD PRIMARY KEY (product_code);


--9. Creating foreign key
ALTER TABLE order_table
ADD FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

ALTER TABLE order_table
ADD FOREIGN KEY (user_uuid)
REFERENCES dim_user (user_uuid); 

ALTER TABLE order_table
ADD FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

ALTER TABLE order_table
ADD FOREIGN KEY (product_code)
REFERENCES dim_product (product_code);

ALTER TABLE order_table
ADD FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);
