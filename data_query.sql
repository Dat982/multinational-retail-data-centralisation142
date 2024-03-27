-- 1. How many stores does the business have and in which countries?
SELECT country_code, count(*) AS store_number
FROM dim_store_details
GROUP BY country_code
ORDER BY country_code;

--2. Which locations currently have the most stores?
SELECT locality, count(*) AS store_number
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 10

--3. Which months produce the highest average cost of sales?
SELECT
	ROUND(CAST(SUM(spend) AS numeric), 2) AS total_sales,
	month
FROM (
	SELECT 
		ord.product_quantity * prod.product_price AS spend,
		dt.month
	FROM orders_table ord
	INNER JOIN dim_date_times dt
		ON ord.date_uuid = dt.date_uuid
	INNER JOIN dim_products prod
		ON ord.product_code = prod.product_code
) x
GROUP BY month
ORDER BY total_sales DESC;

--4. How many sales are coming from online?
SELECT
	COUNT(*) AS numbers_of_sales,
	SUM(product_quantity) AS product_quantity_count,
	location
FROM (
	SELECT 
		ord.product_quantity,
		CASE
			WHEN st.store_type = 'Web Portal' THEN 'Web'
			ELSE 'Offline'
		END AS location
	FROM orders_table ord
	INNER JOIN dim_store_details st
		ON ord.store_code = st.store_code
) x
GROUP BY location
ORDER BY location DESC;

--5. What percentage of sales come through each type of store?
SELECT
	store_type,
	ROUND(CAST(SUM(sale) AS NUMERIC), 2) AS total_sales,
	ROUND(CAST(100 * SUM(sale) / total AS NUMERIC), 2) AS "percentage_total(%)"
FROM (
	SELECT 
		st.store_type,
		ord.product_quantity * prod.product_price AS sale
	FROM orders_table ord
	INNER JOIN dim_store_details st
		ON ord.store_code = st.store_code
	INNER JOIN dim_products prod
		ON ord.product_code = prod.product_code
) x
CROSS JOIN (
	SELECT SUM(ord.product_quantity * prod.product_price) AS total
	FROM orders_table ord
	INNER JOIN dim_products prod
	ON ord.product_code = prod.product_code
) y
GROUP BY x.store_type, y.total
ORDER BY total_sales DESC;

--6. Which months produced the highest sales?
SELECT
	ROUND(CAST(SUM(sale) AS NUMERIC), 2) AS total_sales,
	year,
	month
FROM (
	SELECT 
		dt.year,
		dt.month,
		ord.product_quantity * prod.product_price AS sale
	FROM orders_table ord
	INNER JOIN dim_products prod
		ON ord.product_code = prod.product_code
	INNER JOIN dim_date_times dt
		ON ord.date_uuid = dt.date_uuid
) x
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;

--7. What is our staff headcount?
SELECT 
	SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

--8. Which German store type is selling the most?
SELECT 
	ROUND(CAST(SUM(sale) AS NUMERIC), 2) AS total_sales,
	store_type,
	country_code
FROM (
SELECT
	st.store_type,
	st.country_code,
	ord.product_quantity * prod.product_price AS sale
FROM orders_table ord
INNER JOIN (
	SELECT store_code, store_type, country_code
	FROM dim_store_details 
	WHERE country_code = 'DE'
	) st
	ON ord.store_code = st.store_code
INNER JOIN dim_products prod
	ON ord.product_code = prod.product_code
) x
GROUP BY 
	store_type,
	country_code
ORDER BY 
	total_sales;

--9. How quickly is the company making sales?
SELECT
	year,
	--avg_datediff,
	'"hours": ' || TO_CHAR(avg_datediff, 'HH24') || 
	', "minutes": ' || TO_CHAR(avg_datediff, 'MI') ||
	', "seconds": ' || TO_CHAR(avg_datediff, 'SS') ||
	', "milliseconds": ' || TO_CHAR(avg_datediff, 'MS')
	AS actual_time_taken
FROM (
	SELECT
		year,
		AVG(datediff) AS avg_datediff
	FROM (
		SELECT
			year,
			dt,
			next_dt,
			next_dt - dt AS datediff
		FROM (
			SELECT 
				year,
				dt, 
				LEAD(dt, 1) OVER (ORDER BY dt) next_dt
			FROM (
				SELECT 
					year,
					CAST(year || '-' || month || '-' || day || ' ' || timestamp AS TIMESTAMP(1) WITHOUT TIME ZONE) dt
				FROM dim_date_times
			) concat_datetime
		) get_next_datetime
	) subtract_datetimes
	GROUP BY year
) get_average_interval
ORDER BY avg_datediff DESC
LIMIT 5;