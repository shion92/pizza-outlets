-- Set a new table 
CREATE TABLE transactions (
  shop_id INT,
  date date,
  n_trans INT
);

-- Load csv file 
LOAD DATA LOCAL INFILE '/Users/shion/DE test/sample.csv' INTO TABLE transactions 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES; -- skip the header in the file 


-- Calculate lower and upper date ranges, gaps, and status based on the gap
WITH t_transactions AS (
    SELECT
        shop_id,
        date AS lower_range,
        LEAD(date, 1) OVER (PARTITION BY shop_id ORDER BY date ASC) AS upper_range, 
        DATEDIFF(LEAD(date, 1) OVER (PARTITION BY shop_id ORDER BY date ASC), date) AS gap,
        CASE
            WHEN DATEDIFF(LEAD(date, 1) OVER (PARTITION BY shop_id ORDER BY date ASC), date)> 30 THEN 'clsd'
            ELSE 'open'
        END AS status
        
    FROM transactions
), 
-- Aggregate transactions to get min(lower_range) and max(upper_range) for each shop and status
min_max_na AS(
	SELECT
		shop_id,
		status,
		MIN(lower_range) AS lower_range,
		MAX(upper_range) AS upper_range
	FROM t_transactions
	GROUP BY 1, 2
	-- Include separate record with NULL upper_range if applicable

	UNION ALL (
		SELECT 
			shop_id,
			status,
			lower_range,
			upper_range
		FROM t_transactions
		WHERE upper_range IS NULL 
	)
),

-- Add a previous/next status column
t_status AS(
SELECT 
	shop_id,
    status,
    LAG(status, 1) OVER (PARTITION BY shop_id ORDER BY lower_range ASC) AS prev_status,
    LEAD(status, 1) OVER (PARTITION BY shop_id ORDER BY lower_range ASC) AS next_status,
    lower_range,
    upper_range
FROM min_max_na
),

-- Recalculate the corrected lower_range and upper_range based on status transitions
out_table AS(
SELECT 
	shop_id,
    CASE 
		WHEN lower_range < '2022-12-31' and upper_range IS NULL and prev_status = 'open' THEN 'clsd'
        ELSE status
	END AS status,
    prev_status,
    next_status,
    CASE 
		WHEN status = 'open' AND prev_status = 'clsd' THEN LAG(upper_range, 1) OVER (PARTITION BY shop_id ORDER BY lower_range ASC)
		WHEN status = 'clsd' OR (status = 'open' AND prev_status = 'open' AND lower_range < '2022-12-31') THEN DATE_ADD(lower_range, INTERVAL 1 DAY)
		ELSE lower_range
    END AS lower_range,
    CASE 
		WHEN status = 'open' AND next_status = 'clsd' THEN LEAD(lower_range, 1) OVER (PARTITION BY shop_id ORDER BY lower_range ASC)
        -- Treat `upper_range` as open when the shop has transactions up to 2022-12-31. No data beyond 2022-12-31.
		-- This is based on the same logic as shown in the example.
		WHEN status = 'open' AND next_status = 'open' AND upper_range = '2022-12-31' THEN NULL 
        WHEN status = 'clsd' THEN DATE_SUB(upper_range, INTERVAL 1 DAY)
		ELSE upper_range
    END AS upper_range
FROM 
	t_status
)
-- Output the dataset
SELECT 
	shop_id,
    status,
    lower_range,
    upper_range
FROM out_table
WHERE 
	status = 'clsd' OR (prev_status <> 'open' AND status = 'open' AND lower_range < '2022-12-31') OR (prev_status IS NULL AND status = 'open') ;