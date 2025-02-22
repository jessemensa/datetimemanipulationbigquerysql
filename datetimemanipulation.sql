
-- MANIPULATING DATETIME 
-- Bigquery SQL comes with a plethora of powerful date functions that can be used to enrich your data
-- In todays video we will be going through some of them
-- we will be using thid shipping and order dataset
-- it has transaction id, customer id, order date and time, shipping date and time, delivery date and return date for customer returns

-- INSTRUCTIONS 
-- TODO: KINDLY USE THE DATASET ADDED TO THIS SCRIPT 
-- UPLOAD IT TO BIGQUERY AND USE IT 



SELECT * FROM  `youtubescripts-425810.youtubebigquery.datedataset`; 



-- REMOVING TIMESTAMPS FROM DATE
-- so lets say we want to
-- first remove the time and only keep the dates, we can use DATE function
-- (ACTION) lets run this 
-- now can we see the time stamp removed
SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date,
FROM
  `youtubescripts-425810.youtubebigquery.datedataset`



-- now lets say there are some data entry issues for instance
-- shipping date before order date
-- we can use an if statement to flag them as 1 or 0
-- if shipping date is less than order date then flag it as 1 else 0
-- this then output fields with shipping date before order dates as seen here
-- ACTION (Now lets run this)
-- in the result you can see 0s and 1s 
-- The 1s are the one shipping date before order date 
-- ACTION(SHOW IT) 
SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date,

    -- Flag if shipping is before order
  IF(TIMESTAMP(shipping_datetime) < TIMESTAMP(order_datetime), 1, 0) AS flag_shipping_before_order,
FROM
  `youtubescripts-425810.youtubebigquery.datedataset`


-- Next is we want to calculate days between order date and shipping date
-- we can use the DATE_DIFF and DATE function and specify we want only Days
-- running this gets us the output
SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date,

  -- Calculate the difference in days
  DATE_DIFF(DATE(shipping_datetime), DATE(order_datetime), DAY) AS days_diff,
FROM
  `youtubescripts-425810.youtubebigquery.datedataset`



-- Now we want to enrich the data and be more specific to show the difference in days and hours
-- we use the TIMESTAMP_DIFF function
-- then use the CAST function, which converts it to STRING
-- then for Hours
-- we use same TIMESTAMP_DIFF function
-- use the MOD function, which converts it to 24 hours
-- Cast it as STRING
-- then we use CONCAT function to join them together
-- Now lets run it
-- as you can we have enriched our data into days and hours
SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date,

  -- enrich the data into days and hours 
  CONCAT(
    CAST(TIMESTAMP_DIFF(shipping_datetime, order_datetime, DAY) AS STRING), ' days ',
    CAST(MOD(TIMESTAMP_DIFF(shipping_datetime, order_datetime, HOUR), 24) AS STRING), ' hours'
  ) AS duration_shipping_order_days_hours,
FROM
  `youtubescripts-425810.youtubebigquery.datedataset`

-- lets take it further to days and minutes
-- lets add if statament
-- If shipping date is earlier than order date, a dash sign is added to the negative duration
-- The second used could have been day or minute but second provides more precise way to determine if the duration is negative
-- Now there are new functions we will talk about
-- TIMESTAMP_DIFF gets the difference in days
-- ABS ??
-- CAST function converts the result into a string
-- TIMESTAMP_DIFF gets the difference in days
-- MOD function ??
-- ABS ??
-- Cast function converts the result into a string
SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date,
  
  -- enrich the data into days and minutes 
    CONCAT(
  CAST(TIMESTAMP_DIFF(shipping_datetime, order_datetime, DAY) AS STRING), ' days ',
  CAST(MOD(TIMESTAMP_DIFF(shipping_datetime, order_datetime, MINUTE), 1440) AS STRING), ' minutes'
) AS duration_shipping_order_days_minutes,
FROM
  `youtubescripts-425810.youtubebigquery.datedataset`


-- lets extract Date, Month and YEAR into seperate fields
-- we can use EXTRACT function to do this
-- this extracts the hour into a field called shipping_hour
-- this extracts the day into a field called shipping_day
-- this extracts the year into a field called shipping year
SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date,


-- extract hours, day, year
EXTRACT(HOUR FROM shipping_datetime) AS shipping_hour, 
EXTRACT(DAY FROM shipping_datetime) AS shipping_day, 
EXTRACT(YEAR FROM shipping_datetime) AS shipping_year, 

FROM
  `youtubescripts-425810.youtubebigquery.datedataset`

-- Now lets say we want to go two months before the delivery date
-- ie Lets say delivery date is in January and we want to go two months before that which is November
-- There are two ways to do this
-- first is use DATE_SUB function
-- delivery date
-- INTERNAL function
-- 2 MONTH

-- or

-- DATE_ADD function
-- delivery_date field
-- INTERVAL
-- -2 MONTH

SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date,


-- two months behind 
  DATE_SUB(delivery_date, INTERVAL 2 MONTH) AS two_months_before, 
  DATE_ADD(delivery_date, INTERVAL -2 MONTH) AS two_months_before_one, 




FROM
  `youtubescripts-425810.youtubebigquery.datedataset`



-- Now lets say we want to go two weeks before
-- we use same function DATE_ADD or DATE_SUB
-- first is use DATE_SUB function
-- delivery date
-- INTERNAL function
-- 2 WEEK

-- or

-- DATE_ADD function
-- delivery_date field
-- INTERVAL
-- -2 WEEK

SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date, 


-- two weeks before 
  DATE_SUB(delivery_date, INTERVAL 2 WEEK) AS two_weeks_before, 
  DATE_ADD(delivery_date, INTERVAL -2 WEEK) AS two_weeks_before_one, 




FROM
  `youtubescripts-425810.youtubebigquery.datedataset`



-- Now lets say we want to go three days before delivery date
-- we use same function DATE_ADD or DATE_SUB
-- first is use DATE_SUB function
-- delivery date
-- INTERNAL function
-- 3 DAY

-- or

-- DATE_ADD function
-- delivery_date field
-- INTERVAL
-- -3 DAY

SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date, 

  -- 3 days before 
DATE_SUB(delivery_date, INTERVAL 3 DAY) AS three_days_before, 
DATE_ADD(delivery_date, INTERVAL -3 DAY) AS three_days_before_one, 




FROM
  `youtubescripts-425810.youtubebigquery.datedataset`



-- Now the last thing we are going to do is
-- Lets take the return date
-- There are blank values
-- lets enrich it by converting it into NO return date
-- we can use CASE statement
-- CASE
-- WHEN function is used to make the decision
-- WHEN return date is BLANK then flag it as No return date  
-- else we keep the original value in there which is the RETURN DATE
-- END AS is used to name the field 


SELECT 
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date, 

  -- Replace '(Blank)' with 'No return date'
  CASE
    WHEN return_date = '(blank)' THEN 'No return date'
    ELSE return_date
  END AS return_date_display




FROM
  `youtubescripts-425810.youtubebigquery.datedataset`


-- Now this is the final sql script 

SELECT
  transaction_id,
  customer_id,
  order_datetime,
  DATE(order_datetime) AS order_date, 
  shipping_datetime,
  DATE(shipping_datetime) AS shipping_date, 
  delivery_date,
  return_date,
  
  -- Flag if shipping is before order
  IF(TIMESTAMP(shipping_datetime) < TIMESTAMP(order_datetime), 1, 0) AS flag_shipping_before_order,

    -- Calculate the difference in days
  DATE_DIFF(DATE(shipping_datetime), DATE(order_datetime), DAY) AS days_diff,

  -- 2. 
  CONCAT(
    CAST(TIMESTAMP_DIFF(shipping_datetime, order_datetime, DAY) AS STRING), ' days ',
    CAST(MOD(TIMESTAMP_DIFF(shipping_datetime, order_datetime, HOUR), 24) AS STRING), ' hours'
  ) AS duration_shipping_order_days_hours,

  -- what is this doing ?
  CONCAT(
  IF(TIMESTAMP_DIFF(shipping_datetime, order_datetime, SECOND) < 0, '-', ''),
  CAST(ABS(TIMESTAMP_DIFF(shipping_datetime, order_datetime, DAY)) AS STRING), ' days ',
  CAST(ABS(MOD(TIMESTAMP_DIFF(shipping_datetime, order_datetime, MINUTE), 1440)) AS STRING), ' minutes'
) AS duration_shipping_order_if,

  
  CONCAT(
  CAST(TIMESTAMP_DIFF(shipping_datetime, order_datetime, DAY) AS STRING), ' days ',
  CAST(MOD(TIMESTAMP_DIFF(shipping_datetime, order_datetime, MINUTE), 1440) AS STRING), ' minutes'
) AS duration_shipping_order_days_minutes,

-- extract hours, day, year
EXTRACT(HOUR FROM shipping_datetime) AS shipping_hour, 
EXTRACT(DAY FROM shipping_datetime) AS shipping_day, 
EXTRACT(YEAR FROM shipping_datetime) AS shipping_year, 

AVG(DATE_DIFF(delivery_date, DATE(order_datetime), DAY)) AS avg_processing_days, 

-- Calculate days since delivery
DATE_DIFF(CURRENT_DATE(), DATE(delivery_date), DAY) AS days_since_delivery,

-- two months behind 
  DATE_SUB(delivery_date, INTERVAL 2 MONTH) AS two_months_before, 
  DATE_ADD(delivery_date, INTERVAL -2 MONTH) AS two_months_before_one, 

-- two weeks before 
  DATE_SUB(delivery_date, INTERVAL 2 WEEK) AS two_weeks_before, 
  DATE_ADD(delivery_date, INTERVAL -2 WEEK) AS two_weeks_before_one, 

-- 3 days before 
DATE_SUB(delivery_date, INTERVAL 3 DAY) AS three_days_before, 
DATE_ADD(delivery_date, INTERVAL -3 DAY) AS three_days_before_one, 

  -- Replace '(Blank)' with 'No return date'
  CASE
    WHEN return_date = '(blank)' THEN 'No return date'
    ELSE return_date
  END AS return_date_display

  
FROM
  `youtubescripts-425810.youtubebigquery.datedataset`
  
  group by 
  
  transaction_id,
  customer_id,
  order_datetime,
  shipping_datetime,
  delivery_date,
  return_date;
