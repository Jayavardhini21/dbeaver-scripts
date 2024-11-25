use goodcabs;


Select * FROM city_target_passenger_rating;
SELECT  * FROM dim_city;
SELECT  * FROM dim_date;
SELECT *  FROM dim_repeat_trip_distribution;
SELECT * FROM  fact_passenger_summary;
SELECT * FROM fact_trips;
SELECT  * FROM monthly_target_new_passengers;
SELECT * FROM monthly_target_trips;



-- 1.find the top 3 and last 3 cities by total trips 
WITH CityTotalTrips AS (
    SELECT 
        dc.city_name, 
        SUM(mth_trip.total_target_trips) AS total_trips
    FROM 
        dim_city AS dc
    LEFT JOIN 
        monthly_target_trips AS mth_trip 
    ON 
        dc.city_id = mth_trip.city_id
    GROUP BY 
        dc.city_name
)
SELECT 
    city_name, 
    total_trips
FROM (
    SELECT 
        city_name, 
        total_trips, 
        ROW_NUMBER() OVER (ORDER BY total_trips DESC) AS rank_desc,
        ROW_NUMBER() OVER (ORDER BY total_trips ASC) AS rank_asc
    FROM 
        CityTotalTrips
) Ranked
WHERE 
    rank_desc <= 3 OR rank_asc <= 3
ORDER BY 
    rank_desc;
   
     
-- 2.Avg fare per trip by city 
   

with avgtotaltrip as (
	SELECT dc.city_name, 
		AVG(ft.fare_amount) as avg_amt , 
		AVG(ft.`distance_travelled(km)`) as avg_dist
	FROM 
		dim_city as dc
	JOIN 
		fact_trips as ft ON dc.city_id = ft.city_id
	GROUP BY 
		dc.city_name	
)
-- find the city with avg and low fare
SELECT 
    city_name, 
    avg_amt, 
    avg_dist,
    CASE 
        WHEN avg_amt = (SELECT MAX(avg_amt) FROM avgtotaltrip) THEN 'Highest Average Fare'
        WHEN avg_amt = (SELECT MIN(avg_amt) FROM avgtotaltrip) THEN 'Lowest Average Fare'
    END AS fare_category
FROM 
    avgtotaltrip
WHERE 
    avg_amt = (SELECT MAX(avg_amt) FROM avgtotaltrip)
    OR 
    avg_amt = (SELECT MIN(avg_amt) FROM avgtotaltrip);


-- 3. Average rating by city and passenger type 
with ratingcitybytype AS
(
	 SELECT 
	 	round(avg(fp.total_passengers)) as avg_passengers, ft.driver_rating, ci.city_name
	 FROM 
	 	fact_passenger_summary as fp
	 JOIN 
	 	fact_trips as ft ON fp.city_id = ft.city_id 		
	 JOIN 
	 	dim_city as ci ON  ci.city_id = fp.city_id
	 group by 
	 	ft.driver_rating, ci.city_name
	 order by  
		ft.driver_rating desc
 )
 SELECT 
 	city_name, avg_passengers, driver_rating,
 	case 
 		when avg_passengers = (SELECT max(avg_passengers) from ratingcitybytype)then 'Higest_rating'
 		when avg_passengers = (SELECT MIN(avg_passengers) from ratingcitybytype) then 'Lowest_rating'
 	end as Rating
 FROM 
 	ratingcitybytype
 WHERE 
    avg_passengers = (SELECT MAX(avg_passengers) FROM ratingcitybytype)
    OR 
    avg_passengers = (SELECT MIN(avg_passengers) FROM ratingcitybytype)
group by  city_name, avg_passengers, driver_rating  ;	

-- 4.peak and low demand month by city 

WITH trip_summary AS (
    SELECT
        c.city_name,
        d.month_name,
        COUNT(t.trip_id) AS total_trips
    FROM
        fact_trips t
    JOIN
        dim_date d ON t.date = d.date
    JOIN
        dim_city c ON t.city_id = c.city_id
    GROUP BY
        c.city_name, d.month_name
),
city_monthly_summary AS (
    SELECT
        city_name,
        month_name,
        total_trips,
        ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY total_trips DESC) AS rank_high,
        ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY total_trips ASC) AS rank_low
    FROM
        trip_summary
)
SELECT
    city_name,
    MAX(CASE WHEN rank_high = 1 THEN month_name END) AS highest_trip_month,
    MAX(CASE WHEN rank_low = 1 THEN month_name END) AS lowest_trip_month
FROM
    city_monthly_summary
GROUP BY
    city_name;


-- 5. weekend vs weekdays trip demand by city 
   
SELECT 
	dd.date,
	dd.month_name,
	mt.city_id, 
	SUM( mt.total_target_trips) as total_trips , 
	dd.day_type,
	dc.city_name,
	STR_TO_DATE(`date`, '%d-%m-%Y') <= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) as six_months
FROM 
	dim_date as dd
JOIN  monthly_target_trips mt 
	on dd.start_of_month = mt.month
Join dim_city as dc
	on dc.city_id = mt.city_id 
group by  dd.date,dd.month_name, mt.city_id , dd.day_type, dc.city_name;

   
SELECT 
    dc.city_name,
    dd.day_type,
    SUM(mt.total_target_trips) AS total_trips
FROM 
    dim_date AS dd
JOIN 
    monthly_target_trips AS mt 
    ON dd.start_of_month = mt.month
JOIN 
    dim_city AS dc 
    ON dc.city_id = mt.city_id
WHERE 
    STR_TO_DATE(dd.date, '%d-%m-%Y') >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
GROUP BY 
    dc.city_name, dd.day_type
ORDER BY 
    dc.city_name, dd.day_type;



-- 6.Repeated Passenger Frequency 

WITH RepeatTripCounts AS (
    SELECT
        c.city_name,
        r.trip_count,
        SUM(r.repeat_passenger_count) AS repeat_passenger_count
    FROM
        dim_repeat_trip_distribution r
    JOIN
        dim_city c ON r.city_id = c.city_id
    GROUP BY
        c.city_name, r.trip_count
),
CityTotalRepeatPassengers AS (
    SELECT
        city_name,
        SUM(repeat_passenger_count) AS total_repeat_passengers
    FROM
        RepeatTripCounts
    GROUP BY
        city_name
)
SELECT
    r.city_name,
    r.trip_count,
    r.repeat_passenger_count,
    ROUND((r.repeat_passenger_count / t.total_repeat_passengers) * 100, 2) AS percentage_of_passengers
FROM
    RepeatTripCounts r
JOIN
    CityTotalRepeatPassengers t ON r.city_name = t.city_name
ORDER BY
    r.city_name, r.trip_count;


 
--  7. Monthly target archivement 
   
   
WITH RepeatPassengerFrequency AS (
    -- Calculate the number of repeat passengers for each trip frequency in each city
    SELECT
        dc.city_name,
        rp.trip_count,
        SUM(rp.repeat_passenger_count) AS repeat_passenger_count
    FROM
        dim_repeat_trip_distribution rp
    JOIN
        dim_city dc ON rp.city_id = dc.city_id
    GROUP BY
        dc.city_name, rp.trip_count
),
CityTotalRepeatPassengers AS (
    -- Calculate the total repeat passengers for each city
    SELECT
        city_name,
        SUM(repeat_passenger_count) AS total_repeat_passengers
    FROM
        RepeatPassengerFrequency
    GROUP BY
        city_name
),
FrequencyDistribution AS (
    -- Calculate the percentage of repeat passengers for each trip count in each city
    SELECT
        rpf.city_name,
        rpf.trip_count,
        rpf.repeat_passenger_count,
        ctp.total_repeat_passengers,
        ROUND((rpf.repeat_passenger_count / ctp.total_repeat_passengers) * 100, 2) AS percentage_distribution
    FROM
        RepeatPassengerFrequency rpf
    JOIN
        CityTotalRepeatPassengers ctp ON rpf.city_name = ctp.city_name
)
-- Pivot the data to display percentages for each trip frequency in separate columns
SELECT
    city_name,
    MAX(CASE WHEN trip_count = '2-Trips' THEN percentage_distribution ELSE 0 END) AS "2-Trips (%)",
    MAX(CASE WHEN trip_count = '3-Trips' THEN percentage_distribution ELSE 0 END) AS "3-Trips (%)",
    MAX(CASE WHEN trip_count = '4-Trips' THEN percentage_distribution ELSE 0 END) AS "4-Trips (%)",
    MAX(CASE WHEN trip_count = '5-Trips' THEN percentage_distribution ELSE 0 END) AS "5-Trips (%)",
    MAX(CASE WHEN trip_count = '6-Trips' THEN percentage_distribution ELSE 0 END) AS "6-Trips (%)",
    MAX(CASE WHEN trip_count = '7-Trips' THEN percentage_distribution ELSE 0 END) AS "7-Trips (%)",
    MAX(CASE WHEN trip_count = '8-Trips' THEN percentage_distribution ELSE 0 END) AS "8-Trips (%)",
    MAX(CASE WHEN trip_count = '9-Trips' THEN percentage_distribution ELSE 0 END) AS "9-Trips (%)",
    MAX(CASE WHEN trip_count = '10-Trips' THEN percentage_distribution ELSE 0 END) AS "10-Trips (%)"
FROM
    FrequencyDistribution
GROUP BY
    city_name
ORDER BY
    city_name;   

-- 8.(a) Highest and Lowest Repeat Passenger Rate (RPR%) by city 
  
with rpr_percentage as(
   SELECT
        dc.city_name,
        fps.month,
        SUM(fps.repeat_passengers) AS total_repeat_passengers,
        SUM(fps.total_passengers) AS total_passengers,
        ROUND((SUM(fps.repeat_passengers) / SUM(fps.total_passengers)) * 100, 2) AS RPR_percentage
    FROM
        fact_passenger_summary fps
    JOIN
        dim_city dc ON fps.city_id = dc.city_id
    WHERE
    STR_TO_DATE(fps.month, '%d-%m-%Y') >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)    
    GROUP BY
        dc.city_name, fps.month
)
SELECT 
    city_name, 
    RPR_percentage
FROM (
    SELECT 
        city_name, 
        RPR_percentage, 
        ROW_NUMBER() OVER (ORDER BY RPR_percentage DESC) AS rank_desc,
        ROW_NUMBER() OVER (ORDER BY RPR_percentage ASC) AS rank_asc
    FROM 
        rpr_percentage
) Ranked
WHERE 
    rank_desc <= 3 OR rank_asc <= 3
ORDER BY 
    rank_desc;
        
        
        



/*WITH CityRPR AS (
    SELECT
        dc.city_name,
        fps.month,
        SUM(fps.repeat_passengers) AS total_repeat_passengers,
        SUM(fps.total_passengers) AS total_passengers,
        ROUND((SUM(fps.repeat_passengers) / SUM(fps.total_passengers)) * 100, 2) AS RPR_percentage
    FROM
        fact_passenger_summary fps
    JOIN
        dim_city dc ON fps.city_id = dc.city_id
    GROUP BY
        dc.city_name, fps.month
),
CityAverageRPR AS (
    SELECT
        city_name,
        ROUND(AVG(RPR_percentage), 2) AS avg_RPR_percentage
    FROM
        CityRPR
    GROUP BY
        city_name
),
RankedRPR AS (
    SELECT
        city_name,
        avg_RPR_percentage,
        ROW_NUMBER() OVER (ORDER BY avg_RPR_percentage DESC) AS rank_high,
        ROW_NUMBER() OVER (ORDER BY avg_RPR_percentage ASC) AS rank_low
    FROM
        CityAverageRPR
)
SELECT
    city_name,
    avg_RPR_percentage,
    CASE
        WHEN rank_high <= 2 THEN 'Top'
        WHEN rank_low <= 2 THEN 'Bottom'
        ELSE NULL
    END AS RPR_category
FROM
    RankedRPR
WHERE
    rank_high <= 2 OR rank_low <= 2
ORDER BY
    avg_RPR_percentage DESC;*/
   
   
   


-- 8.(b) Highest and Lowest Repeat Passenger Rate (RPR%) by month
   
   
WITH CityMonthlyRPR AS (
    SELECT
        dc.city_name,
        MONTH(STR_TO_DATE(fps.month, '%d-%m-%Y')) AS extracted_month,
        SUM(fps.repeat_passengers) AS total_repeat_passengers,
        SUM(fps.total_passengers) AS total_passengers,
        ROUND((SUM(fps.repeat_passengers) / SUM(fps.total_passengers)) * 100, 2) AS RPR_percentage
    FROM
        fact_passenger_summary fps
    JOIN
        dim_city dc ON fps.city_id = dc.city_id
    GROUP BY
        dc.city_name, MONTH(STR_TO_DATE(fps.month, '%d-%m-%Y'))
),
RankedCityMonthlyRPR AS (
    SELECT
        city_name,
        extracted_month,
        RPR_percentage,
        ROW_NUMBER() OVER (ORDER BY RPR_percentage DESC) AS rank_high,
        ROW_NUMBER() OVER (ORDER BY RPR_percentage ASC) AS rank_low
    FROM
        CityMonthlyRPR
)
SELECT
    city_name,
    extracted_month,
    RPR_percentage,
    CASE
        WHEN rank_high = 1 THEN 'Highest'
        WHEN rank_low = 1 THEN 'Lowest'
    END AS RPR_category
FROM
    RankedCityMonthlyRPR
WHERE
    rank_high = 1 OR rank_low = 1
ORDER BY
    RPR_category DESC; 

   
   
--   Business Request - 1: City-Level Fare and Trip Summary Report

   
WITH CityTrips AS (
    SELECT 
        dc.city_name,
        COUNT(ft.trip_id) AS total_trips,
        AVG(ft.fare_amount) AS avg_fare_per_trip,
        SUM(ft.fare_amount) / SUM(ft.`distance_travelled(km)`) AS avg_fare_per_km
    FROM 
        fact_trips ft
    JOIN 
        dim_city dc ON ft.city_id = dc.city_id 
    GROUP BY 
        dc.city_name
),
TotalTrips AS (
    SELECT 
        SUM(total_trips) AS overall_total_trips
    FROM 
        CityTrips
)
SELECT 
    ct.city_name,
    ct.total_trips,
    ROUND(ct.avg_fare_per_trip, 2) AS avg_fare_per_trip,
    ROUND(ct.avg_fare_per_km, 2) AS avg_fare_per_km,
    ROUND((ct.total_trips / tt.overall_total_trips) * 100, 2) AS percentage_contribution_to_total_trips
FROM 
    CityTrips ct
CROSS JOIN 
    TotalTrips tt
ORDER BY 
    percentage_contribution_to_total_trips DESC;



-- Business Request - 2: Monthly City-Level Trips Target Performance Report
   
WITH ActualTrips AS ( 
    SELECT 
        dc.city_name,
        MONTH(STR_TO_DATE(ft.date, '%d-%m-%Y')) AS month_number,  -- Using month_number
        COUNT(ft.trip_id) AS actual_trips
    FROM 
        fact_trips ft
    JOIN 
        dim_city dc ON ft.city_id = dc.city_id
    GROUP BY 
        dc.city_name, month_number    
),
PerformanceReport AS (
    SELECT 
        at.city_name,
        mt.month AS target_month,
        at.actual_trips,
        mt.total_target_trips AS target_trips,
        CASE 
            WHEN at.actual_trips > mt.total_target_trips THEN 'Above Target'
            ELSE 'Below Target'
        END AS performance_status,
        ROUND(((at.actual_trips - mt.total_target_trips) / mt.total_target_trips) * 100, 2) AS percentage_difference
    FROM 
        ActualTrips at
    JOIN 
        monthly_target_trips mt 
        ON at.city_name = (SELECT city_name FROM dim_city WHERE city_id = mt.city_id) 
        AND MONTH(STR_TO_DATE(mt.month, '%d-%m-%Y')) = at.month_number  -- Match by month_number
)
SELECT 
    city_name,
    DATE_FORMAT(STR_TO_DATE(target_month, '%d-%m-%Y'), '%M %Y') AS month_name,  -- Formatting the month_name
    actual_trips,
    target_trips,
    performance_status,
    percentage_difference
FROM 
    PerformanceReport
ORDER BY 
    city_name, month_name;
   
   
--  Business Request - 3: City-Level Repeat Passenger Trip Frequency Report  
   
WITH RepeatPassengerSummary AS (
    SELECT
        dc.city_name,
        rp.trip_count,
        SUM(rp.repeat_passenger_count) AS total_repeat_passenger_count, -- Total repeat passengers for each trip count
        SUM(fps.repeat_passengers) AS city_total_repeat_passengers       -- Total repeat passengers in the city
    FROM
        dim_repeat_trip_distribution rp
    JOIN
        dim_city dc ON rp.city_id = dc.city_id
    JOIN
        fact_passenger_summary fps ON fps.city_id = dc.city_id
    GROUP BY
        dc.city_name, rp.trip_count
),
PercentageDistribution AS (
    SELECT
        city_name,
        trip_count,
        ROUND((total_repeat_passenger_count * 100.0) / city_total_repeat_passengers, 2) AS percentage_distribution
    FROM
        RepeatPassengerSummary
)
SELECT
    city_name,
    MAX(CASE WHEN trip_count = '2-Trips' THEN percentage_distribution ELSE 0 END) AS "2-Trips",
    MAX(CASE WHEN trip_count = '3-Trips' THEN percentage_distribution ELSE 0 END) AS "3-Trips",
    MAX(CASE WHEN trip_count = '4-Trips' THEN percentage_distribution ELSE 0 END) AS "4-Trips",
    MAX(CASE WHEN trip_count = '5-Trips' THEN percentage_distribution ELSE 0 END) AS "5-Trips",
    MAX(CASE WHEN trip_count = '6-Trips' THEN percentage_distribution ELSE 0 END) AS "6-Trips",
    MAX(CASE WHEN trip_count = '7-Trips' THEN percentage_distribution ELSE 0 END) AS "7-Trips",
    MAX(CASE WHEN trip_count = '8-Trips' THEN percentage_distribution ELSE 0 END) AS "8-Trips",
    MAX(CASE WHEN trip_count = '9-Trips' THEN percentage_distribution ELSE 0 END) AS "9-Trips",
    MAX(CASE WHEN trip_count = '10-Trips' THEN percentage_distribution ELSE 0 END) AS "10-Trips"
FROM
    PercentageDistribution
GROUP BY
    city_name
ORDER BY
    city_name;
   
   
--   Business Request - 4: Identify Cities with Highest and Lowest Total New Passengers 
  
WITH TotalNewPassengers AS (
    SELECT  
        dc.city_name, 
        SUM(fps.new_passengers) AS total_new_passengers
    FROM 
        fact_passenger_summary fps
    LEFT JOIN 
        dim_city dc ON fps.city_id = dc.city_id
    GROUP BY 
        dc.city_name
),
RankedCities AS (
    SELECT 
        city_name, 
        total_new_passengers, 
        ROW_NUMBER() OVER (ORDER BY total_new_passengers DESC) AS rank_desc,
        ROW_NUMBER() OVER (ORDER BY total_new_passengers ASC) AS rank_asc
    FROM 
        TotalNewPassengers
),
FilteredCities AS (
    SELECT 
        city_name, 
        total_new_passengers,
        CASE 
            WHEN rank_desc <= 3 THEN 'Top 3'
            WHEN rank_asc <= 3 THEN 'Bottom 3'
        END AS city_category
    FROM 
        RankedCities
    WHERE 
        rank_desc <= 3 OR rank_asc <= 3
)
SELECT 
    city_name, 
    total_new_passengers,
    city_category
FROM 
    FilteredCities
ORDER BY 
    city_category, total_new_passengers DESC;
   
   
   
--    
   
WITH CityMonthlyRevenue AS (
    -- Calculate the monthly revenue for each city
    SELECT
        dc.city_name,
        fps.month AS month,
        SUM(ft.fare_amount) AS monthly_revenue
    FROM
        fact_trips ft
    JOIN
        fact_passenger_summary fps ON ft.city_id = fps.city_id
    JOIN
        dim_city dc ON ft.city_id = dc.city_id
    GROUP BY
        dc.city_name, fps.month
),
CityTotalRevenue AS (
    -- Calculate the total revenue for each city
    SELECT
        city_name,
        SUM(monthly_revenue) AS total_revenue
    FROM
        CityMonthlyRevenue
    GROUP BY
        city_name
),
CityMaxRevenue AS (
    -- Find the highest revenue month for each city
    SELECT
        city_name,
        month AS highest_revenue_month,
        monthly_revenue AS highest_revenue
    FROM
        CityMonthlyRevenue cmr
    WHERE
        monthly_revenue = (
            SELECT MAX(monthly_revenue)
            FROM CityMonthlyRevenue
            WHERE city_name = cmr.city_name
        )
)
-- Combine the results to include the percentage contribution
SELECT
    cmr.city_name,
    cmr.highest_revenue_month,
    cmr.highest_revenue AS revenue,
    ROUND((cmr.highest_revenue / ctr.total_revenue) * 100, 2) AS percentage_contribution
FROM
    CityMaxRevenue cmr
JOIN
    CityTotalRevenue ctr ON cmr.city_name = ctr.city_name
ORDER BY
    cmr.city_name;


   
--  Business Request - 6: Repeat Passenger Rate Analysis  
   
 WITH MonthlyRepeatRate AS (
    -- Calculate monthly repeat passenger rate for each city and month
    SELECT 
        dc.city_name,
        fps.month,
        SUM(fps.total_passengers) AS total_passengers,
        SUM(fps.repeat_passengers) AS repeat_passengers,
        ROUND((SUM(fps.repeat_passengers) / SUM(fps.total_passengers)) * 100, 2) AS monthly_repeat_passenger_rate
    FROM 
        fact_passenger_summary fps
    JOIN 
        dim_city dc ON fps.city_id = dc.city_id
    GROUP BY 
        dc.city_name, fps.month
),
CityWideRepeatRate AS (
    -- Calculate city-wide repeat passenger rate aggregated across months
    SELECT 
        dc.city_name,
        SUM(fps.total_passengers) AS total_passengers_citywide,
        SUM(fps.repeat_passengers) AS repeat_passengers_citywide,
        ROUND((SUM(fps.repeat_passengers) / SUM(fps.total_passengers)) * 100, 2) AS city_repeat_passenger_rate
    FROM 
        fact_passenger_summary fps
    JOIN 
        dim_city dc ON fps.city_id = dc.city_id
    GROUP BY 
        dc.city_name
)
-- Combine the results to display monthly and city-wide repeat passenger rates
SELECT 
    mrr.city_name,
    mrr.month,
    mrr.total_passengers,
    mrr.repeat_passengers,
    mrr.monthly_repeat_passenger_rate,
    crr.city_repeat_passenger_rate
FROM 
    MonthlyRepeatRate mrr
JOIN 
    CityWideRepeatRate crr ON mrr.city_name = crr.city_name
ORDER BY 
    mrr.city_name, mrr.month;
  

--    
 SELECT 
    dc.city_name,
    SUM(fps.repeat_passengers) / SUM(fps.total_passengers) * 100 AS repeat_passenger_rate
FROM 
    fact_passenger_summary fps
JOIN 
    dim_city dc ON fps.city_id = dc.city_id
GROUP BY 
    dc.city_name
ORDER BY 
    repeat_passenger_rate DESC;
 
   

SELECT 
    dc.city_name,
    SUM(CASE WHEN WEEKDAY(STR_TO_DATE(ft.date, '%d-%m-%Y')) IN (5, 6) THEN 1 ELSE 0 END) AS weekend_trips,
    SUM(CASE WHEN WEEKDAY(STR_TO_DATE(ft.date, '%d-%m-%Y')) BETWEEN 0 AND 4 THEN 1 ELSE 0 END) AS weekday_trips
FROM 
    fact_trips ft
JOIN 
    dim_city dc ON ft.city_id = dc.city_id
GROUP BY 
    dc.city_name;

SELECT 
    dc.city_name,
    MONTH(STR_TO_DATE(ft.date, '%d-%m-%Y')) AS month,
    SUM(ft.fare_amount) AS monthly_revenue
FROM 
    fact_trips ft
JOIN 
    dim_city dc ON ft.city_id = dc.city_id
GROUP BY 
    dc.city_name, month
ORDER BY 
    city_name, month;
   
   
--  Find the city specific by analysing using weekend vs weekdays
   
SELECT 
    dc.city_name,
    SUM(CASE WHEN WEEKDAY(STR_TO_DATE(ft.date, '%d-%m-%Y')) IN (5, 6) THEN 1 ELSE 0 END) AS weekend_trips,
    SUM(CASE WHEN WEEKDAY(STR_TO_DATE(ft.date, '%d-%m-%Y')) BETWEEN 0 AND 4 THEN 1 ELSE 0 END) AS weekday_trips,
    CASE 
        WHEN SUM(CASE WHEN WEEKDAY(STR_TO_DATE(ft.date, '%d-%m-%Y')) IN (5, 6) THEN 1 ELSE 0 END) > 
             SUM(CASE WHEN WEEKDAY(STR_TO_DATE(ft.date, '%d-%m-%Y')) BETWEEN 0 AND 4 THEN 1 ELSE 0 END) 
        THEN 'Tourism'
        ELSE 'Business'
    END AS city_specific
FROM 
    fact_trips ft
JOIN 
    dim_city dc ON ft.city_id = dc.city_id
GROUP BY 
    dc.city_name;

SELECT * 
from dim_city   

  