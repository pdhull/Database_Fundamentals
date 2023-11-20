-- Assignment 1: SQL Basics
-- Submitted by Rupali Wadhawan (220189445), Aimal Dastagirzada (220088928), Mahin Bindra (220089330), Pratiksha (220137626)

-- #1: Identify the items with the highest and lowest (non-zero) unit price?
SELECT MAX(bs.unit_price) as Maximum_Unit_Price,
        MIN(bs.unit_price) as Minimum_Unit_Price
FROM assignment01.bakery_sales as bs
WHERE unit_price>0;

-- #2: Write a SQL query to report the second most sold item from the bakery table. If there is no second most sold item, the query should report NULL.
With sales_rank as (SELECT SUM(bs.quantity) as quantity_sum, bs.article,
       rank() over (order by SUM(bs.quantity) desc) as sale_rank
FROM assignment01.bakery_sales bs
GROUP BY bs.article)
SELECT sale_rank, article, quantity_sum
    from sales_rank
where sale_rank=2;

-- #3: Write a SQL query to report the top 3 most sold items for every month in 2022 including their monthly sales.
SELECT * FROM
(
    SELECT EXTRACT('month' from bs.sale_date) as month, bs.article, sum(bs.quantity) as quantity_sum,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT('month' from bs.sale_date) Order by sum(bs.quantity) DESC) AS Sales_rank
    FROM assignment01.bakery_sales bs
    WHERE EXTRACT('year' from bs.sale_date)=2022
    group by EXTRACT('month' from bs.sale_date), bs.article
) rank
WHERE Sales_rank <=3;

-- #4: Write a SQL query to report all the tickets with 5 or more articles in August 2022 including the number of articles in each ticket.
SELECT bs.ticket_number, count(bs.article) as Count_article
FROM assignment01.bakery_sales bs
WHERE bs.sale_date>='2022-08-01' and bs.sale_date<='2022-08-31'
group by bs.ticket_number
HAVING COUNT(bs.ARTICLE)>=5;

-- #5: Write a SQL query to calculate the average sales per day in August 2022?
SELECT EXTRACT(DAY from bs.sale_date) AS sales_date, AVG(bs.quantity*unit_price) AS average_sales
FROM assignment01.bakery_sales bs
WHERE bs.sale_date>='2022-08-01' and bs.sale_date<='2022-08-31'
GROUP BY sales_date
ORDER BY sales_date;

-- #6: Write a SQL query to identify the day of the week with more sales?
SELECT EXTRACT('DOW' FROM bs.sale_date) AS day_of_week,
       SUM(bs.quantity*bs.unit_price) AS total_sales
FROM assignment01.bakery_sales bs
GROUP BY bs.sale_date, day_of_week
ORDER BY total_sales DESC
LIMIT 1;

-- #7: What time of the day is the traditional Baguette more popular?
SELECT EXTRACT('HOUR' FROM bs.sale_datetime) AS HOUR, sum(bs.quantity) as quantity_sum
from assignment01.bakery_sales bs
where bs.article='TRADITIONAL BAGUETTE'
group by HOUR
order by quantity_sum desc
LIMIT 1;

-- #8: Write a SQL query to find the articles with the lowest sales in each month?
SELECT * FROM
(
    SELECT EXTRACT('month' from bs.sale_date) as month, bs.article, sum(bs.quantity*bs.unit_price) as sales,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT('month' from bs.sale_date) Order by sum(bs.quantity*bs.unit_price)) AS Sales_rank
    FROM assignment01.bakery_sales bs
    group by EXTRACT('month' from bs.sale_date), bs.article
) rank
WHERE Sales_rank=1
order by month;

-- #9: Write a query to calculate the percentage of sales for each item between 2022-01-01 and 2022-01-31
SELECT bs.article, (sum(bs.quantity*bs.unit_price)/
                cast((select
                     sum(bs.quantity*bs.unit_price) from assignment01.bakery_sales bs
                                   where bs.sale_date between '2022-08-01' and '2022-08-31') as decimal))*100 as percentage
from assignment01.bakery_sales bs
where bs.sale_date between '2022-08-01' and '2022-08-31'
group by bs.article;

-- #10: The order rate is computed by dividing the volume of a specific article divided by the total amount of
-- items ordered in a specific date. Calculate the order rate for the Baguette for every month during 2022.
SELECT SUM(case when bs.article='BAGUETTE' then bs.quantity else 0 end) as total_qty_baguette,
       SUM(bs.quantity) as total_items,
       date_part('month', bs.sale_date) AS sales_month,
       (SUM(case when bs.article='BAGUETTE' then bs.quantity else 0 end)::numeric)/(SUM(bs.quantity)::numeric) as order_rate
from assignment01.bakery_sales bs
where EXTRACT('year' from bs.sale_date)=2022
group by date_part('month', bs.sale_date)
