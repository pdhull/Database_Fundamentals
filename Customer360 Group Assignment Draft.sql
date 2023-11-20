with customer_order as (select cd.customer_id,
       o.*
from fact_tables.orders as o
left join dimensions.customer_dimension as cd
on (o.fk_customer= cd.sk_customer)
order by  cd.customer_id),
    customerid_date as (
select cd.*,
       dd.*
from dimensions.customer_dimension as cd
cross join dimensions.date_dimension as dd
order by cd.customer_id, dd.date),
    customer_all_week as(
select cid.customer_id,
       cid.sk_customer,
       cid.first_name,
       cid.last_name,
       cid.sk_date,
       cid.date,
       cid.year_week,
       cid.week,
       co.order_id,
       co.order_number,
       co.order_date,
       co.fk_order_date,
       co.fk_customer,
       co.fk_product,
       co.order_item_id,
       co.unit_price,
       co.discount,
       co.price_paid
from customerid_date as cid
left join customer_order as co
on (cid.customer_id= co.customer_id) and
   (cid.sk_date= co.fk_order_date)
order by cid.customer_id, cid.sk_date),
    weekly_revenue as(
  select cw.customer_id,
         cw.year_week,
         coalesce(sum(cw.price_paid), 0) as week_revenue,
         coalesce(sum(cw.discount), 0) as week_discount,
         coalesce(SUM(SUM(cw.price_paid)) OVER (PARTITION BY cw.customer_id ORDER BY cw.year_week), 0)AS cumulative_revenue
from customer_all_week as cw
group by cw.customer_id, cw.year_week
order by cw.customer_id, cw.year_week),
    weekly_revenue_loyalty as(
    select *,
           dense_rank() over (PARTITION BY wr.customer_id ORDER BY wr.cumulative_revenue) loyalty
from weekly_revenue as wr),
    week_dates as(
select dd.year_week,
       min(date) as week_start_date,
       max(date) as week_end_date
    from dimensions.date_dimension as dd
    group  by dd.year_week
    order by dd.year_week),
final_revenue_calculation as(
select wrl.*,
       wd.week_start_date,
       wd.week_end_date
from weekly_revenue_loyalty as wrl
left join  week_dates as wd
on (wrl.year_week= wd.year_week)),
     conversions_with_customer_id AS (
SELECT cd.customer_id,
       cs.conversion_date,
       cs.conversion_type,
       cs.conversion_channel,
       row_number() over (PARTITION BY cd.customer_id ORDER BY cs.conversion_date) recurrence,
       LEAD(cs.conversion_date) OVER (PARTITION BY cd.customer_id ORDER BY cs.conversion_date) next_conversion_date,
       cs.order_number,
       dd.year_week
FROM fact_tables.conversions AS cs
INNER JOIN dimensions.customer_dimension AS cd
  ON cs.fk_customer = cd.sk_customer
INNER JOIN dimensions.date_dimension AS dd
  ON cs.fk_conversion_date = dd.sk_date
), orders_with_customer_id AS (
SELECT cd.customer_id,
       row_number() over (PARTITION BY cd.customer_id ORDER BY o.order_date) order_recurrence,
       o.order_date,
       o.order_id,
       LEAD(o.order_date) OVER (PARTITION BY cd.customer_id ORDER BY o.order_date) next_order_date,
       o.order_number,
       pd.product_name,
       o.price_paid,
       o.discount
FROM fact_tables.orders AS o
INNER JOIN dimensions.customer_dimension AS cd
  ON o.fk_customer = cd.sk_customer
INNER JOIN dimensions.product_dimension  AS pd
  ON o.fk_product = pd.sk_product
), conversions_with_first_orders AS (
SELECT cs.*,
       o.order_date,
       o.product_name,
       o.price_paid
FROM conversions_with_customer_id AS cs
LEFT JOIN orders_with_customer_id AS o
  ON cs.order_number = o.order_number
), first_orders as (
select cs.order_date as first_order_date,
       cs.order_id as first_order_id,
       cs.product_name as first_order_product,
       cs.price_paid as first_order_total_paid,
       cs.discount as first_order_discount,
       ci.year_week as first_order_week,
       ci.customer_id
from orders_with_customer_id as cs
left join conversions_with_customer_id as ci
on (cs.order_number = ci.order_number)
where cs.order_recurrence= 1),
    firsts_of_cust as(
SELECT cs_fo.*,
       fo.first_order_date,
       fo.first_order_week,
       fo.first_order_id,
       fo.first_order_product,
       fo.first_order_total_paid,
       fo.first_order_discount
FROM conversions_with_first_orders AS cs_fo
left join first_orders as fo
on (cs_fo.customer_id= fo.customer_id)
order by cs_fo.customer_id),
   f_1 as (
select
       fc.customer_id,
       fc.conversion_date,
       fc.conversion_type,
       fc.conversion_channel,
       fc.recurrence,
       fc.next_conversion_date,
       fc.order_number,
       fc.order_date,
       fc.product_name,
       fc.price_paid,
       fc.first_order_date,
       fc.first_order_week,
       fc.first_order_id,
       fc.first_order_product,
       fc.first_order_total_paid,
       fc.first_order_discount,
    wrc.year_week,
    wrc.week_revenue,
    wrc.week_discount,
    wrc.cumulative_revenue,
    wrc.loyalty,
    wrc.week_start_date,
    wrc.week_end_date
from  final_revenue_calculation as wrc
left join firsts_of_cust as fc
on (wrc.customer_id= fc.customer_id) and
(wrc.week_end_date>=fc.conversion_date)

order by fc.customer_id, fc.conversion_date, wrc.year_week),

 ft1 as (
    select *
      from f_1
      where conversion_date< week_end_date and
             next_conversion_date> week_end_date),
   ft2 as (
        select *
        from f_1
        where next_conversion_date is null),
 union_all_table as (
        select *
from ft1

union all
select *
from ft2)
   -- final_table_2 as(

      select customer_id,
             conversion_date,
             recurrence,
             conversion_type,
             conversion_channel,
             next_conversion_date,
             first_order_date,
             first_order_week,
             first_order_id,
             first_order_product,
             first_order_total_paid,
             first_order_discount,
             year_week as order_week,
             week_revenue,
             week_discount,
             cumulative_revenue,
             loyalty
from union_all_table
order by customer_id, conversion_date, year_week;

select * from fact_tables.orders
select * from dimensions.customer_dimension