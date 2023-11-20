-- Project 1: Customer 360
-- Submitted by: Aimal Dastagirzada (220088928), Mahin Bindra (220089330), Pratiksha (220137626), Rupali Wadhawan (220189445)

With conv_cx_order_prod as (
SELECT
    cd.customer_id,
    CD.FIRST_NAME,
    CD.LAST_NAME,
    row_number() over (PARTITION BY cd.customer_id ORDER BY o.order_date) order_recurrence,
    o.order_date,
    o.order_id,
    LEAD(o.order_date) OVER (PARTITION BY cd.customer_id ORDER BY o.order_date) next_order,
    o.order_number,
    pd.product_name,
    o.price_paid,
    o.unit_price,
    o.discount,
    dd2.year_week
FROM fact_tables.orders AS o
INNER JOIN dimensions.customer_dimension AS cd
  ON o.fk_customer = cd.sk_customer
INNER JOIN dimensions.product_dimension  AS pd
  ON o.fk_product = pd.sk_product
join dimensions.date_dimension dd2
on dd2.sk_date = o.fk_order_date
),
conv_with_cx as (
    SELECT
    cd.customer_id as cust_id,
    c.conversion_id,
    c.conversion_date,
    DD.year_week AS CONVERSION_WEEK,
    c.conversion_type,
    c.conversion_channel,
    row_number() over (PARTITION BY cd.customer_id ORDER BY c.conversion_date) recurrence,
    LEAD(c.conversion_date) OVER (PARTITION BY cd.customer_id ORDER BY c.conversion_date) next_conversion_date,
    LEAD(DD.year_week) OVER (PARTITION BY cd.customer_id ORDER BY c.conversion_date) next_conversion_week
    from fact_tables.conversions c
    join dimensions.customer_dimension cd
    on cd.sk_customer = c.fk_customer
    JOIN dimensions.date_dimension DD
    ON DD.date = C.conversion_date
    order by conversion_id
    ),
conversions_with_first_orders AS (
SELECT o.customer_id as cust_id,
    o.order_date as first_order_date,
    o.order_id as first_order_id,
    o.order_number as first_order_number,
    dd.year_week as first_order_week,
    o.product_name as first_order_product,
    o.unit_price as first_order_unit_price,
    o.discount as first_order_discount,
    o.price_paid as first_order_price_paid

FROM conv_cx_order_prod o
join dimensions.date_dimension dd
on o.order_date = dd.date
where order_recurrence=1
),
    WEEKLY_DATA AS (
    SELECT
        DD.year_week AS ORDER_WEEK,
        SUM(o.unit_price) as grand_total,
        SUM(O.price_paid) as week_revenue,
        SUM(O.discount) as week_discounts,
        COUNT(O.order_id) as total_orders_per_week
    FROM fact_tables.orders AS O
    RIGHT JOIN dimensions.date_dimension AS DD
    ON DD.sk_date = O.fk_order_date
    GROUP BY DD.YEAR_WEEK
    ORDER BY dd.year_week
)

SELECT
    ccop.customer_id,
    CCOP.first_name,
    CCOP.last_name,
    cc.conversion_id,
    CC.recurrence,
    CC.conversion_type,
    CC.conversion_date,
    CC.CONVERSION_WEEK,
    CC.conversion_channel,
    CC.next_conversion_date,
    cc.next_conversion_week,
    cfo.first_order_number,
    cfo.first_order_date,
    cfo.first_order_week,
    cfo.first_order_product,
    cfo.first_order_discount,
    cfo.first_order_price_paid,
    cfo.first_order_unit_price,
    w.ORDER_WEEK,
    w.grand_total,
    w.week_revenue,
    w.week_discounts,
    SUM(w.week_revenue) over (partition by cc.conversion_id order by dd.year_week) as cumulative_revenue,
    SUM(w.total_orders_per_week) over (partition by cc.cust_id order by dd.year_week) as loyalty
    from conv_cx_order_prod ccop
    join conv_with_cx cc
    on ccop.customer_id= cc.cust_id
    JOIN conversions_with_first_orders CFO
    ON CFO.cust_id=CCOP.customer_id
    JOIN dimensions.date_dimension DD
    ON DD.date=ccop.order_date
    join WEEKLY_DATA w
    on w.order_week = dd.year_week
order by customer_id, conversion_id,dd.year_week;



conversions_with_first_orders AS (
SELECT o.customer_id as cust_id,
    o.order_date as first_order_date,
    o.order_id as first_order_id,
    o.product_name as first_order_product,
    o.price_paid as first_order_total_paid,
    o.price_paid as first_order_discount
FROM conv_cx_order_prod o
where order_recurrence=1
) SELECT *





-- all first order details for each customer
conversions_with_first_orders AS (
SELECT o.customer_id as cust_id,
    o.order_date as first_order_date,
    o.order_id as first_order_id,
    o.product_name as first_order_product,
    o.price_paid as first_order_total_paid,
    o.price_paid as first_order_discount
FROM conv_cx_order_prod o
where order_recurrence=1
)
SELECT *
from conv_cx_order_prod c
join conversions_with_first_orders cf
on c.customer_id = cf.cust_id