import pandas as pd 
import duckdb


customers = pd.read_csv('DataLake/customers/customers_dataset.csv')
orders = pd.read_csv('DataLake/orders/orders_dataset.csv')
orders_items = pd.read_csv('DataLake/orders/order_items_dataset.csv')
payments = pd.read_csv('DataLake/payments/order_payments_dataset.csv')
products_category = pd.read_csv('DataLake/products/product_category_name_translation.csv')
products = pd.read_csv('DataLake/products/products_dataset.csv')
reviews = pd.read_csv('DataLake/reviews/order_reviews_dataset.csv')
sellers = pd.read_csv('DataLake/sellers/sellers_dataset.csv')
geolocation = pd.read_csv('DataLake/geolocation_dataset.csv')

con = duckdb.connect(':memory:')
con.sql("""
    ATTACH 'dbname=datalake user=postgres password=password host=localhost port=5432' 
    AS postgres_db (TYPE POSTGRES)
""")
#dim_customers
con.sql("""
    INSERT INTO postgres_db.dim_customers
    SELECT 
        customer_id,
        customer_unique_id,
        customer_city,
        customer_state
    FROM customers
""")

#dim_selllers
con.sql("""
    INSERT INTO postgres_db.dim_sellers
    SELECT 
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM sellers
""")

#dim_products
con.sql("""
    INSERT INTO postgres_db.dim_products
    SELECT DISTINCT
        o.product_id,
        p.product_category_name,
        MAX(o.price) as price
    FROM orders_items o
    LEFT JOIN products p ON o.product_id = p.product_id
    GROUP BY o.product_id, p.product_category_name
""")

#dim_time
con.sql("""
    INSERT INTO postgres_db.dim_time (date_id, order_date)
        WITH unique_dates AS (
        SELECT DISTINCT
            order_purchase_timestamp::DATE as order_date
        FROM orders
    ),
    numbered_dates AS (
        SELECT
            order_date,
            'D' || ROW_NUMBER() OVER (ORDER BY order_date) AS date_id
        FROM unique_dates
    )
    SELECT date_id, order_date FROM numbered_dates
""")

#fact_sales
con.sql("""
INSERT INTO postgres_db.fact_sales
WITH dim_time AS (
    WITH unique_dates AS (
        SELECT DISTINCT
            order_purchase_timestamp::DATE as order_date
        FROM orders
    ),
    numbered_dates AS (
        SELECT
            order_date,
            'D' || ROW_NUMBER() OVER (ORDER BY order_date) AS date_id
        FROM unique_dates
    )
    SELECT date_id, order_date FROM numbered_dates
)
SELECT
    oi.order_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    dt.date_id AS date_purchase_id,
    SUM(oi.price + oi.freight_value) AS payment_value,
    COUNT(*) AS quantity,
    (o.order_delivered_customer_date::DATE - o.order_purchase_timestamp::DATE) AS delivery_time,
    o.order_status,
    (o.order_estimated_delivery_date::DATE - o.order_purchase_timestamp::DATE) AS estimated_delivery_time
FROM orders_items AS oi
JOIN orders AS o
    ON oi.order_id = o.order_id
JOIN dim_time AS dt 
    ON o.order_purchase_timestamp::DATE = dt.order_date
GROUP BY 1,2,3,4,5,8,9,10
ORDER BY oi.order_id, oi.product_id
""")