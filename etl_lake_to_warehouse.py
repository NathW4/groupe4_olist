import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# Chemin vers le Data Lake
data_lake_path = r"C:\Users\dantu\Desktop\code\data_warehouse_management\DataLake"

# Charger les données brutes
customers = pd.read_csv(os.path.join(data_lake_path, "customers/olist_customers_dataset.csv"))
sellers =  pd.read_csv(os.path.join(data_lake_path, "sellers/olist_sellers_dataset.csv"))
products = pd.read_csv(os.path.join(data_lake_path, "products/olist_products_dataset.csv"))
product_category_translate = pd.read_csv(os.path.join(data_lake_path, "products/product_category_name_translation.csv"))
orders_items = pd.read_csv(os.path.join(data_lake_path, "orders/olist_order_items_dataset.csv"))
orders = pd.read_csv(os.path.join(data_lake_path, "orders/olist_orders_dataset.csv"))


customers_filtered = customers[['customer_id', 'customer_unique_id', 'customer_city', 'customer_state']]

sellers_filtered = sellers[['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']]
sellers_filtered = sellers_filtered.rename(columns={'seller_zip_code_prefix': 'seller_zipcode'})

merged_df = pd.merge(orders_items[['product_id', 'price']], products[['product_id', 'product_category_name']],
                     on='product_id', how='left')
aggregated_df = merged_df.groupby(['product_id', 'product_category_name'], as_index=False).agg(
    product_price=('price', 'max')
)
aggregated_df = pd.merge(aggregated_df, product_category_translate[['product_category_name', 'product_category_name_english']],
                         on='product_category_name', how='left')
aggregated_df['product_category_name'] = aggregated_df['product_category_name_english']
aggregated_df = aggregated_df.drop(columns=['product_category_name_english'])
aggregated_df = aggregated_df.rename(columns={'product_category_name': 'product_category'})

orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
unique_dates = orders[['order_purchase_timestamp']].drop_duplicates()
unique_dates = unique_dates.sort_values(by='order_purchase_timestamp')
unique_dates['date_id'] = 'D' + (unique_dates.reset_index().index + 1).astype(str)
time_df = unique_dates.rename(columns={'order_purchase_timestamp': 'order_date'})

orders['order_purchase_date'] = pd.to_datetime(orders['order_purchase_timestamp']).dt.date
dim_time = orders[['order_purchase_date']].drop_duplicates().reset_index(drop=True)
dim_time = dim_time.sort_values(by='order_purchase_date').reset_index(drop=True)
dim_time['date_id'] = 'D' + (dim_time.index + 1).astype(str)

merged_orders = pd.merge(
    orders_items,
    orders[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
            'order_delivered_customer_date', 'order_estimated_delivery_date', 'order_purchase_date']],
    on='order_id',
    how='left'
)

merged_orders = pd.merge(
    merged_orders,
    dim_time,
    left_on='order_purchase_date',
    right_on='order_purchase_date',
    how='left'
)

merged_orders['order_delivered_customer_date'] = pd.to_datetime(
    merged_orders['order_delivered_customer_date'], errors='coerce'
)
merged_orders['order_purchase_timestamp'] = pd.to_datetime(
    merged_orders['order_purchase_timestamp'], errors='coerce'
)
merged_orders['order_estimated_delivery_date'] = pd.to_datetime(
    merged_orders['order_estimated_delivery_date'], errors='coerce'
)

merged_orders['payment_value'] = merged_orders['price'] + merged_orders['freight_value']

merged_orders['delivery_time'] = (
    merged_orders['order_delivered_customer_date'] - merged_orders['order_purchase_timestamp']
).dt.days

merged_orders['estimated_delivery_time'] = (
    merged_orders['order_estimated_delivery_date'] - merged_orders['order_purchase_timestamp']
).dt.days

fact_sales = merged_orders.groupby(
    ['order_id', 'customer_id', 'product_id', 'seller_id', 'date_id', 'order_status']
).agg(
    payment_value=('payment_value', 'sum'),
    quantity=('order_item_id', 'count'),
    delivery_time=('delivery_time', 'first'),
    estimated_delivery_time=('estimated_delivery_time', 'first')
).reset_index()

fact_sales = fact_sales.rename(columns={
    'date_id': 'date_purchase_id',
    'order_status': 'status'
})


# Connexion PostgreSQL avec pg8000
engine = create_engine("postgresql+pg8000://etl_user:password@localhost:5432/olist_dwh")

# Tester la connexion
try:
    with engine.connect() as connection:
        print("Connection to PostgreSQL successful!")
except OperationalError as e:
    print("Failed to connect to PostgreSQL:", e)
    exit()

# Charger les données dans PostgreSQL avec une transaction
try:
    with engine.begin() as connection:
        customers_filtered.to_sql('dim_customers', connection, if_exists='append', index=False)
        print("Data successfully loaded into 'dim_customers' table.")

        sellers_filtered.to_sql('dim_sellers', connection, if_exists='append', index=False)
        print("Data successfully loaded into 'dim_sellers' table.")

        aggregated_df.to_sql('dim_products', connection, if_exists='append', index=False)
        print("Data successfully loaded into 'dim_products' table.")

        time_df.to_sql('dim_time', connection, if_exists='append', index=False)
        print("Data successfully loaded into 'dim_time' table.")

        fact_sales.to_sql('fact_orders', connection, if_exists='append', index=False)
        print("Data successfully loaded into 'fact_sales' table.")
except ImportError as e:
    print("Error while loading data into PostgreSQL:", e)
