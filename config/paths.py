# config/paths.py

import os

BASE_PATH = os.getenv('ETL_BASE_PATH')

if BASE_PATH is None:
    raise ValueError(
        'ETL_BASE_PATH environment is not set'
    )

# Input paths
BRONZE_ORDERS_INPUT = f"{BASE_PATH}/data/orders"
BRONZE_CUSTOMERS_INPUT = f"{BASE_PATH}/data/customers"

# Output paths
BRONZE_ORDERS_OUTPUT = f"{BASE_PATH}/bronze/orders"
BRONZE_CUSTOMERS_OUTPUT = f"{BASE_PATH}/bronze/customers"

SILVER_ORDERS_OUTPUT = f"{BASE_PATH}/silver/orders"
SILVER_CUSTOMERS_OUTPUT = f"{BASE_PATH}/silver/customers"

FACT_ORDERS_OUTPUT = f"{BASE_PATH}/gold/fact_orders"
