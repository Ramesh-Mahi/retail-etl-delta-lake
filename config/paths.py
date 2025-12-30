# config/paths.py

import os
from pathlib import Path

BASE_PATH = os.getenv('ETL_BASE_PATH')

if BASE_PATH is None:
    raise ValueError(
        'ETL_BASE_PATH environment is not set'
    )

BASE_PATH = Path(BASE_PATH)

# Input paths
BRONZE_ORDERS_INPUT = BASE_PATH / 'data' / 'orders'
BRONZE_CUSTOMERS_INPUT = BASE_PATH / 'data' / 'customers'

# Output paths
BRONZE_ORDERS_OUTPUT = BASE_PATH / 'bronze' / 'orders'
BRONZE_CUSTOMERS_OUTPUT = BASE_PATH / 'bronze' / 'customers'

SILVER_ORDERS_OUTPUT = BASE_PATH / 'silver' / 'orders'
SILVER_CUSTOMERS_OUTPUT = BASE_PATH / 'silver' / 'customers'

SILVER_ORDERS_OUTPUT = SILVER_ORDERS_OUTPUT.resolve().as_uri()
SILVER_CUSTOMERS_OUTPUT = SILVER_CUSTOMERS_OUTPUT.resolve().as_uri()

DIM_CUSTOMERS_TABLE_PATH = BASE_PATH / 'gold' / 'dim_customers'
FACT_ORDERS_OUTPUT = BASE_PATH / 'gold' / 'fact_orders'

DIM_CUSTOMERS_TABLE_PATH = DIM_CUSTOMERS_TABLE_PATH.resolve().as_uri()
FACT_ORDERS_OUTPUT = FACT_ORDERS_OUTPUT.resolve().as_uri()
