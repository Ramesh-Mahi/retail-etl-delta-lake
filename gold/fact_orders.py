from common.spark_session import get_spark
from pyspark.sql import functions as F
from config.paths import *

spark = get_spark('Retail_ETL')

# Building a fact table here in the gold pipeline

silver_orders_df = spark.read.format('delta').load(SILVER_ORDERS_OUTPUT)

dim_customers_df = spark.read.format('delta').load(DIM_CUSTOMERS_TABLE_PATH).select(
        'customer_sk',
        'customer_id',
        'name',
        'city',
        'start_date',
        'end_date'
    )

fact_orders_df = (
    silver_orders_df.alias('t').join(
        dim_customers_df.alias('s'),
        (F.col('t.customer_id') == F.col('s.customer_id')) &
        (F.col('t.order_date') >= F.col('s.start_date')) &
        (
            (F.col('t.order_date') < F.col('s.end_date')) |
            (F.col('s.end_date').isNull())
        ),
        'left'
    )
    .select(
        F.col('t.order_id'),
        #handle late arriving / missing dimension -order arrive before customer or customer record is delayed (-1 = unknown customer)
        F.coalesce(F.col('s.customer_sk'),F.lit(-1)).alias('customer_sk'),
        F.col('t.order_date'),
        F.col('t.amount'),
        F.col('t.status'),
        F.col('s.name').alias('customer_name'),
        F.col('s.city').alias('customer_city')
    )
)

fact_orders_df.write \
    .format('delta') \
    .mode('overwrite') \
    .save(FACT_ORDERS_OUTPUT)

fact_orders_df.show(truncate=False)