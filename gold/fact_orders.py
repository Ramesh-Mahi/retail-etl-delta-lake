from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.paths import *

spark = (SparkSession.builder
         .appName('Retail_ETL')
         .getOrCreate())

# Building a fact table here in the gold pipeline

silver_orders_df = spark.read.format('delta').load(SILVER_CUSTOMERS_OUTPUT)

dim_customers_df = (spark.read
                    .table('dim_customers')
                    .filter(F.col('is_current') == True))

fact_orders_df = (
    silver_orders_df.alias('t').join(
        dim_customers_df.alias('s'),
        F.col('t.customer_id') == F.col('s.customer_id'),
        'left'
    )
    .select(
        F.col('t.order_id'),
        F.col('t.customer_id'),
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