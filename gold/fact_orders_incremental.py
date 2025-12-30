from common.spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from config.paths import *

spark = get_spark('Retail_ETL')

#1 reading silver orders
silver_orders_df = spark.read.format('delta').load(SILVER_ORDERS_OUTPUT)

#2 read existing fact table (if exists) to get last processed order_date

try:
    fact_orders_existing = spark.read.format('delta').load(FACT_ORDERS_OUTPUT)

    max_order_date = (fact_orders_existing
                      .agg(F.max('order_date').alias('max_date'))
                      .collect()[0]['max_date'])
except AnalysisException:
    max_order_date = None

#3 filter only new orders

if max_order_date:
    incremental_orders_df = silver_orders_df.filter(
        F.col('order_date') > F.lit(max_order_date)
    )
else:
    incremental_orders_df = silver_orders_df

#4 read full dimension table (NO is_current filter to keep all the historical data)
dim_customers_df = spark.read.format('delta').load(DIM_CUSTOMERS_TABLE_PATH)

#5 resolve the surrogate key using the date logic - SCD type 2
fact_orders_incremental_df = (
    incremental_orders_df.alias('t').join(
        dim_customers_df.alias('s'),
        (F.col('t.customer_id')==F.col('s.customer_id')) &
        (F.col('t.order_date') >= F.col('s.start_date')) &
        (
            (F.col('t.order_date') < F.col('s.end_date')) |
            (F.col('s.end_date').isNull())
        ),
        'left'
    )
    .select(
        F.col('t.order_id'),
        F.col('s.customer_sk'),
        F.col('t.order_date'),
        F.col('t.amount'),
        F.col('t.status'),
        F.col('s.name').alias('customer_name'),
        F.col('s.city').alias('customer_city')
    )
)

#6 Append incremental records to fact table
fact_orders_incremental_df.write\
    .format('delta')\
    .mode('append')\
    .save(FACT_ORDERS_OUTPUT)

fact_orders_incremental_df.show(truncate=False)