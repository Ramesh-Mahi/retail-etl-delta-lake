from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config.paths import *

spark = (SparkSession.builder
         .appName('Retail_ETL')
         .getOrCreate())

orders_schema = StructType([
    StructField('order_id', StringType()),
    StructField('customer_id', StringType()),
    StructField('order_date', TimestampType()),
    StructField('amount', IntegerType()),
    StructField('status', StringType()),
    StructField('updated_at', TimestampType())
])

# Bronze layer ingests raw source data in append mode using Delta Lake,
# with explicit schemas and ingestion timestamps for auditability and replay

bronze_orders = (spark.read
                 .format('csv')
                 .option('header', True)
                 .schema(orders_schema)
                 .load(BRONZE_ORDERS_INPUT)
                 .withColumn('ingestion_ts', F.current_timestamp()))

bronze_orders.write \
    .format('delta') \
    .mode('append') \
    .save(BRONZE_ORDERS_OUTPUT)