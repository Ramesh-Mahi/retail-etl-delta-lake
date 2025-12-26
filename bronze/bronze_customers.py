from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config.paths import *

spark = (SparkSession.builder
         .appName('Retail_ETL')
         .getOrCreate())

customer_schema = StructType([
    StructField('customer_id', StringType()),
    StructField('name', StringType()),
    StructField('email', StringType()),
    StructField('city', StringType()),
    StructField('updated_at', TimestampType())
])

bronze_customers = (spark.read
                    .format('csv')
                    .option('header', True)
                    .schema(customer_schema)
                    .load(BRONZE_CUSTOMERS_INPUT)
                    .withColumn('ingestion_ts', F.current_timestamp()))

bronze_customers.write \
    .format('delta') \
    .mode('append') \
    .save(BRONZE_CUSTOMERS_OUTPUT)
