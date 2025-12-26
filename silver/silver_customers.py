from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.paths import *

spark = (SparkSession.builder
         .appName('Retail_ETL')
         .getOrCreate())

bronze_customers_df = spark.read.format('delta').load(BRONZE_CUSTOMERS_OUTPUT)

window_spec2 = Window.partitionBy('customer_id').orderBy(F.col('updated_at').desc())

silver_customers = (
    bronze_customers_df
    .withColumn('rn', F.row_number().over(window_spec2))
    .filter(F.col('rn') == 1)
    .drop('rn')
)

silver_customers.write \
    .format('delta') \
    .mode('overwrite') \
    .save(SILVER_CUSTOMERS_OUTPUT)
