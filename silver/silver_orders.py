from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.paths import *

spark = (SparkSession.builder
         .appName('Retail_ETL')
         .getOrCreate())

bronze_orders_df = spark.read \
    .format('delta') \
    .load(BRONZE_ORDERS_OUTPUT)

window_spec = Window.partitionBy('order_id').orderBy(F.desc('updated_at'))

silver_orders = (bronze_orders_df
                 .withColumn('rn', F.row_number().over(window_spec))
                 .filter(F.col('rn') == 1)
                 .drop('rn'))

silver_orders.write \
    .format('delta') \
    .mode('overwrite') \
    .save(SILVER_ORDERS_OUTPUT)
