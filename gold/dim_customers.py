from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from config.paths import *

spark = (SparkSession.builder
         .appName('Retail_ETL')
         .getOrCreate())

# gold dimensions contain business entities with historical tracking

spark.sql('''
CREATE TABLE IF NOT EXISTS dim_customers(
    customer_id STRING,
    name STRING,
    email STRING,
    city STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
    )
    USING DELTA
    LOCATION "C:/Users/rames/PycharmProjects/pyspark/retail_etl/gold/dim_customers";
''')

silver_customers_df = spark.read.format('delta').load(SILVER_CUSTOMERS_OUTPUT)

dim_customers = DeltaTable.forName(spark, 'dim_customers')

dim_customers.alias('t') \
    .merge(silver_customers_df.alias('s'),
           't.customer_id = s.customer_id AND t.is_current = true') \
    .whenMatchedUpdate(
    condition='''
    t.name <> s.name OR 
    t.email <> s.email OR
    t.city <> s.city    
    ''',
    set={
        'end_date': F.current_timestamp(),
        'is_current': F.lit(False)
    }) \
    .whenNotMatchedInsert(
    values={
        'customer_id': 's.customer_id',
        'name': 's.name',
        'email': 's.email',
        'city': 's.city',
        'start_date': F.current_timestamp(),
        'end_date': F.lit(None).cast('timestamp'),
        'is_current': F.lit(True)
    }
).execute()
