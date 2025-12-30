from common.spark_session import get_spark
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from config.paths import *
from pyspark.sql.window import Window

spark = get_spark('Retail_ETL')
# gold dimensions contain business entities with historical tracking
print("DIM_CUSTOMERS_TABLE_PATH =", DIM_CUSTOMERS_LOCATION)
print('SILVER_CUSTOMERS_OUTPUT=', SILVER_CUSTOMERS_OUTPUT)
#DIM_CUSTOMERS_TABLE_PATH = r'C:/Users/rames/PycharmProjects/pyspark/retail-etl-delta-lake/gold/dim_customers'

spark.sql(f'''
CREATE TABLE IF NOT EXISTS dim_customers(
    customer_sk BIGINT, 
    customer_id STRING,
    name STRING,
    email STRING,
    city STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
    )
    USING DELTA
    LOCATION '{DIM_CUSTOMERS_LOCATION}'
''')

silver_customers_df = spark.read.format('delta').load(SILVER_CUSTOMERS_OUTPUT)

dim_customers_df = spark.read.table('dim_customers')

max_sk = dim_customers_df.agg(F.max('customer_sk')).collect()[0][0]
max_sk = max_sk if max_sk is not None else 0

new_dim_rows = (
    silver_customers_df
    .withColumn('customer_sk', F.row_number().over(Window.orderBy('customer_id'))
                + F.lit(max_sk))
)

dim_customers = DeltaTable.forName(spark, 'dim_customers')

dim_customers.alias('t') \
    .merge(new_dim_rows.alias('s'),
           't.customer_id = s.customer_id AND t.is_current = true') \
    .whenMatchedUpdate(
    #NULL equality handling logic so using <=> with NOT
    condition='''
    NOT (
    t.name <=> s.name AND
    t.email <=> s.email AND
    t.city <=> s.city
    )
    ''',
    set={
        'end_date': F.col('s.updated_at'),
        'is_current': F.lit(False)
    }) \
    .whenNotMatchedInsert(
    values={
        'customer_sk': 's.customer_sk',
        'customer_id': 's.customer_id',
        'name': 's.name',
        'email': 's.email',
        'city': 's.city',
        'start_date': F.col('s.updated_at'),
        'end_date': F.lit(None).cast('timestamp'),
        'is_current': F.lit(True)
    }
).execute()

