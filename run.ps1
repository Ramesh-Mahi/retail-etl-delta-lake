$DELTA="io.delta:delta-spark_2.12:3.1.0"
$CONF1="spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
$CONF2="spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

spark-submit --packages $DELTA --conf $CONF1 --conf $CONF2 --py-files config.zip bronze/bronze_customers.py
spark-submit --packages $DELTA --conf $CONF1 --conf $CONF2 --py-files config.zip bronze/bronze_orders.py

spark-submit --packages $DELTA --conf $CONF1 --conf $CONF2 --py-files config.zip silver/silver_customers.py
spark-submit --packages $DELTA --conf $CONF1 --conf $CONF2 --py-files config.zip silver/silver_orders.py

spark-submit --packages $DELTA --conf $CONF1 --conf $CONF2 --py-files config.zip gold/dim_customers.py
spark-submit --packages $DELTA --conf $CONF1 --conf $CONF2 --py-files config.zip gold/fact_orders.py
