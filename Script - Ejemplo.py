# Glue ETL de ejemplo (simplificado)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, round as sround, expr

spark = SparkSession.builder.getOrCreate()

products = spark.read.option("header", True).option("inferSchema", True) \
    .csv("s3://<tu-bucket>/novashop/raw/products/")

sales = spark.read.option("header", True).option("inferSchema", True) \
    .csv("s3://<tu-bucket>/novashop/raw/sales/")

joined = sales.join(products, on="product_id", how="left")

# Métricas calculadas
with_metrics = joined.withColumn("subtotal", col("qty") * col("unit_price")) \
    .withColumn("discount_amount", sround(col("subtotal") * col("discount_pct"), 2)) \
    .withColumn("total", sround(col("subtotal") - col("discount_amount"), 2)) \
    .withColumn("cogs", sround(col("qty") * col("unit_cost"), 2)) \
    .withColumn("profit", sround(col("total") - col("cogs"), 2)) \
    .withColumn("year", year(col("order_date"))) \
    .withColumn("month", month(col("order_date")))

# Escribir CURATED en Parquet, particionado por año/mes
with_metrics.write.mode("overwrite").partitionBy("year", "month") \
    .parquet("s3://<tu-bucket>/novashop/curated/sales_merged/")
