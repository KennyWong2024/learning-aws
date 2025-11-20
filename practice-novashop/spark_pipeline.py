## Nota: esto no se ejecutó en pySpark local, fue ejecutado en AWS Glue Script Editor
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, year, month, round as sround, expr

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## "E" Extract

dyf_sales = glueContext.create_dynamic_frame.from_catalog(
    database="practice-database-group-one", 
    table_name="novashop_sales_csv"
)

dyf_products = glueContext.create_dynamic_frame.from_catalog(
    database="practice-database-group-one", 
    table_name="novashop_products_csv"
)

# 2. Convertimos a DataFrames
df_sales = dyf_sales.toDF()
df_products = dyf_products.toDF().withColumnRenamed("unit_price", "unit_price_catalog")

## "T" Transform
# 3. Join nativo (Usando Data Frames y no Dynamic Frames)
df_join_sales_products = df_sales.join(
    df_products, 
    df_sales["product_id"] == df_products["product_id"], 
    "left"
)

## Cálculo de métricas (Brindado por el ejercicio)
with_metrics = df_join_sales_products.withColumn("subtotal", col("qty") * col("unit_price")) \
    .withColumn("discount_amount", sround(col("subtotal") * col("discount_pct"), 2)) \
    .withColumn("total", sround(col("subtotal") - col("discount_amount"), 2)) \
    .withColumn("cogs", sround(col("qty") * col("unit_cost"), 2)) \
    .withColumn("profit", sround(col("total") - col("cogs"), 2)) \
    .withColumn("year", year(col("order_date"))) \
    .withColumn("month", month(col("order_date")))

## "L" Load

dyf_output = DynamicFrame.fromDF(with_metrics, glueContext, "dyf_output")

# 2. Escribimos en S3
glueContext.write_dynamic_frame.from_options(
    frame = dyf_output,
    connection_type = "s3",
    connection_options = {
        "path": "s3://group-one-project-uh/practices_group/novashop-curated/"
    },
    format = "parquet"
)

job.commit()