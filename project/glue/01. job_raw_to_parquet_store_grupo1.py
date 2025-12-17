import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Lectura de datos
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "retail_grupo1", 
    table_name = "raw_store", 
    transformation_ctx = "datasource0"
)

## Transformaciones a snake_case
applymapping1 = ApplyMapping.apply(
    frame = datasource0, 
    mappings = [
        ("store", "long", "store_id", "string"), 
        ("storetype", "string", "store_type", "string"),
        ("assortment", "string", "assortment", "string"),
        ("competitiondistance", "long", "competition_distance", "long"),
        ("competitionopensincemonth", "long", "competition_open_since_month", "long"),
        ("competitionopensinceyear", "long", "competition_open_since_year", "long"),
        ("promo2", "long", "promo2", "long"),
        ("promo2sinceweek", "long", "promo2_since_week", "long"),
        ("promo2sinceyear", "long", "promo2_since_year", "long"),
        ("promointerval", "string", "promo_interval", "string")
    ], 
    transformation_ctx = "applymapping1"
)

spark_df = applymapping1.toDF()
spark_df_clean = spark_df.withColumn("promo_interval", 
    when(col("promo_interval") == "", None).otherwise(col("promo_interval"))
)
final_dynamic_frame = DynamicFrame.fromDF(spark_df_clean, glueContext, "final_dynamic_frame")


## Carga de datos
datasink2 = glueContext.write_dynamic_frame.from_options(
    frame = final_dynamic_frame, 
    connection_type = "s3", 
    connection_options = {
        "path": "s3://uh-retail-grupo1/curated/store/curated_store"
    }, 
    format = "parquet", 
    transformation_ctx = "datasink2"
)

job.commit()