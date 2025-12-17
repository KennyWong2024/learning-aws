import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Lectura de datos
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "retail_grupo1", 
    table_name = "raw_store_csv", 
    transformation_ctx = "datasource0"
)

## Transformacion, nombres snake_case
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

## Carga de datos en parquet
datasink2 = glueContext.write_dynamic_frame.from_options(
    frame = applymapping1, 
    connection_type = "s3", 
    connection_options = {
        "path": "s3://group-one-project-uh/project/curated/store/"
    }, 
    format = "parquet", 
    transformation_ctx = "datasink2"
)

job.commit()