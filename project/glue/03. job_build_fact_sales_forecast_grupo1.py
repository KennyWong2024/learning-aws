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
    table_name = "raw_sample_submission", 
    transformation_ctx = "datasource0"
)

## Transformaciones
applymapping1 = ApplyMapping.apply(
    frame = datasource0, 
    mappings = [
        ("id", "long", "store_id", "string"), 
        ("sales", "long", "sales", "long")
    ], 
    transformation_ctx = "applymapping1"
)

## Carga de Datos
df_final = applymapping1.toDF()

# Ruta destino
path_output = "s3://uh-retail-grupo1/curated/fact_sales_forecast/"

df_final.write.mode("overwrite").parquet(path_output)

job.commit()