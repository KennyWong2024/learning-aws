import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat, lit, lpad, split, when

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Lectura de datos 
path_input = "s3://uh-retail-grupo1/curated/store/"
df_store = spark.read.parquet(path_input)

## Columnas derivadas 

# competition_open_year_month (YYYY-MM)
df_transformed = df_store.withColumn("competition_open_year_month", 
    when(
        col("competition_open_since_year").isNotNull() & col("competition_open_since_month").isNotNull(),
        concat(
            col("competition_open_since_year"), 
            lit("-"), 
            lpad(col("competition_open_since_month").cast("string"), 2, "0")
        )
    ).otherwise(None)
)

# promo2_start_year_week (YYYY-WW)
df_transformed = df_transformed.withColumn("promo2_start_year_week", 
    when(
        col("promo2_since_year").isNotNull() & col("promo2_since_week").isNotNull(),
        concat(
            col("promo2_since_year"), 
            lit("-"), 
            lpad(col("promo2_since_week").cast("string"), 2, "0")
        )
    ).otherwise(None)
)

# promo_interval_array (String -> Array)
df_transformed = df_transformed.withColumn("promo_interval_array", 
    split(col("promo_interval"), ",")
)

## Maping de datos
df_final = df_transformed.select(
    col("store_id"),
    col("store_type"),
    col("assortment"),
    col("competition_distance"),
    
    # Columnas originales de Competition
    col("competition_open_since_month"),
    col("competition_open_since_year"),
    
    # Nueva columna derivada Competition
    col("competition_open_year_month"), 
    
    # Columnas originales de Promo2
    col("promo2"),
    col("promo2_since_week"),
    col("promo2_since_year"),
    
    # Nueva columna derivada Promo2
    col("promo2_start_year_week"), 

    # Intervalos
    col("promo_interval"),     # Original STRING
    col("promo_interval_array") # Arreglo
)

## Carga de datos
path_output = "s3://uh-retail-grupo1/curated/dim_store/"
df_final.write.mode("overwrite").parquet(path_output)

job.commit()