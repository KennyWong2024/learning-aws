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

## Columnas derivadas (Transformaciones)
# A) competition_open_year_month (YYYY-MM)
# Usamos lpad para asegurar que el mes 9 se convierta en '09'
# Solo lo calculamos si tenemos ambos datos (Año y Mes), si no, queda Null.
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

# B) promo2_start_year_week (YYYY-WW)
# Similar al anterior, aseguramos que la semana tenga 2 dígitos.
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

# C) promo_interval_array (String -> Array)
# Convertimos "Jan,Apr,Jul,Oct" en un array real ["Jan", "Apr", "Jul", "Oct"]
# La función split hace esto automáticamente. Si es nulo, devuelve nulo.
df_transformed = df_transformed.withColumn("promo_interval_array", 
    split(col("promo_interval"), ",")
)

## Carga de datos
path_output = "s3://uh-retail-grupo1/curated/dim_store/"

df_transformed.write.mode("overwrite").parquet(path_output)

job.commit()