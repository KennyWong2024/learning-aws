import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame 
from pyspark.sql.functions import to_date, col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Lectura de datos
Test_node = glueContext.create_dynamic_frame.from_catalog(database="project_db_group_one", table_name="raw_test_csv", transformation_ctx="Test_node")
Train_node = glueContext.create_dynamic_frame.from_catalog(database="project_db_group_one", table_name="raw_train_csv", transformation_ctx="Train_node")
Store_node = glueContext.create_dynamic_frame.from_catalog(database="project_db_group_one", table_name="raw_store_csv", transformation_ctx="Store_node")
Sample_node = glueContext.create_dynamic_frame.from_catalog(database="project_db_group_one", table_name="raw_sample_submission_csv", transformation_ctx="Sample_node")

# Lectura de campso de fechas como tal
def transformar_fechas(dynamic_frame):
    df = dynamic_frame.toDF()
    df_clean = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    return DynamicFrame.fromDF(df_clean, glueContext, "df_clean")

# Aplicamos la limpieza a las tablas que tienen fecha
Test_with_dates = transformar_fechas(Test_node)
Train_with_dates = transformar_fechas(Train_node)

# 2. Tranformaciones

# Mapeo para TEST
Test_mapped = ApplyMapping.apply(frame = Test_with_dates, mappings = [
    ("id", "long", "id", "long"),
    ("store", "long", "store", "long"),
    ("dayofweek", "long", "dayofweek", "long"),
    ("date", "date", "date", "date"),             ## Origen 'date' -> Destino 'date'
    ("open", "long", "is_open", "string"),        ## Renombrado y a String
    ("promo", "long", "promo", "long"),
    ("stateholiday", "choice", "stateholiday", "string"), ## Forzamos a string
    ("schoolholiday", "long", "schoolholiday", "string") 
], transformation_ctx = "Test_mapped")

# Mapeo para TRAIN
# Nota: Usamos 'Train_with_dates' en lugar de 'Train_node'
Train_mapped = ApplyMapping.apply(frame = Train_with_dates, mappings = [
    ("store", "long", "store", "long"),
    ("dayofweek", "long", "dayofweek", "long"),
    ("date", "date", "date", "date"),   
    ("sales", "long", "sales", "long"),
    ("customers", "long", "customers", "long"),
    ("open", "long", "is_open", "string"), 
    ("promo", "long", "promo", "long"),
    ("stateholiday", "choice", "stateholiday", "string"),
    ("schoolholiday", "long", "schoolholiday", "string") 
], transformation_ctx = "Train_mapped")

# 3. ESCRIBIR A REDSHIFT

# Test
glueContext.write_dynamic_frame.from_options(frame=Test_mapped, connection_type="redshift", connection_options={
    "redshiftTmpDir": "s3://aws-glue-assets-155139033392-us-east-2/temporary/", 
    "useConnectionProperties": "true", 
    "dbtable": "project_bronze.raw_test", 
    "connectionName": "Redshift Group One", 
    "preactions": "TRUNCATE TABLE project_bronze.raw_test;"
}, transformation_ctx="InsertTest")

# Train
glueContext.write_dynamic_frame.from_options(frame=Train_mapped, connection_type="redshift", connection_options={
    "redshiftTmpDir": "s3://aws-glue-assets-155139033392-us-east-2/temporary/", 
    "useConnectionProperties": "true", 
    "dbtable": "project_bronze.raw_train", 
    "connectionName": "Redshift Group One", 
    "preactions": "TRUNCATE TABLE project_bronze.raw_train;"
}, transformation_ctx="InsertTrain")

# Store
glueContext.write_dynamic_frame.from_options(frame=Store_node, connection_type="redshift", connection_options={
    "redshiftTmpDir": "s3://aws-glue-assets-155139033392-us-east-2/temporary/", 
    "useConnectionProperties": "true", 
    "dbtable": "project_bronze.raw_store", 
    "connectionName": "Redshift Group One", 
    "preactions": "TRUNCATE TABLE project_bronze.raw_store;"
}, transformation_ctx="InsertStore")

# Sample
glueContext.write_dynamic_frame.from_options(frame=Sample_node, connection_type="redshift", connection_options={
    "redshiftTmpDir": "s3://aws-glue-assets-155139033392-us-east-2/temporary/", 
    "useConnectionProperties": "true", 
    "dbtable": "project_bronze.raw_sample_submission", 
    "connectionName": "Redshift Group One", 
    "preactions": "TRUNCATE TABLE project_bronze.raw_sample_submission;"
}, transformation_ctx="InsertSample")

job.commit()