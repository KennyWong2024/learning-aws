# Práctica Novashop: Pipeline ETL con AWS Glue y Athena

## Objetivo

Implementar un flujo de datos (Pipeline ETL) utilizando servicios de AWS para procesar información de ventas y productos. El objetivo es catalogar archivos CSV crudos, transformarlos y unificarlos usando AWS Glue (PySpark) para generar una tabla optimizada en formato Parquet, y finalmente realizar análisis de negocio mediante consultas en AWS Athena.

## Arquitectura de Datos
Los datos se organizan en Amazon S3 bajo la siguiente estructura:

- **Raw Data:** *s3://group-one-project-uh/practices_group/novashop/raw/* (Archivos CSV originales).

- **Curated Data:** *s3://group-one-project-uh/practices_group/novashop/curated/* (Datos procesados en Parquet).

## Ingesta y Catalogación (Raw Zone)

#### Creación de Crawlers

Utilizando la consola de AWS Glue, se creó el crawler **group-one-novashop** para catalogar la estructura de los archivos *.csv* ubicados en la ruta raw.

#### Validación y Ajuste de Ingesta (Visual ETL)

Inicialmente, tras ejecutar el crawler, las consultas en Athena no devolvieron resultados (tablas vacías).

```sql
SELECT * FROM "novashop_db_one"."raw_novashop_products_csv" limit 10;
SELECT * FROM "novashop_db_one"."raw_novashop_sales_csv" limit 10;
```

**Solución:** Para garantizar la disponibilidad de los datos en el Glue Data Catalog, se implementó un Job Visual llamado **group-one-novashop**. Este trabajo extrae los datos explícitamente de S3 y los inserta en las tablas del catálogo.

Tras la ejecución exitosa del Job, se validó la integridad de los datos mediante un JOIN de prueba:

```sql
SELECT
    A.*,
    B.*
FROM "novashop_db_one"."raw_novashop_sales_csv" AS A
LEFT JOIN "novashop_db_one"."raw_novashop_products_csv" AS B
ON A.product_id = B.product_id
```

## Transformación con PySpark (Script Editor)

Para la fase de transformación y curación de datos, se optó por utilizar el Script Editor de AWS Glue Studio con Spark.

#### Estructura del Script
El script sigue las mejores prácticas de AWS Glue, encapsulando la lógica de transformación entre la inicialización y la confirmación del Job.

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## NOTA: Para que Glue procese correctamente el código y mantenga el estado,
## una buena práctica es colocar toda la lógica ETL entre job.init() y job.commit()

# ... [Lógica de extracción, Join, cálculos y escritura a Parquet] ...

job.commit()
```

#### Ejecución y Depuración

Durante el desarrollo del script PySpark se realizaron varias iteraciones para ajustar las transformaciones (Joins y métricas calculadas) y asegurar la escritura correcta en formato .parquet particionado.

![alt text](..\images\novashop_runs_pyspark_script.png)

## Capa Curada y Analítica (Curated Zone)

Una vez generado el archivo unificado en formato Parquet en la ruta s3://.../novashop/curated/, se procedió a disponibilizar esta tabla para análisis.

#### Definición de Tabla en Athena
Se creó la tabla externa novashop_db.sales_curated apuntando a los datos procesados.

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS sales_curated (
  order_id bigint,
  order_date string,
  store_id string,
  city string,
  customer_id bigint,
  product_id bigint,
  qty int,
  unit_price double,
  discount_pct double,
  payment_method string,
  channel string,
  product_name string,
  category string,
  brand string,
  unit_price_catalog double,
  unit_cost double,
  subtotal double,
  discount_amount double,
  total double,
  cogs double,
  profit double
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://group-one-project-uh/practices_group/novashop/curated/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

Luego es necesario ejecutar lo siguiente
```sql
MSCK REPAIR TABLE sales_curated
```

Fuente de guia para DDL: https://repost.aws/es/knowledge-center/athena-create-use-partitioned-tables

#### Evidencia de Base de Datos
En la siguiente captura se observan las tablas originales (csv) y la tabla final unificada (curated).

#### Resultados del Análisis de Negocio
A continuación, se presentan las consultas SQL realizadas en AWS Athena para responder a las preguntas de negocio planteadas.

1) Ventas totales y margen bruto (suma de total y de profit) del periodo


2) Ticket promedio (promedio de total por orden)


3) Top 5 categorías por ventas.


4) Tendencia mensual de ventas y margen


5) Ciudades con mejor rendimiento por ventas


6) Marca con mayor contribución a la utilidad


7) Método de pago: distribución de ventas y conteo de órdenes


8) Canal (Tienda vs Online): comparación de ventas, utilidad y ticket promedio


9) Clientes únicos y tasa de recompra (clientes con más de 1 orden)


10) Descuento total aplicado y su efecto en el margen (profit/ventas por nivel de descuento)

