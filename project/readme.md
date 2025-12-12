# Proyecto Final - Data Warehouse

## Paso 1: Carga de datos crudos
Carga de data en el S3, a continuación el URI

```bash
s3://group-one-project-uh/project/raw/
```

## Paso 2: Catalogación (Glue Catalog)
Creación de crawlers con el fin de crear los catálogos en glue y tenerlos listos en la database (Glue Catalog) **project_db_group_one**

- Creación del Crawler
![alt text](../images/project_01.png)

- Creación de Base de datos y definición de catalogos (Glue Catalog)
![alt text](../images/project_02.png)

- Validación en Athena
![alt text](../images/project_03.png)

## Paso 3: Preparación de Redshift
Creación de tablas para data cruda, haremos un *ELT*, esto debido a que hay datos problematicos que primero queremos entender y probar apra dar tratamiento,
aprovecharemos la potencia de **Redshift** para utilziarla como *Lakehouse*, no tenemos acceso por temas academicos a soluciones como **DBT** pero simularemos
una orquestación de datos utilizando prefijos ordenados para entedner el proceso

### Bronze Layer
```sql
CREATE TABLE "group_one"."project_bronze"."raw_sample_submission" (
    id BIGINT,
    sales BIGINT
);

CREATE TABLE "group_one"."project_bronze"."raw_store"(
  store BIGINT, 
  storetype VARCHAR(100), 
  assortment VARCHAR(100), 
  competitiondistance BIGINT, 
  competitionopensincemonth BIGINT, 
  competitionopensinceyear BIGINT, 
  promo2 BIGINT, 
  promo2sinceweek BIGINT, 
  promo2sinceyear BIGINT, 
  promointerval VARCHAR(100)
);

CREATE TABLE "group_one"."project_bronze"."raw_test" (
  id BIGINT, 
  store BIGINT, 
  dayofweek BIGINT, 
  date DATE, 
  is_open VARCHAR(50),
  promo BIGINT, 
  stateholiday VARCHAR(50),
  schoolholiday VARCHAR(50) 
);

CREATE TABLE "group_one"."project_bronze"."raw_train" (
  store BIGINT, 
  dayofweek BIGINT, 
  date DATE, 
  sales BIGINT, 
  customers BIGINT, 
  is_open VARCHAR(50),
  promo BIGINT, 
  stateholiday VARCHAR(50),
  schoolholiday VARCHAR(50)
);
```

Una vez definidos las tabals de destino mcargaremos la información mediante Glue utilziando Spark, el código de carga se encuentra en **project\glue\01. EL Job.py**