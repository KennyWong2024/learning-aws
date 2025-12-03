## DDL Tablas
DDL de las tablas en la que insertaremos la información utilizando **COPY** from *csv* desde **S3**

### Areas
```sql
CREATE TABLE global_supplies.areas (
    area VARCHAR(50) NOT NULL, 
    supervisor VARCHAR(100),
    PRIMARY KEY (area)
);
```

### Empleados
```sql
CREATE TABLE global_supplies.empleados (
    id_empleado INT NOT NULL,
    nombre VARCHAR(100),
    turno VARCHAR(50),
    pais VARCHAR(50),
    PRIMARY KEY (id_empleado)
);
```

### Productos
```sql
CREATE TABLE global_supplies.productos (
    id_producto INT NOT NULL,
    nombre_producto VARCHAR(100),
    tipo_producto VARCHAR(100),
    PRIMARY KEY (id_producto)
);
```

### Movimientos
```sql
CREATE TABLE global_supplies.movimientos (
    id_movimiento INT NOT NULL,
    id_empleado INT,
    id_producto INT,
    fecha DATE,
    hora TIME,
    cantidad INT,
    area VARCHAR(50),
    PRIMARY KEY (id_movimiento),
    FOREIGN KEY (id_empleado) REFERENCES global_supplies.empleados(id_empleado),
    FOREIGN KEY (id_producto) REFERENCES global_supplies.productos(id_producto),
    FOREIGN KEY (area) REFERENCES global_supplies.areas(area)
);
```

## Ingesta de Datos
Insertaremos los datos directos de **S3** con **COPY**

### Areas
```sql
COPY global_supplies.areas 
FROM 's3://group-one-project-uh/practices_group/global_supplies/raw/areas.csv' 
IAM_ROLE 'arn:aws:iam::155139033392:role/redshiftuh' 
FORMAT AS CSV 
DELIMITER ';' 
QUOTE '"' 
IGNOREHEADER 1
REGION AS 'us-east-2';
```

### Empleados
```sql
COPY global_supplies.empleados 
FROM 's3://group-one-project-uh/practices_group/global_supplies/raw/empleados.csv' 
IAM_ROLE 'arn:aws:iam::155139033392:role/redshiftuh' 
FORMAT AS CSV 
DELIMITER ';' 
QUOTE '"' 
IGNOREHEADER 1
REGION AS 'us-east-2';
```

### Productos
```sql
COPY global_supplies.productos 
FROM 's3://group-one-project-uh/practices_group/global_supplies/raw/productos.csv' 
IAM_ROLE 'arn:aws:iam::155139033392:role/redshiftuh' 
FORMAT AS CSV 
DELIMITER ';' 
QUOTE '"' 
IGNOREHEADER 1
REGION AS 'us-east-2';
```

### Movimientos
```sql
COPY global_supplies.movimientos 
FROM 's3://group-one-project-uh/practices_group/global_supplies/raw/movimientos.csv' 
IAM_ROLE 'arn:aws:iam::155139033392:role/redshiftuh' 
FORMAT AS CSV 
DELIMITER ';' 
QUOTE '"' 
IGNOREHEADER 1
REGION AS 'us-east-2';
```


## Consultas Requeridas

1 ¿Qué empleado tiene más movimientos 
registrados? 
```sql

```

2 ¿Qué producto fue movido más veces? 
```sql

```

3 ¿En qué día se registraron más 
movimientos?
```sql

```

4 ¿Qué área tuvo la mayor cantidad total 
movida?
```sql

```

5 ¿Qué turno tiene más empleados?
```sql

```

6 ¿Qué país tiene más empleados? 
```sql

```

7 ¿Qué tipo de producto es más frecuente 
en inventario?
```sql

```

8 ¿Qué supervisor tiene más áreas 
asignadas?
```sql

```

9 ¿Qué empleados han movido más de 
10 productos en total?
```sql

```

10 ¿Qué hora tuvo más movimientos 
registrados? 
```sql

```