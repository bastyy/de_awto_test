# Test Data Engineer awto

Levantar Entorno:
<br />
La base de datos Postgres en Docker, en este ejemplo se uso por medio de un docker-compose lo cual se adjunto en el repo.

```
version: '3.1'

services:

  db:
    image: postgres
    restart: always
    environment:
      POSTGRES_PASSWORD: example

  adminer:
    image: adminer
    restart: always
    ports:
      - 8080:8080

```
<br />
<br />

Codigo para levantar el contenedor:
<br />
```
docker-compose -f docker-compose.yml up
```
<br />
<br />

## Respuesta a lo solicitado
<br />
1.- Diseñar un modelo de datos. Genere una propuesta sobre cómo guardar los datos. Justifique esa propuesta y explique por qué es la mejor opción.
<br />

Dentro del modelo de datos lo ideal seria separar la data en tablas aplicando las 3 Formas Normales dando como resultado la siguiente sintaxis SQL.
<br />
```sql
-- Crear la tabla de vehículos
CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY
    -- Aca se puede agregar otros campos relacionados con los vehículos si es necesario como por ejemplo 
    -- caracteristicas lo que nos llevaria a generar mas tablas como por ejemplo la marca.
);

-- Crear la tabla de membresia
CREATE TABLE memberships (
    membership_id SERIAL PRIMARY KEY
    -- Mismo caso como la tabla de vehiculos, se puede agregar otros campos relacionados con las membresia
    -- tipo, nombre, inicio, valor, pero conlleva a lo mismo, el generar otra tabla para ir aplicando
    -- las 3 formas normales.
);

-- Crear la tabla de estados de viaje
CREATE TABLE trip_status (
    status_id SERIAL PRIMARY KEY
    -- Mismo caso, se puede tener el nombre o el detalle del status del viaje puesto que estariamos guardando
    -- solo la PK.
);

-- Crear la tabla de usuarios
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name_user VARCHAR(255),
    rut_user VARCHAR(20),
    membership_id INTEGER UNIQUE REFERENCES memberships(membership_id)
);

-- Crear la tabla de viajes
CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    vehicle_id INTEGER REFERENCES vehicles(vehicle_id),
    booking_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status_id INTEGER REFERENCES trip_status(status_id),
    travel_dist INTEGER,
    membership_id INTEGER REFERENCES memberships(membership_id),
    price_amount NUMERIC,
    price_tax NUMERIC,
    price_total NUMERIC,
    start_lat NUMERIC,
    start_lon NUMERIC,
    end_lat NUMERIC,
    end_lon NUMERIC
);
```
<br />
Aca se normalizo el CSV entregado, llevandolo a un modelo relacional, se realizo una integridad de datos en
<br />
base a las PK y FK para tener una mayor referenciacion de que significa cada status o id
<br />
Al separar las tablas permite que el input de data sea mas facil y no afecte al modelo completo, permitiendo
<br />
realizar querys de manera mas simples y con mejor dendimiento y por ultimo se entrega un modelo escalable
<br />
en donde se pueden agregar mas tablas.
<br />
<br />
<br />
2.- Crea una base de datos en Postgres usando Docker.
<br />
Aca la base de datos se creo y se llama postgres (que viene por defecto en docker)
<br />
![image](https://github.com/bastyy/de_awto_test/assets/31254863/02195dc1-4e91-4793-a184-758deeab5509)
<br />
<br />
3.- Crea las tablas del modelo de datos que diseñaste en el paso 1. Puede usar scripts SQL o código en Python.
<br />
El codigo sql se encuentra mas arriba.
<br />
<br />
4.- Genera archivos en Python para cargar los datos del archivo trips.csv en las tablas que creaste en el paso anterior.
<br />
El script en Python es el siguiente:


```python
import csv
import psycopg2
from psycopg2 import sql

# Conexión a la base de datos
conn = psycopg2.connect(
    host="tu_host",
    port=tu_puerto,
    database="tu_base_de_datos",
    user="usuario",
    password="contraseña"
)

# Crear un cursor
cur = conn.cursor()

# Ruta del archivo CSV
csv_file_path = "Ruta_de_tu_archicvo/trips.csv"

# Nombre de las tablas
table_names = ["users", "vehicles", "memberships", "trip_statuses", "trips"]

# Cargar datos desde el archivo CSV a las tablas
for table_name in table_names:
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        
        # Preparar la inserción dinámica
        columns = reader.fieldnames
        columns_str = ', '.join(columns)
        values_str = ', '.join(['%({})s'.format(column) for column in columns])
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(columns_str),
            sql.SQL(values_str)
        )

        # Ejecutar la inserción
        cur.executemany(insert_query, reader)

# Confirmar los cambios y cerrar la conexión
conn.commit()
cur.close()
conn.close()

```
# Aca no se pudo realizar la conexion por problemas de DNS y Networking, ya que el contenedor esta en una red distinta al de mi DNS :(

5.- Cree una nueva tabla en Postgres llamada resumen_diario.
<br />
Para esto se genero el siguiente codigo en Python:
<br />
```python
import csv
import psycopg2
from datetime import datetime, date
from collections import defaultdict

# Conexión a la base de datos
conn = psycopg2.connect(
    host="tu_host",
    database="tu_base_de_datos",
    user="tu_usuario",
    password="tu_contraseña"
)

# Crear un cursor
cur = conn.cursor()

# Ruta del archivo CSV
csv_file_path = "ruta_del_archivo.csv"

# Diccionario para almacenar datos resumen_diario
resumen_diario_data = defaultdict(lambda: {'cantidad_viajes': 0, 'suma_ingresos': 0, 'suma_distancia_km': 0})

# Leer el archivo CSV y realizar el procesamiento
with open(csv_file_path, 'r') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        # Convertir fechas de texto a objetos de fecha
        booking_time = datetime.strptime(row['booking_time'], '%Y-%m-%d %H:%M:%S')
        
        # Extraer información de la fecha
        fecha = booking_time.date()

        # Actualizar datos en el diccionario resumen_diario
        resumen_diario_data[fecha]['cantidad_viajes'] += 1
        resumen_diario_data[fecha]['suma_ingresos'] += float(row['price_total'])
        resumen_diario_data[fecha]['suma_distancia_km'] += float(row['travel_dist'])

# Calcular el promedio de ingresos
for fecha, data in resumen_diario_data.items():
    data['promedio_ingresos'] = data['suma_ingresos'] / data['cantidad_viajes'] if data['cantidad_viajes'] > 0 else 0

# Insertar datos en la tabla resumen_diario
for fecha, data in resumen_diario_data.items():
    cur.execute(
        "INSERT INTO resumen_diario (fecha, cantidad_viajes, suma_ingresos, promedio_ingresos, suma_distancia_km) "
        "VALUES (%s, %s, %s, %s, %s)",
        (fecha, data['cantidad_viajes'], data['suma_ingresos'], data['promedio_ingresos'], data['suma_distancia_km'])
    )

# Confirmar los cambios y cerrar la conexión
conn.commit()
cur.close()
conn.close()

```
<br />
Ahora expliquemos el codigo
<br />
Conexion a la base de datos postgres:
```python
conn = psycopg2.connect(
    host="tu_host",
    database="tu_base_de_datos",
    user="tu_usuario",
    password="tu_contraseña"
)

```
Se crea un cursos para que recorra la tabla
```python
cur = conn.cursor()
```
Le decimos donde esta el archivo
```python
csv_file_path = "ruta_del_archivo.csv"
```
Generamos un doccionario para almacenar datos temporales
```python
resumen_diario_data = defaultdict(lambda: {'cantidad_viajes': 0, 'suma_ingresos': 0, 'suma_distancia_km': 0})
```
Abrinos y leemos el CSV 
```python
with open(csv_file_path, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # ... (ver siguiente punto)
```
Procesamos cada fila del CSV pero nos enfocamos principalmente en el booking_time y la extraimos
```python
booking_time = datetime.strptime(row['booking_time'], '%Y-%m-%d %H:%M:%S')
fecha = booking_time.date()
```
Calculamos el promedio para cada fecha del diccionario
```python
for fecha, data in resumen_diario_data.items():
    data['promedio_ingresos'] = data['suma_ingresos'] / data['cantidad_viajes'] if data['cantidad_viajes'] > 0 else 0
```
Y para finalizar insermaos los datos:
```python
for fecha, data in resumen_diario_data.items():
    cur.execute(
        "INSERT INTO resumen_diario (fecha, cantidad_viajes, suma_ingresos, promedio_ingresos, suma_distancia_km) "
        "VALUES (%s, %s, %s, %s, %s)",
        (fecha, data['cantidad_viajes'], data['suma_ingresos'], data['promedio_ingresos'], data['suma_distancia_km'])
    )
```






```
docker-compose -f docker-compose.yml up
```




