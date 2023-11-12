# Test Data Engineer awto


## Primer paso

### Levantar Entorno
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
Señale (sin necesidad de implementar) qué procesos podría desarrollar para asegurar la consistencia de los datos en la tabla resumen_diario.
Procesos y/o mejoras (pensando que estamos onpremises)
1. Transacciones Atómicas:
Asegúrate de que todas las operaciones que modifican los datos de la tabla resumen_diario estén dentro de transacciones atómicas. Esto garantizará que todas las operaciones se completen correctamente o que no tengan ningún efecto.
2. Bloqueo de Tabla:
Antes de ejecutar el proceso ETL, considera utilizar un bloqueo de tabla (LOCK TABLE) para evitar que otras transacciones interfieran con la actualización de la tabla durante el proceso.
3. Procedimiento Almacenado:
Considera encapsular la lógica del proceso ETL en un procedimiento almacenado en la base de datos. Los procedimientos almacenados pueden simplificar el manejo de transacciones y asegurar la consistencia de datos.
4. Registro de Errores:
Implementa un sistema de registro de errores para registrar cualquier problema durante el proceso ETL. Si algo sale mal, tendrás un registro detallado para investigar y corregir los problemas.
5. Backup Regular:
Realiza copias de seguridad regulares de la base de datos antes de ejecutar el proceso ETL. Esto proporciona un punto de recuperación en caso de que algo salga mal.
6. Restricciones de Integridad:
Utiliza restricciones de integridad en la base de datos para asegurar que ciertas condiciones se cumplan. Por ejemplo, puedes agregar restricciones de clave única o verificar restricciones en las columnas que deben cumplir ciertos criterios.
7. Auditoría:
Implementa un sistema de auditoría para realizar un seguimiento de las modificaciones en la tabla resumen_diario. Esto puede ayudar a identificar y corregir problemas de consistencia.
8. Validaciones Pre-Ejecución:
Antes de ejecutar el proceso ETL, realiza validaciones previas para asegurarte de que los datos de entrada sean coherentes y cumplan con los requisitos esperados.
9. Uso de Triggers:
Considera el uso de triggers para automatizar ciertas acciones antes o después de las operaciones en la tabla resumen_diario. Por ejemplo, puedes usar un trigger BEFORE INSERT para validar datos antes de la inserción.
10. Monitoreo Continuo:
Establece un sistema de monitoreo continuo para supervisar el rendimiento y la integridad de la base de datos. Esto puede incluir alertas automáticas en caso de errores o desviaciones inesperadas, tratar de realizar una conexion a una herramienta como Grafana u otra a nivel de monitoreo.
11. Alarmas:
Implementar alarmas en caso que un proceso falle, asi se puede saber en el momento y no esperar hasta el final.
<br />
<br />

Si estuvieramos en BigQuery, lo mismo seria:

1. Transacciones en BigQuery:
BigQuery no admite transacciones en el sentido tradicional, ya que está diseñado para consultas analíticas a gran escala. Sin embargo, las operaciones individuales son atómicas. Puedes organizar tu proceso de ETL de manera que las operaciones críticas (como la actualización de datos en la tabla resumen_diario) se realicen en pasos separados y puedan ser revertidas en caso de errores.
2. Jobs Programados (Scheduler):
Usa la funcionalidad de programación de trabajos (jobs) en BigQuery para ejecutar el proceso ETL de manera programada y automática a las 23:59 horas todos los días.
3. Partitioning (costos) y Clustering :
Aprovecha las capacidades de particionamiento y clustering en BigQuery. Puedes particionar la tabla resumen_diario por fecha para facilitar la administración de datos diarios y mejorar el rendimiento de las consultas.
4. Operaciones Atómicas con Copia de Tabla:
Realiza operaciones de copia de tabla para aplicar cambios atómicamente. Puedes realizar una copia de la tabla existente, aplicar las transformaciones necesarias en la nueva copia y luego reemplazar la tabla original con la copia. Esto ayuda a garantizar que la tabla resumen_diario se actualice de manera coherente
5. Auditoría y Registro de Cambios:
Utiliza la funcionalidad de auditoría y registro de cambios en BigQuery para realizar un seguimiento de las modificaciones en la tabla resumen_diario. Puedes consultar los registros de auditoría para identificar y corregir problemas de consistencia.
6. Validaciones y Limpieza de Datos:
Realiza validaciones de datos antes de ejecutar el proceso ETL. Puedes usar consultas SQL para identificar y corregir posibles problemas de datos antes de cargarlos en la tabla resumen_diario.
7. so de Cloud Functions:
Si necesitas ejecutar procesos adicionales o realizar acciones específicas antes o después del proceso ETL, considera integrar Google Cloud Functions en tu flujo de trabajo.
8. Alertas y Monitoreo:
Configura alertas para recibir notificaciones en caso de que haya errores o desviaciones inesperadas durante el proceso ETL. Puedes usar Google Cloud Monitoring para monitorear la salud de tu sistema.
9. Backup y Snapshots:
Realiza copias de seguridad periódicas o crea snapshots de tu dataset en BigQuery antes de ejecutar el proceso ETL. Esto proporcionará un punto de recuperación en caso de problemas.

<br />
<br />

La empresa quiere implementar un sistema de descuentos mediante cupones. ¿Cómo modificarías el modelo de datos para agregarlo? Describa su propuesta, justifique y explique por qué es la mejor opción. No es necesario que lo implemente. (teniendo en cuenta que todo esta en una tabla llamada trips)

Creamos la tabla copones donde tendremos la informacion de los cupones disponibles, mismo caso, podriamos generar mas datos pero al aplicar las 3 FN se crearian mas tablas:
```sql
CREATE TABLE coupons (
    coupon_id SERIAL PRIMARY KEY,
    coupon_code VARCHAR(20) UNIQUE,
    discount_percentage NUMERIC, -- Porcentaje de descuento del cupón
    expiration_date DATE -- Fecha de vencimiento del cupón
);
```

Creamos la tabla de uso de cupones para tener informacion sobre el uso de los cupones en los viajes.
```sql
CREATE TABLE coupon_usage (
    coupon_usage_id SERIAL PRIMARY KEY,
    trip_id INTEGER UNIQUE REFERENCES trips(trip_id),
    coupon_id INTEGER REFERENCES coupons(coupon_id)
);
```

Modificamos la tabla trips para saber quien aplico el cupon de descuento
```sql
ALTER TABLE trips
ADD COLUMN discount_amount NUMERIC DEFAULT 0;
```

Justificacion
1. Separamos responsablidades entre los datos de los viajes y los datos relacionados a los cupones, con esto ganamos mantenimiento y gestion de datos, si aplicaramos el modelo de datos que tenemos arriba, se tendria seria el mismo principio.
2. Ganamos escalabilidad ingresando diferentes tipos de cupones y descuentos en caso que existan a futuros, ejemplos: descuentos fijos, con porcentajes, por tiempo limitado, convenios con medios de pago, etc).
3. Tenemos una mayor integridad en base a las FK de las tablas.
4. Seguimiento mas claro de quien y cuando se aplicaron los cupones.
5. Adaptabilidad en base a futuros requisitos, en caso que se cambie el sistema de cupones no afecta a la data principal.
6. Tenemos consistencia y claridad en base a cuanto fue el monto especifico del cupon y en que viaje.
7. Nos permite generar querys mas simples en base a Joins ya que tenemos separada la data.

Si nos ponemos a pensar esta seria mejor opcion siempre y cuando se aplique de la misma forma a nivel de Postgres, si lo llevamos a un dataset en GCP, lo ideal seria generar tablas separadas como se aprecia en el modelo de datos anteriormente generado, teniendo la data tal como se extrae y generando procesos schedulados para genrar y llegar con mayor precion a la capa de visualizacion pensando en una mirada de Datalake.
(https://github.com/bastyy/de_awto_test/assets/31254863/8496f27a-6de8-4a96-861f-0164e914d484)

<br />

# Fin 
Muchas gracias, me gustaria saber como poder conectar mi host (dns local IPV4) a la misma red de Docker (postgres).

Gracias. :)
