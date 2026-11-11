# Flujo ETL con Prefect – Retrasos de vuelos (puntos d y f)

Estructura igual a la del cuadernillo 401.1 de la clase: una base PostgreSQL local en Docker simula el servidor de la compañía; un script de Python con Prefect consulta esa base, transforma la información y carga **solo los resultados** en una base PostgreSQL en AWS.

```
CSV anuales ──► PostgreSQL local (Docker) ──► Prefect: consulta → lectura → asignación ──► PostgreSQL en AWS ──► Grafana (Docker)
                (tabla vuelos, 18,5 M filas)                                              (tablas de analítica)   (tablero gerencial)
```

## Contenido de la carpeta

| Archivo | Para qué |
|---|---|
| `etl_vuelos.py` | El flujo de Prefect (se ejecuta con `python etl_vuelos.py`) |
| `docker-compose.yml` | Levanta PostgreSQL local, la interfaz de Prefect y Grafana |
| `grafana/` | Conexión de Grafana a AWS y el tablero gerencial (se cargan solos al iniciar Grafana) |
| `.env.ejemplo` | Plantilla de las conexiones; se copia como `.env` |
| `requirements.txt` | Librerías necesarias |
| `archivos_csv/catalogo_aerolineas.csv` | Archivo de referencia: nombre, tipo y marca de cada aerolínea |
| `archivos_csv/niveles_puntualidad.csv` | Archivo de referencia: límites para clasificar la puntualidad (como `comisiones.csv` en clase) |
| `datos_nuevos/` | Aquí se copian los CSV que deben entrar a la base |
| `datos_procesados/` | El flujo mueve aquí los CSV ya cargados |

## Paso 1. Instalar las librerías (una sola vez)

Abra la carpeta `flujo_vuelos` en VS Code y, en la terminal, con el entorno activado (`conda activate bigdata_env`):

```
pip install -r requirements.txt
```

## Paso 2. Crear la base de datos en AWS (una sola vez)

En la consola de AWS:

1. Entre a **RDS → Create database**.
2. Elija **Standard create**, motor **PostgreSQL** y plantilla **Free tier**.
3. **DB instance identifier:** `vuelos-analitica`. **Master username:** `postgres`. Defina una contraseña y guárdela.
4. En **Connectivity**, marque **Public access: Yes** y cree un grupo de seguridad nuevo.
5. En **Additional configuration**, escriba en **Initial database name:** `vuelos_analitica`.
6. Pulse **Create database** y espere unos minutos hasta que el estado sea *Available*.
7. Abra la base, copie el **Endpoint** y, en el grupo de seguridad (pestaña *Connectivity & security*), agregue una regla de entrada **PostgreSQL, puerto 5432, origen My IP**.

Pruebe la conexión desde DBeaver con el endpoint, el puerto 5432, el usuario `postgres`, la contraseña y la base `vuelos_analitica`.

> Revise que la instancia quede en la capa gratuita y elimínela cuando el curso termine para evitar cobros.

## Paso 3. Configurar las conexiones

Copie `.env.ejemplo` como `.env` y complete:

- `LOCAL_PASSWORD`: una clave cualquiera para el PostgreSQL local.
- `AWS_HOST`: el endpoint de RDS. `AWS_PASSWORD`: la contraseña de RDS.

El archivo `.env` está en `.gitignore`: las contraseñas no se suben a GitHub.

## Paso 4. Levantar PostgreSQL local y la interfaz de Prefect

Con Docker Desktop abierto, en la terminal de VS Code:

```
docker compose up -d
```

- PostgreSQL local queda en `localhost:55432` (base `vuelos_local`). Puede verla en DBeaver.
- La interfaz de Prefect queda en http://localhost:4200.

## Paso 5. Demostración: ejecución inicial (2016 y 2017)

Copie `2016.csv` y `2017.csv` en `datos_nuevos/` y ejecute:

```
python etl_vuelos.py
```

El flujo carga los dos años a la base local (tabla `vuelos`), calcula la analítica y crea en AWS las tablas `analitica_vuelos`, `analitica_horas` y `registro_cargas`. Tome capturas de:

- La terminal con el mensaje `Flujo terminado ...`.
- La interfaz de Prefect: **Runs → (la ejecución) → Graph**.
- DBeaver mostrando la tabla `analitica_vuelos` en AWS.

## Paso 6. Demostración: llegan datos nuevos (2018)

Copie `2018.csv` en `datos_nuevos/` y ejecute otra vez `python etl_vuelos.py`. El flujo detecta el archivo, lo agrega a la base local, recalcula todo y actualiza AWS. En `registro_cargas` (AWS) queda una fila por ejecución con los archivos nuevos y los años incluidos.

## Paso 7. Ejecución sin datos nuevos

Ejecute una tercera vez sin archivos en `datos_nuevos/`. El flujo no carga nada a la base local y vuelve a publicar la analítica en AWS.

## Paso 8. Tablero en Grafana (punto g)

1. En su `.env` agregue la línea `GRAFANA_PASSWORD=` con una clave para entrar a Grafana (usuario `admin`).
2. Con Docker Desktop abierto, en la carpeta `flujo_vuelos`:

```
docker compose up -d
```

   Docker descarga Grafana la primera vez (unos minutos) y deja los otros contenedores como están.
3. Abra http://localhost:3000, entre con `admin` y su clave. El tablero **Retrasos de vuelos en EE. UU. – Tablero gerencial** abre como página de inicio (también está en *Dashboards → Vuelos*).

Grafana se conecta solo a la base de AWS con los datos del `.env` (archivo `grafana/provisioning/datasources/aws_postgres.yml`) y lee únicamente las tablas de resultados. Cada vez que el flujo de Prefect actualiza AWS, basta con recargar el tablero; los filtros de año y aerolínea toman los valores nuevos solos.

| Panel | Pregunta que responde |
|---|---|
| Vuelos analizados, % con retraso, % cancelados, retraso medio | ¿Cómo estamos en general? (el % de retraso se pinta verde, amarillo o rojo con los límites de `niveles_puntualidad.csv`) |
| % de retraso por mes | ¿Mejora o empeora con el tiempo? |
| Minutos de retraso por causa | ¿Qué parte del retraso depende de la aerolínea? |
| % de retraso por aerolínea | ¿Qué aerolíneas están por encima de los límites? |
| % de retraso por hora de salida | ¿A qué horas conviene reforzar la operación? |
| % de retraso por mes del año | ¿En qué temporada se concentra el retraso? |
| 15 aeropuertos con más vuelos | ¿Dónde se concentran los problemas? |
| Ejecuciones del flujo | ¿Cuándo se actualizaron los datos por última vez? |

Si el gráfico mensual queda vacío al llegar un año nuevo, amplíe el rango de fechas arriba a la derecha (viene fijo de 2016 a 2018).

## Tablas que quedan en AWS

| Tabla | Contenido | Uso |
|---|---|---|
| `analitica_vuelos` | Una fila por mes, aerolínea y aeropuerto de origen: vuelos, cancelados, retrasados, minutos por causa, tasas y nivel de puntualidad | Grafana y conclusiones |
| `analitica_horas` | Tasa de retraso por año, mes y hora programada de salida | Gráfico de hora por mes en Grafana |
| `registro_cargas` | Una fila por ejecución del flujo | Trazabilidad de las ejecuciones |

## Programar el flujo (opcional)

Para que corra solo el primer día de cada mes a las 6:00, cambie la última línea de `etl_vuelos.py` por:

```python
if __name__ == "__main__":
    flujo.serve(name="etl-vuelos-mensual", cron="0 6 1 * *")
```

## Si algo falla

| Mensaje | Qué revisar |
|---|---|
| `connection refused` en el puerto 55432 | Docker Desktop abierto y `docker compose up -d` ejecutado |
| `timeout` o `could not connect` hacia AWS | La regla de entrada del grupo de seguridad (puerto 5432, My IP) y que la base tenga acceso público |
| No se conecta a `127.0.0.1:4200` | El contenedor `prefect-server` está corriendo; si no quiere usar la interfaz, ponga `#` al inicio de `PREFECT_API_URL` en `.env` |
| `faltan las columnas` | El CSV no tiene la estructura del dataset original |
| Grafana muestra *No data* o un triángulo rojo en los paneles | *Connections → Data sources → AWS vuelos_analitica → Save & test*. Si falla, revise `AWS_HOST`, `AWS_PASSWORD` y `AWS_DB` en `.env` y reinicie con `docker compose up -d --force-recreate grafana` |
| La clave de AWS tiene el signo `$` | En `.env` escríbalo doble (`$$`), porque Docker lo interpreta como variable |
| Puerto 3000 ocupado | Cambie `"3000:3000"` por `"3001:3000"` en `docker-compose.yml` y entre por http://localhost:3001 |
