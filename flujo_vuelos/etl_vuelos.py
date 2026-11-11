"""
Flujo ETL con Prefect – Retrasos y cancelaciones de vuelos en EE. UU.

Pasos (misma estructura del cuadernillo 401.1 de la clase):
  1. carga_datos_nuevos_local       : si hay CSV anuales en datos_nuevos/, los carga a PostgreSQL local (Docker)
  2. consulta_local                 : consulta SQL a la base local que limpia y agrega por mes, aerolínea y aeropuerto
  3. consulta_local_horas           : consulta SQL de la tasa de retraso por hora programada de salida
  4. lectura_catalogo_aerolineas    : lee el archivo de referencia con el tipo y la marca de cada aerolínea
  5. lectura_niveles_puntualidad    : lee el archivo con los límites de puntualidad
  6. lectura_registro_vuelos_analitica : lee el archivo generado en el paso 2
  7. asignacion_indicadores         : calcula tasas y asigna el nivel de puntualidad
  8. carga_aws                      : carga los resultados (no los datos originales) a PostgreSQL en AWS

Ejecución:  python etl_vuelos.py
"""
import shutil
import time
from datetime import datetime
from pathlib import Path
import os

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")          # se carga antes de Prefect para que tome PREFECT_API_URL

import numpy as np
import pandas as pd
from prefect import flow, task, get_run_logger
from sqlalchemy import Column, Date, DateTime, Float, Integer, String, create_engine, text
from sqlalchemy.orm import declarative_base

ARCHIVOS_CSV = BASE_DIR / "archivos_csv"
DATOS_NUEVOS = BASE_DIR / "datos_nuevos"
DATOS_PROCESADOS = BASE_DIR / "datos_procesados"
UMBRAL_RETRASO = 15

# Columnas del archivo original (en minúscula) y su tipo en la base local
COLUMNAS_VUELOS = {
    "fl_date": "DATE", "op_carrier": "VARCHAR(3)", "op_carrier_fl_num": "INTEGER", "origin": "VARCHAR(5)",
    "dest": "VARCHAR(5)", "crs_dep_time": "INTEGER", "dep_time": "REAL", "dep_delay": "REAL", "taxi_out": "REAL",
    "wheels_off": "REAL", "wheels_on": "REAL", "taxi_in": "REAL", "crs_arr_time": "INTEGER", "arr_time": "REAL",
    "arr_delay": "REAL", "cancelled": "REAL", "cancellation_code": "VARCHAR(1)", "diverted": "REAL",
    "crs_elapsed_time": "REAL", "actual_elapsed_time": "REAL", "air_time": "REAL", "distance": "REAL",
    "carrier_delay": "REAL", "weather_delay": "REAL", "nas_delay": "REAL", "security_delay": "REAL",
    "late_aircraft_delay": "REAL", "columna_vacia": "TEXT",
}


# ----------------------------------------------------------------------------
# Conexiones (los parámetros se leen del archivo .env)
# ----------------------------------------------------------------------------
def conexion_local():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('LOCAL_USER')}:{os.getenv('LOCAL_PASSWORD')}@{os.getenv('LOCAL_HOST')}:"
        f"{os.getenv('LOCAL_PORT')}/{os.getenv('LOCAL_DB')}", echo=False)


def conexion_aws():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('AWS_USER')}:{os.getenv('AWS_PASSWORD')}@{os.getenv('AWS_HOST')}:"
        f"{os.getenv('AWS_PORT')}/{os.getenv('AWS_DB')}", echo=False)


# ----------------------------------------------------------------------------
# Paso 1. Cargar los archivos nuevos a la base local
# ----------------------------------------------------------------------------
@task(retries=1)
def carga_datos_nuevos_local():
    log = get_run_logger()
    engine = conexion_local()
    columnas_sql = ",\n    ".join(f"{c} {t}" for c, t in COLUMNAS_VUELOS.items())
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS vuelos (\n    {columnas_sql}\n)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_vuelos_fecha ON vuelos (fl_date)"))
        conn.execute(text("""CREATE TABLE IF NOT EXISTS cargas_locales (
            archivo VARCHAR(100), anio INTEGER, filas INTEGER, fecha_carga TIMESTAMP)"""))

    archivos = sorted(DATOS_NUEVOS.glob("*.csv"))
    if not archivos:
        log.info("No hay archivos nuevos en datos_nuevos/: se recalcula la analítica con lo que ya está en la base")
        return []

    DATOS_PROCESADOS.mkdir(exist_ok=True)
    cargados = []
    for ruta in archivos:
        t0 = time.perf_counter()
        with open(ruta, encoding="utf-8") as f:
            encabezado = f.readline().strip().split(",")
        # Validación: el archivo debe traer las columnas esperadas
        nombres = [c.strip().lower() for c in encabezado]
        nombres = ["columna_vacia" if (c == "" or c.startswith("unnamed")) else c for c in nombres]
        faltan = [c for c in COLUMNAS_VUELOS if c != "columna_vacia" and c not in nombres]
        if faltan:
            raise ValueError(f"{ruta.name}: faltan las columnas {faltan}")

        with open(ruta, encoding="utf-8") as f:
            f.readline()
            primera = f.readline().split(",")
        anio = int(primera[nombres.index("fl_date")][:4])

        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            # Si el año ya existía (archivo corregido o repetido), se reemplaza: no se duplican datos
            cur.execute("DELETE FROM vuelos WHERE fl_date >= %s AND fl_date < %s", (f"{anio}-01-01", f"{anio + 1}-01-01"))
            with open(ruta, encoding="utf-8") as f:
                cur.copy_expert(f"COPY vuelos ({', '.join(nombres)}) FROM STDIN WITH (FORMAT csv, HEADER true)", f)
            cur.execute("SELECT COUNT(*) FROM vuelos WHERE fl_date >= %s AND fl_date < %s", (f"{anio}-01-01", f"{anio + 1}-01-01"))
            filas = cur.fetchone()[0]
            cur.execute("INSERT INTO cargas_locales VALUES (%s, %s, %s, %s)", (ruta.name, anio, filas, datetime.now()))
            raw.commit()
        finally:
            raw.close()

        shutil.move(str(ruta), DATOS_PROCESADOS / ruta.name)
        log.info(f"{ruta.name}: {filas:,} vuelos de {anio} cargados a la base local en {time.perf_counter() - t0:,.1f} s")
        cargados.append({"archivo": ruta.name, "anio": anio, "filas": filas})

    with engine.begin() as conn:
        conn.execute(text("ANALYZE vuelos"))
    return cargados


# ----------------------------------------------------------------------------
# Paso 2. Consulta a la base local: limpieza (reglas del EDA) y agregación
# ----------------------------------------------------------------------------
SQL_LIMPIEZA = f"""
WITH base AS (
    SELECT *,
           (COALESCE(cancelled, 0) = 0 AND COALESCE(diverted, 0) = 0) AS operado,
           distance / NULLIF(air_time, 0) * 60 AS velocidad
    FROM vuelos
),
limpio AS (
    SELECT * FROM base
    WHERE NOT (operado AND (dep_delay IS NULL OR arr_delay IS NULL OR air_time IS NULL))   -- nulos sin explicación
      AND NOT COALESCE(air_time <= 0, FALSE)                                                -- valores imposibles
      AND NOT COALESCE(air_time > actual_elapsed_time, FALSE)
      AND NOT COALESCE(velocidad > 700 OR velocidad < 100, FALSE)
      AND NOT COALESCE(dep_delay < -90, FALSE)
      AND NOT COALESCE(crs_elapsed_time <= 0, FALSE)
      AND MOD(crs_dep_time, 100) < 60
)
"""


@task
def consulta_local(archivos_cargados):
    log = get_run_logger()
    sql_query = SQL_LIMPIEZA + f"""
    SELECT DATE_TRUNC('month', fl_date)::date AS periodo,
           EXTRACT(YEAR FROM fl_date)::int    AS anio,
           EXTRACT(MONTH FROM fl_date)::int   AS mes,
           op_carrier,
           origin,
           COUNT(*)                                                          AS vuelos,
           SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)                    AS cancelados,
           SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END)                     AS desviados,
           SUM(CASE WHEN operado THEN 1 ELSE 0 END)                          AS operados,
           SUM(CASE WHEN operado AND arr_delay >= {UMBRAL_RETRASO} THEN 1 ELSE 0 END) AS retrasados,
           COALESCE(SUM(CASE WHEN operado THEN arr_delay END), 0)            AS suma_retraso_min,
           COALESCE(SUM(CASE WHEN operado THEN carrier_delay END), 0)        AS min_aerolinea,
           COALESCE(SUM(CASE WHEN operado THEN weather_delay END), 0)        AS min_clima,
           COALESCE(SUM(CASE WHEN operado THEN nas_delay END), 0)            AS min_sistema_aereo,
           COALESCE(SUM(CASE WHEN operado THEN security_delay END), 0)       AS min_seguridad,
           COALESCE(SUM(CASE WHEN operado THEN late_aircraft_delay END), 0)  AS min_avion_retrasado,
           SUM(CASE WHEN cancellation_code = 'A' THEN 1 ELSE 0 END)          AS cancel_aerolinea,
           SUM(CASE WHEN cancellation_code = 'B' THEN 1 ELSE 0 END)          AS cancel_clima,
           SUM(CASE WHEN cancellation_code = 'C' THEN 1 ELSE 0 END)          AS cancel_sistema_aereo,
           SUM(CASE WHEN cancellation_code = 'D' THEN 1 ELSE 0 END)          AS cancel_seguridad
    FROM limpio
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 4, 5
    """
    t0 = time.perf_counter()
    registro_vuelos = pd.read_sql_query(text(sql_query), con=conexion_local())
    ARCHIVOS_CSV.mkdir(exist_ok=True)
    registro_vuelos.to_csv(ARCHIVOS_CSV / "registro_vuelos_analitica.csv", index=False)
    log.info(f"Consulta local: {int(registro_vuelos['vuelos'].sum()):,} vuelos agregados en "
             f"{len(registro_vuelos):,} filas ({time.perf_counter() - t0:,.1f} s)")
    return registro_vuelos


@task
def consulta_local_horas(archivos_cargados):
    sql_query = SQL_LIMPIEZA + f"""
    SELECT EXTRACT(YEAR FROM fl_date)::int  AS anio,
           EXTRACT(MONTH FROM fl_date)::int AS mes,
           MOD(crs_dep_time / 100, 24)       AS hora_salida,
           COUNT(*)                                                          AS vuelos,
           SUM(CASE WHEN operado THEN 1 ELSE 0 END)                          AS operados,
           SUM(CASE WHEN operado AND arr_delay >= {UMBRAL_RETRASO} THEN 1 ELSE 0 END) AS retrasados,
           SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)                    AS cancelados
    FROM limpio
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """
    registro_horas = pd.read_sql_query(text(sql_query), con=conexion_local())
    registro_horas["tasa_retraso"] = registro_horas["retrasados"] / registro_horas["operados"].replace(0, np.nan) * 100
    registro_horas.to_csv(ARCHIVOS_CSV / "registro_horas_analitica.csv", index=False)
    return registro_horas


# ----------------------------------------------------------------------------
# Pasos 3 a 5. Lectura de archivos
# ----------------------------------------------------------------------------
@task
def lectura_catalogo_aerolineas():
    return pd.read_csv(ARCHIVOS_CSV / "catalogo_aerolineas.csv")


@task
def lectura_niveles_puntualidad():
    return pd.read_csv(ARCHIVOS_CSV / "niveles_puntualidad.csv")


@task
def lectura_registro_vuelos_analitica():
    return pd.read_csv(ARCHIVOS_CSV / "registro_vuelos_analitica.csv", parse_dates=["periodo"])


# ----------------------------------------------------------------------------
# Paso 6. Asignación de indicadores (equivale a la asignación de comisiones)
# ----------------------------------------------------------------------------
@task
def asignacion_indicadores(registro_vuelos, catalogo, niveles):
    log = get_run_logger()
    analitica = registro_vuelos.merge(catalogo[["op_carrier", "nombre_aerolinea", "tipo_aerolinea"]],
                                      on="op_carrier", how="left")
    sin_catalogo = analitica.loc[analitica["nombre_aerolinea"].isna(), "op_carrier"].unique().tolist()
    if sin_catalogo:
        log.warning(f"Aerolíneas sin catálogo (se marcan como 'Sin clasificar'): {sin_catalogo}")
    analitica[["nombre_aerolinea", "tipo_aerolinea"]] = analitica[["nombre_aerolinea", "tipo_aerolinea"]].fillna("Sin clasificar")

    operados = analitica["operados"].replace(0, np.nan)
    analitica["tasa_retraso"] = analitica["retrasados"] / operados * 100
    analitica["tasa_cancelacion"] = analitica["cancelados"] / analitica["vuelos"] * 100
    analitica["retraso_medio_min"] = analitica["suma_retraso_min"] / operados

    condiciones = [
        analitica["operados"] == 0,
        analitica["tasa_retraso"] < niveles.iloc[0, 1],
        (analitica["tasa_retraso"] >= niveles.iloc[0, 1]) & (analitica["tasa_retraso"] < niveles.iloc[1, 1]),
        analitica["tasa_retraso"] >= niveles.iloc[1, 1],
    ]
    valores = ["Sin vuelos operados", niveles.iloc[0, 2], niveles.iloc[1, 2], "Baja"]
    analitica["nivel_puntualidad"] = np.select(condiciones, valores, default="Sin vuelos operados")

    analitica.to_csv(ARCHIVOS_CSV / "vuelos_analitica.csv", index=False)
    log.info(f"Indicadores asignados: {analitica['nivel_puntualidad'].value_counts().to_dict()}")
    return analitica


# ----------------------------------------------------------------------------
# Paso 7. Carga de los resultados a la base de datos en AWS
# ----------------------------------------------------------------------------
Base = declarative_base()


class AnaliticaVuelos(Base):
    __tablename__ = "analitica_vuelos"
    id = Column(Integer, primary_key=True)
    periodo = Column(Date)
    anio = Column(Integer)
    mes = Column(Integer)
    op_carrier = Column(String(3))
    nombre_aerolinea = Column(String(40))
    tipo_aerolinea = Column(String(20))
    origin = Column(String(5))
    vuelos = Column(Integer)
    cancelados = Column(Integer)
    desviados = Column(Integer)
    operados = Column(Integer)
    retrasados = Column(Integer)
    suma_retraso_min = Column(Float)
    min_aerolinea = Column(Float)
    min_clima = Column(Float)
    min_sistema_aereo = Column(Float)
    min_seguridad = Column(Float)
    min_avion_retrasado = Column(Float)
    cancel_aerolinea = Column(Integer)
    cancel_clima = Column(Integer)
    cancel_sistema_aereo = Column(Integer)
    cancel_seguridad = Column(Integer)
    tasa_retraso = Column(Float)
    tasa_cancelacion = Column(Float)
    retraso_medio_min = Column(Float)
    nivel_puntualidad = Column(String(25))
    fecha_carga = Column(DateTime)


class AnaliticaHoras(Base):
    __tablename__ = "analitica_horas"
    id = Column(Integer, primary_key=True)
    anio = Column(Integer)
    mes = Column(Integer)
    hora_salida = Column(Integer)
    vuelos = Column(Integer)
    operados = Column(Integer)
    retrasados = Column(Integer)
    cancelados = Column(Integer)
    tasa_retraso = Column(Float)
    fecha_carga = Column(DateTime)


class RegistroCargas(Base):
    __tablename__ = "registro_cargas"
    id = Column(Integer, primary_key=True)
    fecha_ejecucion = Column(DateTime)
    archivos_nuevos = Column(String(200))
    anios_en_analitica = Column(String(100))
    vuelos_analizados = Column(Integer)
    filas_analitica_vuelos = Column(Integer)
    filas_analitica_horas = Column(Integer)
    duracion_s = Column(Float)


@task(retries=2, retry_delay_seconds=10)
def carga_aws(analitica, registro_horas, archivos_cargados, inicio):
    log = get_run_logger()
    engine = conexion_aws()
    Base.metadata.create_all(engine)
    ahora = datetime.now()
    columnas_vuelos = [c.name for c in AnaliticaVuelos.__table__.columns if c.name != "id"]
    columnas_horas = [c.name for c in AnaliticaHoras.__table__.columns if c.name != "id"]
    vuelos_df = analitica.assign(fecha_carga=ahora)[columnas_vuelos]
    horas_df = registro_horas.assign(fecha_carga=ahora)[columnas_horas]

    with engine.begin() as conn:      # todo o nada: si algo falla, AWS queda como estaba
        # La analítica se recalcula completa en cada ejecución: se reemplaza para no duplicar
        conn.execute(text("DELETE FROM analitica_vuelos"))
        conn.execute(text("DELETE FROM analitica_horas"))
        vuelos_df.to_sql("analitica_vuelos", conn, if_exists="append", index=False, method="multi", chunksize=2000)
        horas_df.to_sql("analitica_horas", conn, if_exists="append", index=False, method="multi", chunksize=2000)
        conn.execute(RegistroCargas.__table__.insert().values(
            fecha_ejecucion=ahora,
            archivos_nuevos=", ".join(a["archivo"] for a in archivos_cargados) or "ninguno",
            anios_en_analitica=", ".join(map(str, sorted(analitica["anio"].unique().tolist()))),
            vuelos_analizados=int(analitica["vuelos"].sum()),
            filas_analitica_vuelos=len(vuelos_df),
            filas_analitica_horas=len(horas_df),
            duracion_s=round(time.perf_counter() - inicio, 1),
        ))
    log.info(f"AWS actualizado: {len(vuelos_df):,} filas en analitica_vuelos y {len(horas_df):,} en analitica_horas")
    return len(vuelos_df)


# ----------------------------------------------------------------------------
# Flujo
# ----------------------------------------------------------------------------
@flow(name="etl-vuelos", log_prints=True)
def flujo():
    inicio = time.perf_counter()
    archivos_cargados = carga_datos_nuevos_local()
    consulta_servidor_local = consulta_local(archivos_cargados)
    consulta_horas = consulta_local_horas(archivos_cargados)
    catalogo = lectura_catalogo_aerolineas()
    niveles = lectura_niveles_puntualidad()
    lectura_registro_vuelos = lectura_registro_vuelos_analitica(wait_for=[consulta_servidor_local])
    analitica = asignacion_indicadores(lectura_registro_vuelos, catalogo, niveles)
    filas = carga_aws(analitica, consulta_horas, archivos_cargados, inicio)

    anios = sorted(analitica["anio"].unique().tolist())
    tasa = analitica["retrasados"].sum() / analitica["operados"].sum() * 100
    print(f"Flujo terminado en {time.perf_counter() - inicio:,.1f} s | archivos nuevos: "
          f"{[a['archivo'] for a in archivos_cargados] or 'ninguno'} | años en la analítica: {anios} | "
          f"tasa de retraso: {tasa:.2f} % | filas en AWS: {filas:,}")
    return analitica


if __name__ == "__main__":
    flujo()
