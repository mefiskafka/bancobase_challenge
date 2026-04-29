"""ETL Ecobici GBFS Challenge - Airflow + MinIO + Polars + Trino.

Pipeline (cada 10 minutos):
  1. fetch_gbfs_station_status: GET al feed GBFS de Ecobici CDMX (station_status).
  2. clean_and_transform: limpieza + tipos + columnas derivadas con Polars.
  3. append_to_bronze: append idempotente al parquet del día en MinIO (particion fecha=YYYY-MM-DD).
  4. register_trino_schema: CREATE SCHEMA IF NOT EXISTS bronze.ecobici.
  5. register_trino_table: CREATE TABLE IF NOT EXISTS bronze.ecobici.tbl_station_status particionada por fecha.

Reusa los patrones del ejercicio 1:
  - I/O parquet a MinIO via Polars + storage_options (object_store).
  - Polars escribe con s3://; Trino registra con s3a:// (Hive Metastore solo reconoce s3a).
  - Connection ya existente del ejercicio 1: trino_default (AIRFLOW_CONN_TRINO_DEFAULT).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import polars as pl
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# ---------- MinIO / S3 ----------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio1234")
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "bck-bronze")

# Prefijo bronze del dataset Ecobici (sin trailing fecha=...).
ECOBICI_PREFIX = "master/ecobici/station_status"
PARQUET_FILENAME = "data.parquet"
# Staging parquet: punto de paso entre clean_and_transform y append_to_bronze.
STAGING_KEY = "_staging/ecobici/station_status_clean.parquet"

# Orden y tipos canonicos del schema bronze (incluye fecha al final por convencion Hive).
BRONZE_COLUMN_ORDER = [
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "last_reported_utc",
    "last_reported_cdmx",
    "last_updated_utc",
    "last_updated_cdmx",
    "is_installed",
    "is_renting",
    "is_returning",
    "imputed",
    "total_capacity",
    "occupancy_rate",
    "ingestion_ts",
    "fecha",
]

# ---------- GBFS feed ----------
GBFS_STATION_STATUS_URL = "https://gbfs.mex.lyftbikes.com/gbfs/en/station_status.json"
HTTP_TIMEOUT_SECONDS = 30
HTTP_MAX_RETRIES = 3
HTTP_BACKOFF_FACTOR = 1.5

# ---------- Trino ----------
TRINO_CONN_ID = "trino_default"
TRINO_CATALOG = "bronze"
TRINO_SCHEMA = "ecobici"
TRINO_TABLE = "tbl_station_status"

# ---------- Zonas horarias ----------
TZ_UTC = "UTC"
TZ_CDMX = "America/Mexico_City"


def _storage_options() -> dict[str, str]:
    """Storage options para Polars (object_store backend) contra MinIO via HTTP plano."""
    return {
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
        "aws_endpoint_url": MINIO_ENDPOINT,
        "aws_region": MINIO_REGION,
        "aws_allow_http": "true",
    }


def _s3_uri(bucket: str, key: str) -> str:
    """URI s3:// para Polars (object_store no entiende s3a://)."""
    return f"s3://{bucket}/{key}"


def _s3a_uri(bucket: str, key: str) -> str:
    """URI s3a:// para Trino/Hive Metastore (mismo objeto fisico)."""
    return f"s3a://{bucket}/{key}"


@dag(
    dag_id="etl_ecobici_gbfs_challenge",
    description="Ingesta GBFS Ecobici CDMX cada 10 min -> bronze parquet particionado por fecha -> Trino",
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    tags=["bancobase", "challenge", "ecobici", "gbfs", "polars", "trino"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def etl_ecobici_gbfs_challenge():
    @task
    def fetch_gbfs_station_status() -> dict[str, Any]:
        """1 - GET al feed GBFS station_status. Valida HTTP 200 y data.stations."""
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retry = Retry(
            total=HTTP_MAX_RETRIES,
            backoff_factor=HTTP_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))

        log.info("GET %s (timeout=%ds, retries=%d)", GBFS_STATION_STATUS_URL, HTTP_TIMEOUT_SECONDS, HTTP_MAX_RETRIES)
        response = session.get(GBFS_STATION_STATUS_URL, timeout=HTTP_TIMEOUT_SECONDS)
        if response.status_code != 200:
            raise RuntimeError(
                f"GBFS feed returned HTTP {response.status_code}: {response.text[:200]}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"GBFS feed did not return valid JSON: {exc}") from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"GBFS payload is not a JSON object (got {type(payload).__name__})")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("GBFS payload missing 'data' object")

        stations = data.get("stations")
        if not isinstance(stations, list):
            raise RuntimeError("GBFS payload missing 'data.stations' list")

        # last_updated puede venir en el root (estandar GBFS) o dentro de data (algunos feeds).
        last_updated = payload.get("last_updated")
        if last_updated is None:
            last_updated = data.get("last_updated")
        if last_updated is None:
            raise RuntimeError("GBFS payload missing 'last_updated' (root nor data)")

        log.info(
            "GBFS feed OK: %d stations, last_updated=%s",
            len(stations),
            last_updated,
        )
        return {"payload": payload, "last_updated": int(last_updated), "stations_count": len(stations)}

    @task
    def clean_and_transform(payload: dict[str, Any]) -> dict[str, Any]:
        """2 - data.stations -> Polars DataFrame, limpieza y columnas derivadas."""
        from airflow.operators.python import get_current_context

        raw = payload["payload"]
        feed_last_updated = int(payload["last_updated"])
        stations_count = int(payload["stations_count"])
        stations = raw["data"]["stations"]
        log.info("Input: %d estaciones del feed (last_updated=%d)", stations_count, feed_last_updated)

        # Proyeccion explicita: ignoramos campos opcionales/anidados (vehicle_types_available, etc).
        # Si una estacion no trae alguno de los campos requeridos, queda como null y se filtra abajo.
        projected = [
            {
                "station_id": s.get("station_id"),
                "num_bikes_available": s.get("num_bikes_available"),
                "num_docks_available": s.get("num_docks_available"),
                "last_reported": s.get("last_reported"),
                "is_installed": s.get("is_installed"),
                "is_renting": s.get("is_renting"),
                "is_returning": s.get("is_returning"),
            }
            for s in stations
        ]
        df = pl.DataFrame(
            projected,
            schema={
                "station_id": pl.Utf8,
                "num_bikes_available": pl.Int64,
                "num_docks_available": pl.Int64,
                "last_reported": pl.Int64,
                "is_installed": pl.Int64,
                "is_renting": pl.Int64,
                "is_returning": pl.Int64,
            },
            strict=False,
        )

        # Filtrar station_id nulo o vacio (cubre tambien "no-string" porque ya casteamos a Utf8).
        before_filter = df.height
        df = df.filter(pl.col("station_id").is_not_null() & (pl.col("station_id") != ""))
        dropped_invalid_id = before_filter - df.height
        if dropped_invalid_id:
            log.info("Filtradas %d estaciones por station_id nulo/invalido", dropped_invalid_id)

        # Casts de tiempo: epoch (s) -> Datetime UTC y CDMX (tz-aware en memoria).
        df = df.with_columns(
            pl.from_epoch(pl.col("last_reported"), time_unit="s")
            .dt.replace_time_zone(TZ_UTC)
            .alias("last_reported_utc"),
        )
        df = df.with_columns(
            pl.col("last_reported_utc").dt.convert_time_zone(TZ_CDMX).alias("last_reported_cdmx"),
            pl.from_epoch(pl.lit(feed_last_updated), time_unit="s")
            .dt.replace_time_zone(TZ_UTC)
            .alias("last_updated_utc"),
        )
        df = df.with_columns(
            pl.col("last_updated_utc").dt.convert_time_zone(TZ_CDMX).alias("last_updated_cdmx"),
        )

        # Booleans (cast desde 0/1; tambien acepta true/false si el feed los entrega asi).
        df = df.with_columns(
            pl.col("is_installed").cast(pl.Boolean, strict=False),
            pl.col("is_renting").cast(pl.Boolean, strict=False),
            pl.col("is_returning").cast(pl.Boolean, strict=False),
        )

        # Marca de imputacion ANTES de fillnull para registrar el estado original.
        df = df.with_columns(
            (
                pl.col("num_bikes_available").is_null()
                | pl.col("num_docks_available").is_null()
            ).alias("imputed"),
        )
        df = df.with_columns(
            pl.col("num_bikes_available").fill_null(0),
            pl.col("num_docks_available").fill_null(0),
        )

        # Si la estacion no esta instalada, fuerza contadores a 0 (independiente de lo que haya reportado).
        not_installed = pl.col("is_installed") == False  # noqa: E712 - polars necesita ==, no `is`
        df = df.with_columns(
            pl.when(not_installed).then(0).otherwise(pl.col("num_bikes_available")).alias("num_bikes_available"),
            pl.when(not_installed).then(0).otherwise(pl.col("num_docks_available")).alias("num_docks_available"),
        )

        # Dedup por station_id, conservando el reporte mas reciente.
        before_dedup = df.height
        df = df.sort("last_reported_utc", descending=True, nulls_last=True).unique(
            subset=["station_id"], keep="first", maintain_order=True
        )
        dropped_dedup = before_dedup - df.height
        if dropped_dedup:
            log.info("Dedup elimino %d filas duplicadas por station_id", dropped_dedup)

        # ingestion_ts: timestamp del DAG run (idempotente entre reintentos).
        ctx = get_current_context()
        ingestion_dt = ctx["data_interval_end"]  # pendulum.DateTime tz-aware en UTC
        # Polars rechaza pl.lit() sobre strings ISO con TZ embebido si no le damos formato.
        # Pasamos un datetime naive en wall-clock UTC; Polars lo materializa como Datetime("us").
        ingestion_naive_utc = ingestion_dt.replace(tzinfo=None)
        fecha_str = ingestion_dt.in_timezone(TZ_CDMX).strftime("%Y-%m-%d")
        log.info("ingestion_ts=%s (UTC) fecha=%s (CDMX)", ingestion_dt.isoformat(), fecha_str)

        df = df.with_columns(
            pl.lit(ingestion_naive_utc).cast(pl.Datetime("us")).alias("ingestion_ts"),
            pl.lit(fecha_str).alias("fecha"),
        )

        # Columnas derivadas: capacity y occupancy_rate (con guardia contra div/0).
        df = df.with_columns(
            (pl.col("num_bikes_available") + pl.col("num_docks_available"))
            .cast(pl.Int64)
            .alias("total_capacity"),
        )
        df = df.with_columns(
            pl.when(pl.col("total_capacity") > 0)
            .then(pl.col("num_bikes_available").cast(pl.Float64) / pl.col("total_capacity").cast(pl.Float64))
            .otherwise(0.0)
            .alias("occupancy_rate"),
        )

        if df.height == 0:
            raise RuntimeError(
                f"clean_and_transform dejo 0 filas (input={stations_count}, "
                f"dropped_invalid_id={dropped_invalid_id}, dropped_dedup={dropped_dedup}). "
                "No se escribira parquet vacio."
            )

        # Schema explicito y orden estable. Strippea timezones antes de escribir parquet
        # para que Polars no convierta los valores _cdmx a UTC al serializar.
        df_out = df.select(
            pl.col("station_id").cast(pl.Utf8),
            pl.col("num_bikes_available").cast(pl.Int64),
            pl.col("num_docks_available").cast(pl.Int64),
            pl.col("last_reported_utc").dt.replace_time_zone(None).cast(pl.Datetime("us")),
            pl.col("last_reported_cdmx").dt.replace_time_zone(None).cast(pl.Datetime("us")),
            pl.col("last_updated_utc").dt.replace_time_zone(None).cast(pl.Datetime("us")),
            pl.col("last_updated_cdmx").dt.replace_time_zone(None).cast(pl.Datetime("us")),
            pl.col("is_installed").cast(pl.Boolean),
            pl.col("is_renting").cast(pl.Boolean),
            pl.col("is_returning").cast(pl.Boolean),
            pl.col("imputed").cast(pl.Boolean),
            pl.col("total_capacity").cast(pl.Int64),
            pl.col("occupancy_rate").cast(pl.Float64),
            pl.col("ingestion_ts").cast(pl.Datetime("us")),
            pl.col("fecha").cast(pl.Utf8),
        )
        # Asercion de orden vs constante: si alguien edita uno y olvida el otro, el DAG falla rapido.
        if df_out.columns != BRONZE_COLUMN_ORDER:
            raise RuntimeError(
                f"Schema mismatch: got {df_out.columns}, expected {BRONZE_COLUMN_ORDER}"
            )

        staging_uri = _s3_uri(BRONZE_BUCKET, STAGING_KEY)
        df_out.write_parquet(staging_uri, storage_options=_storage_options())
        log.info(
            "Staging parquet escrito en %s con %d filas (input=%d, dropped_invalid_id=%d, dropped_dedup=%d)",
            staging_uri,
            df_out.height,
            stations_count,
            dropped_invalid_id,
            dropped_dedup,
        )
        return {
            "staging_uri": staging_uri,
            "fecha": fecha_str,
            "rows": df_out.height,
            "rows_input": stations_count,
            "rows_dropped_invalid_id": dropped_invalid_id,
            "rows_dropped_dedup": dropped_dedup,
        }

    @task
    def append_to_bronze(meta: dict[str, Any]) -> dict[str, Any]:
        """3 - Append idempotente al parquet del dia (dedup por station_id+last_reported)."""
        staging_uri = meta["staging_uri"]
        fecha = meta["fecha"]

        # Path destino del dia. El parquet vive bajo Hive layout: fecha=YYYY-MM-DD/data.parquet.
        target_key = f"{ECOBICI_PREFIX}/fecha={fecha}/{PARQUET_FILENAME}"
        target_uri = _s3_uri(BRONZE_BUCKET, target_key)
        log.info("Target del dia: %s", target_uri)

        new_df = pl.scan_parquet(staging_uri, storage_options=_storage_options()).collect()
        log.info("Filas nuevas (staging): %d", new_df.height)

        # Intento de read del parquet existente. Polars/object_store no expone HEAD,
        # asi que hacemos try/except y discriminamos por mensaje de error.
        existing_df: pl.DataFrame | None = None
        try:
            existing_df = pl.scan_parquet(target_uri, storage_options=_storage_options()).collect()
            log.info("Parquet del dia ya existe: %d filas", existing_df.height)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            not_found_signals = ("not found", "nosuchkey", "no such file", "notfound", "404")
            if any(sig in msg for sig in not_found_signals):
                log.info("Parquet del dia no existe, primer write para fecha=%s", fecha)
            else:
                raise

        if existing_df is not None:
            # Schemas deben coincidir; si difieren es bug nuestro y queremos fallar fuerte.
            if existing_df.columns != new_df.columns:
                raise RuntimeError(
                    f"Schema mismatch entre parquet existente y nuevo:\n"
                    f"  existente: {existing_df.columns}\n"
                    f"  nuevo:     {new_df.columns}"
                )
            combined = pl.concat([existing_df, new_df], how="vertical")
            rows_before = existing_df.height
        else:
            combined = new_df
            rows_before = 0

        # Dedup por (station_id, last_reported_utc). Idempotencia: corridas que reciben
        # el mismo snapshot del feed no duplican filas.
        before_dedup = combined.height
        combined = combined.unique(
            subset=["station_id", "last_reported_utc"], keep="last", maintain_order=False
        )
        dedup_removed = before_dedup - combined.height

        # Orden estable para que el parquet sea reproducible.
        combined = combined.sort(["last_reported_utc", "station_id"], nulls_last=True)

        # Sanity de orden de columnas (debe seguir siendo BRONZE_COLUMN_ORDER).
        if combined.columns != BRONZE_COLUMN_ORDER:
            raise RuntimeError(
                f"Schema final fuera de orden: {combined.columns} vs {BRONZE_COLUMN_ORDER}"
            )

        combined.write_parquet(target_uri, storage_options=_storage_options())
        log.info(
            "Parquet escrito en %s | rows_before=%d rows_new=%d rows_after=%d dedup_removed=%d",
            target_uri,
            rows_before,
            new_df.height,
            combined.height,
            dedup_removed,
        )
        return {
            "target_uri": target_uri,
            "target_key": target_key,
            "fecha": fecha,
            "rows_before": rows_before,
            "rows_new": new_df.height,
            "rows_after": combined.height,
            "dedup_removed": dedup_removed,
        }

    @task
    def register_trino_schema() -> str:
        """4 - CREATE SCHEMA IF NOT EXISTS bronze.ecobici."""
        from airflow.providers.trino.hooks.trino import TrinoHook

        hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)
        # location del schema apunta al bucket bronze (mismo patron que ejercicio 1).
        # Hive necesita s3a:// para resolver el FS via su ServiceLoader.
        schema_location = _s3a_uri(BRONZE_BUCKET, "")
        sql = (
            f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA} "
            f"WITH (location = '{schema_location}')"
        )
        log.info("Trino DDL: %s", sql)
        hook.run(sql)
        log.info("Schema %s.%s registrado", TRINO_CATALOG, TRINO_SCHEMA)
        return f"{TRINO_CATALOG}.{TRINO_SCHEMA}"

    @task
    def register_trino_table(append_meta: dict[str, Any]) -> dict[str, Any]:
        """5 - CREATE TABLE IF NOT EXISTS bronze.ecobici.tbl_station_status particionada por fecha."""
        from airflow.providers.trino.hooks.trino import TrinoHook

        hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)

        # external_location apunta al prefijo del dataset (sin la fecha=...).
        # Hive lee particiones bajo este prefijo cuando declaramos partitioned_by.
        table_location = _s3a_uri(BRONZE_BUCKET, ECOBICI_PREFIX + "/")
        table_fqn = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}"

        # Schema EXACTAMENTE igual al DataFrame escrito en append_to_bronze.
        # Convencion Hive: las columnas de particion van al FINAL del column list.
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_fqn} (
                station_id           VARCHAR,
                num_bikes_available  BIGINT,
                num_docks_available  BIGINT,
                last_reported_utc    TIMESTAMP(3),
                last_reported_cdmx   TIMESTAMP(3),
                last_updated_utc     TIMESTAMP(3),
                last_updated_cdmx    TIMESTAMP(3),
                is_installed         BOOLEAN,
                is_renting           BOOLEAN,
                is_returning         BOOLEAN,
                imputed              BOOLEAN,
                total_capacity       BIGINT,
                occupancy_rate       DOUBLE,
                ingestion_ts         TIMESTAMP(3),
                fecha                VARCHAR
            )
            WITH (
                external_location = '{table_location}',
                format = 'PARQUET',
                partitioned_by = ARRAY['fecha']
            )
        """
        log.info("Trino DDL CREATE TABLE en %s -> %s", table_fqn, table_location)
        hook.run(ddl)

        # Descubrir la particion del dia en el metastore. Sin este paso, Trino no ve
        # las particiones nuevas (estan en MinIO pero no registradas en Hive Metastore).
        # Mode 'ADD' es idempotente: solo agrega particiones nuevas, no toca existentes.
        sync_sql = (
            f"CALL {TRINO_CATALOG}.system.sync_partition_metadata("
            f"'{TRINO_SCHEMA}', '{TRINO_TABLE}', 'ADD')"
        )
        log.info("Trino sync particiones: %s", sync_sql)
        hook.run(sync_sql)

        # Conteo de validacion (incluye todas las particiones, no solo la del run actual).
        count_records = hook.get_records(f"SELECT count(*) FROM {table_fqn}")
        total_rows = count_records[0][0] if count_records else None
        log.info(
            "Tabla %s lista. fecha=%s rows_partition=%d total_rows=%s",
            table_fqn,
            append_meta["fecha"],
            append_meta["rows_after"],
            total_rows,
        )
        return {
            "table_fqn": table_fqn,
            "table_location": table_location,
            "fecha": append_meta["fecha"],
            "rows_partition": append_meta["rows_after"],
            "total_rows": total_rows,
        }

    raw = fetch_gbfs_station_status()
    cleaned = clean_and_transform(raw)
    appended = append_to_bronze(cleaned)
    schema_done = register_trino_schema()
    table_done = register_trino_table(appended)
    appended >> schema_done >> table_done


etl_ecobici_gbfs_challenge()
