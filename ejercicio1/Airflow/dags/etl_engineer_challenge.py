"""ETL Engineer Challenge - Airflow + MinIO + Polars + Trino.

Pipeline:
  1. Read CSV from s3://bck-landing/data/data_prueba_tecnica.csv
  2. Clean + transform with Polars; quarantine bad rows
  3. Aggregate clean data by (name, created_at)
  4. Write final parquet to s3://bck-bronze/master/ and s3://bck-bronze/master_agg/
  5. Register schema bronze.prueba and tables tbl_data / tbl_data_agg in Trino
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import polars as pl
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio1234")
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")

LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "bck-landing")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "bck-bronze")
LANDING_OBJECT_KEY = os.environ.get("LANDING_OBJECT_KEY", "data/data_prueba_tecnica.csv")
BRONZE_MASTER_KEY = os.environ.get("BRONZE_MASTER_KEY", "master/data_prueba_tecnica.parquet")
BRONZE_AGG_KEY = os.environ.get("BRONZE_AGG_KEY", "master_agg/tbl_data_agg.parquet")
BRONZE_QUARANTINE_KEY = os.environ.get(
    "BRONZE_QUARANTINE_KEY", "quarantine/data_prueba_tecnica_quarantine.parquet"
)

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "root")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "bronze")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "prueba")
TRINO_TABLE_DETAIL = "tbl_data"
TRINO_TABLE_AGG = "tbl_data_agg"

STAGING_RAW_KEY = "_staging/raw.parquet"
STAGING_CLEAN_KEY = "_staging/clean.parquet"
STAGING_AGG_KEY = "_staging/agg.parquet"
STAGING_QUARANTINE_KEY = "_staging/quarantine.parquet"

VALID_STATUSES = [
    "paid",
    "voided",
    "pending_payment",
    "refunded",
    "charged_back",
    "pre_authorized",
    "expired",
    "partially_refunded",
]
DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y%m%d"]
AMOUNT_MAX = 1_000_000.0


def _storage_options() -> dict[str, str]:
    # aws_allow_http is mandatory: MinIO is exposed via plain HTTP, not HTTPS.
    return {
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
        "aws_endpoint_url": MINIO_ENDPOINT,
        "aws_region": MINIO_REGION,
        "aws_allow_http": "true",
    }


def _s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def _parse_dates(col: str) -> pl.Expr:
    return pl.coalesce(
        [pl.col(col).str.strptime(pl.Date, format=fmt, strict=False) for fmt in DATE_FORMATS]
    )


@dag(
    dag_id="etl_engineer_challenge",
    description="MinIO landing CSV -> clean+aggregate (Polars) -> bronze parquet -> Trino external tables",
    schedule=None,
    catchup=False,
    start_date=datetime(2024, 1, 1),
    tags=["bancobase", "challenge", "etl", "polars", "trino"],
    default_args={"owner": "data-engineering", "retries": 0},
    doc_md=__doc__,
)
def etl_engineer_challenge():
    @task
    def read_csv_from_landing() -> dict[str, Any]:
        """3.1 - Read raw CSV from MinIO landing and persist a parquet snapshot to staging."""
        src = _s3_uri(LANDING_BUCKET, LANDING_OBJECT_KEY)
        log.info("Reading CSV from %s", src)
        # scan_csv (not read_csv) forces Polars to use its native object_store backend instead
        # of falling back to fsspec/s3fs, which uses different storage_options keys and would
        # reject our aws_* options.
        df = pl.scan_csv(
            src,
            storage_options=_storage_options(),
            infer_schema_length=0,
        ).collect()

        # The source CSV has CRLF line endings (actually \r\r\n). Polars eol_char defaults to \n,
        # so the trailing \r leaks into the last header (paid_at\r) and into every last cell value.
        # Normalize: strip whitespace from column names and from every string value.
        df = df.rename({c: c.strip() for c in df.columns})
        df = df.with_columns(pl.col(pl.Utf8).str.strip_chars())

        log.info("Raw rows=%d cols=%s", df.height, df.columns)

        raw_uri = _s3_uri(BRONZE_BUCKET, STAGING_RAW_KEY)
        df.write_parquet(raw_uri, storage_options=_storage_options())
        return {"raw_uri": raw_uri, "rows": df.height}

    @task
    def clean_transform_aggregate(meta: dict[str, Any]) -> dict[str, Any]:
        """3.2 - Cleaning + transformation + aggregations with Polars. Splits clean vs quarantine."""
        df = pl.scan_parquet(meta["raw_uri"], storage_options=_storage_options()).collect()
        log.info("Loaded raw=%d", df.height)

        # Tokens like 'MiPas0xFFFF' / 'MiP0xFFFF' / '0xFFFF' are corruption of the only large
        # merchant name 'MiPasajefy'; impute back. Truly-empty names cannot be inferred -> quarantine.
        df = df.with_columns(
            pl.when(pl.col("name").str.contains("0xFFFF"))
            .then(pl.lit("MiPasajefy"))
            .otherwise(pl.col("name"))
            .alias("name_clean"),
            _parse_dates("created_at").alias("created_at_clean"),
            _parse_dates("paid_at").alias("paid_at_clean"),
            # Float64 cast may yield null (junk) or +inf (e.g. 3.0e213231213123 overflow).
            pl.col("amount").cast(pl.Float64, strict=False).alias("amount_clean"),
        )

        is_paid_status = pl.col("status").is_in(["paid", "refunded", "partially_refunded"])
        paid_at_is_blank = pl.col("paid_at").is_null() | (
            pl.col("paid_at").str.strip_chars() == ""
        )

        df = df.with_columns(
            (pl.col("id").is_null() | (pl.col("id").str.strip_chars() == "")).alias(
                "flag_id_missing"
            ),
            (
                pl.col("name_clean").is_null()
                | (pl.col("name_clean").str.strip_chars() == "")
            ).alias("flag_name_missing"),
            (
                pl.col("company_id").is_null()
                | (pl.col("company_id").str.strip_chars() == "")
            ).alias("flag_company_missing"),
            (~pl.col("status").is_in(VALID_STATUSES)).alias("flag_status_invalid"),
            (
                pl.col("amount_clean").is_null()
                | ~pl.col("amount_clean").is_finite()
                | (pl.col("amount_clean") > AMOUNT_MAX)
                | (pl.col("amount_clean") < 0)
            ).alias("flag_amount_invalid"),
            pl.col("created_at_clean").is_null().alias("flag_created_at_invalid"),
            # paid_at must parse only when status implies it. Empty paid_at on non-paid rows is legitimate.
            (is_paid_status & ~paid_at_is_blank & pl.col("paid_at_clean").is_null()).alias(
                "flag_paid_at_invalid"
            ),
        )

        flag_cols = [
            "flag_id_missing",
            "flag_name_missing",
            "flag_company_missing",
            "flag_status_invalid",
            "flag_amount_invalid",
            "flag_created_at_invalid",
            "flag_paid_at_invalid",
        ]
        df = df.with_columns(pl.any_horizontal(*flag_cols).alias("is_quarantine"))

        clean = df.filter(~pl.col("is_quarantine"))
        quarantine = df.filter(pl.col("is_quarantine"))

        before = clean.height
        clean = clean.unique(subset=["id"], keep="first", maintain_order=True)
        log.info(
            "Dedup removed %d duplicates (clean: %d -> %d)",
            before - clean.height,
            before,
            clean.height,
        )

        clean_final = clean.select(
            pl.col("id").cast(pl.Utf8),
            pl.col("name_clean").cast(pl.Utf8).alias("name"),
            pl.col("company_id").cast(pl.Utf8),
            pl.col("amount_clean").cast(pl.Float64).alias("amount"),
            pl.col("status").cast(pl.Utf8),
            pl.col("created_at_clean").alias("created_at"),
            pl.col("paid_at_clean").alias("paid_at"),
        )

        quarantine_final = quarantine.select(
            pl.col("id"),
            pl.col("name"),
            pl.col("company_id"),
            pl.col("amount"),
            pl.col("status"),
            pl.col("created_at"),
            pl.col("paid_at"),
            *[pl.col(c) for c in flag_cols],
        )

        agg = (
            clean_final.group_by(["name", "created_at"])
            .agg(
                pl.len().cast(pl.Int64).alias("tx_count"),
                pl.col("amount").sum().alias("total_amount"),
                pl.when(pl.col("status") == "paid")
                .then(pl.col("amount"))
                .otherwise(0.0)
                .sum()
                .alias("paid_amount"),
                pl.col("status").eq("paid").sum().cast(pl.Int64).alias("paid_count"),
                pl.col("company_id").n_unique().cast(pl.Int64).alias("distinct_companies"),
            )
            .sort(["name", "created_at"])
        )

        clean_uri = _s3_uri(BRONZE_BUCKET, STAGING_CLEAN_KEY)
        quarantine_uri = _s3_uri(BRONZE_BUCKET, STAGING_QUARANTINE_KEY)
        agg_uri = _s3_uri(BRONZE_BUCKET, STAGING_AGG_KEY)
        clean_final.write_parquet(clean_uri, storage_options=_storage_options())
        quarantine_final.write_parquet(quarantine_uri, storage_options=_storage_options())
        agg.write_parquet(agg_uri, storage_options=_storage_options())

        log.info(
            "Stats: input=%d clean=%d quarantine=%d agg_rows=%d",
            df.height,
            clean_final.height,
            quarantine_final.height,
            agg.height,
        )
        return {
            "clean_uri": clean_uri,
            "quarantine_uri": quarantine_uri,
            "agg_uri": agg_uri,
            "rows_input": df.height,
            "rows_clean": clean_final.height,
            "rows_quarantine": quarantine_final.height,
            "rows_agg": agg.height,
        }

    @task
    def write_parquet_to_bronze(meta: dict[str, Any]) -> dict[str, Any]:
        """3.3 - Promote staging parquets to canonical bronze paths (overwrite)."""
        clean_df = pl.scan_parquet(meta["clean_uri"], storage_options=_storage_options()).collect()
        agg_df = pl.scan_parquet(meta["agg_uri"], storage_options=_storage_options()).collect()
        quarantine_df = pl.scan_parquet(
            meta["quarantine_uri"], storage_options=_storage_options()
        ).collect()

        master_uri = _s3_uri(BRONZE_BUCKET, BRONZE_MASTER_KEY)
        agg_uri = _s3_uri(BRONZE_BUCKET, BRONZE_AGG_KEY)
        quarantine_uri = _s3_uri(BRONZE_BUCKET, BRONZE_QUARANTINE_KEY)

        clean_df.write_parquet(master_uri, storage_options=_storage_options())
        agg_df.write_parquet(agg_uri, storage_options=_storage_options())
        quarantine_df.write_parquet(quarantine_uri, storage_options=_storage_options())

        log.info(
            "Bronze written: master=%s agg=%s quarantine=%s",
            master_uri,
            agg_uri,
            quarantine_uri,
        )
        return {
            "master_uri": master_uri,
            "agg_uri": agg_uri,
            "quarantine_uri": quarantine_uri,
            # Trino external_location must point at a directory (prefix), not a single object.
            "master_dir": _s3_uri(BRONZE_BUCKET, "master/"),
            "agg_dir": _s3_uri(BRONZE_BUCKET, "master_agg/"),
        }

    @task
    def register_in_trino(paths: dict[str, Any]) -> dict[str, Any]:
        """4.1 - Create schema bronze.prueba and external tables tbl_data / tbl_data_agg."""
        from trino.dbapi import connect

        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
        )

        def run(sql: str):
            log.info("Trino SQL: %s", sql.strip().splitlines()[0][:200])
            cur = conn.cursor()
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return None

        # Hive Metastore validates locations using Hadoop FS, where 's3' scheme is not registered
        # (only 's3a' is, via the S3A ServiceLoader). Trino's native S3 reader treats both
        # schemes identically, so we hand the metastore 's3a://' URIs while Polars keeps writing
        # via 's3://' (object_store). Both point at the same MinIO objects.
        master_dir = paths["master_dir"].replace("s3://", "s3a://", 1)
        agg_dir = paths["agg_dir"].replace("s3://", "s3a://", 1)
        schema_location = f"s3a://{BRONZE_BUCKET}/"

        run(
            f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA} "
            f"WITH (location = '{schema_location}')"
        )

        run(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE_DETAIL}")
        run(
            f"""
            CREATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE_DETAIL} (
                id          VARCHAR,
                name        VARCHAR,
                company_id  VARCHAR,
                amount      DOUBLE,
                status      VARCHAR,
                created_at  DATE,
                paid_at     DATE
            )
            WITH (
                external_location = '{master_dir}',
                format = 'PARQUET'
            )
            """
        )

        run(f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE_AGG}")
        run(
            f"""
            CREATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE_AGG} (
                name                VARCHAR,
                created_at          DATE,
                tx_count            BIGINT,
                total_amount        DOUBLE,
                paid_amount         DOUBLE,
                paid_count          BIGINT,
                distinct_companies  BIGINT
            )
            WITH (
                external_location = '{agg_dir}',
                format = 'PARQUET'
            )
            """
        )

        detail_count = run(
            f"SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE_DETAIL}"
        )
        agg_count = run(f"SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE_AGG}")
        log.info("Trino detail count=%s agg count=%s", detail_count, agg_count)
        return {
            "detail_count": detail_count[0][0] if detail_count else None,
            "agg_count": agg_count[0][0] if agg_count else None,
        }

    raw = read_csv_from_landing()
    transformed = clean_transform_aggregate(raw)
    bronze_paths = write_parquet_to_bronze(transformed)
    register_in_trino(bronze_paths)


etl_engineer_challenge()
