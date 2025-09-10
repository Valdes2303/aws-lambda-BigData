import os
import json
import logging
from datetime import datetime, timezone
import urllib.parse

import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Config por variables de entorno  ---
DB_HOST = os.environ["RDS_HOST"]
DB_USER = os.environ["RDS_USER"]
DB_PASSWORD = os.environ["RDS_PASSWORD"]
DB_NAME = os.environ["RDS_DB_NAME"]         
DB_PORT = int(os.environ.get("RDS_PORT", "3306"))

s3 = boto3.client("s3")  # región la toma del runtime (us-east-1)

def _connect_db():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,          
        port=DB_PORT,
        connect_timeout=10,
        read_timeout=10,
        write_timeout=10,
        cursorclass=pymysql.cursors.Cursor,
        autocommit=False,
    )

def _pairs_to_rows(pairs):
    """
    Convierte [[ts_ms, valor], ...] a [(fechahora(datetime), valor(float)), ...]
    """
    rows = []
    for i, p in enumerate(pairs):
        try:
            ts_ms = int(p[0])
            valor = float(p[1])
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)
            rows.append((dt, valor))
        except Exception as e:
            logger.warning("Fila inválida en índice %s: %s -> %s", i, p, e)
    return rows

def _dicts_to_rows(dicts):
    """
    Convierte [{'fechahora': 'YYYY-MM-DD HH:MM:SS', 'valor': X}, ...] a la misma tupla.
    Toma solo el primer elemento si tu JSON viene con un único dict en una lista.
    """
    rows = []
    seq = dicts if isinstance(dicts, list) else [dicts]
    for i, d in enumerate(seq):
        try:
            fh = d.get("fechahora")
            valor = float(d.get("valor"))
            # Acepta formatos comunes de fecha/hora
            dt = datetime.fromisoformat(fh.replace("Z", "").replace("T", " ")).replace(tzinfo=None)
            rows.append((dt, valor))
        except Exception as e:
            logger.warning("Dict inválido en índice %s: %s -> %s", i, d, e)
    return rows

def _json_to_rows(data):
    """
    Acepta ambos formatos:
      - [[ts_ms, valor], ...]
      - [{'fechahora': ..., 'valor': ...}, ...]
    Devuelve lista de tuplas [(fechahora(datetime), valor(float)), ...]
    """
    if isinstance(data, list) and data:
        # Caso lista de pares
        if isinstance(data[0], (list, tuple)) and len(data[0]) >= 2:
            return _pairs_to_rows(data)
        # Caso lista de dicts
        if isinstance(data[0], dict):
            return _dicts_to_rows(data)
    # Caso dict suelto
    if isinstance(data, dict):
        return _dicts_to_rows(data)
    return []

def s3_to_rds_handler(event, context):
    """
    Handler invocado por S3:ObjectCreated:*
    Lee el JSON, extrae filas (fechahora, valor) y hace UPSERT en dolar(fechahora, valor).
    """
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key    = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        logger.info("Nuevo archivo: s3://%s/%s", bucket, key)

        if not key.lower().endswith(".json"):
            logger.info("Skip (no es .json): %s", key)
            return {"statusCode": 200, "body": "No JSON"}

        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()

        # Parse JSON
        try:
            data = json.loads(raw)
        except Exception as e:
            logger.error("Error parseando JSON: %s", e)
            return {"statusCode": 200, "body": "Archivo no es JSON válido"}

        rows = _json_to_rows(data)
        if not rows:
            logger.warning("Sin datos válidos en el JSON. Muestra: %s", str(data)[:400])
            return {"statusCode": 200, "body": "Sin datos válidos"}

        # Conectar a tu DB ya existente
        conn = _connect_db()
        try:
            with conn.cursor() as cur:
                # UPSERT (idempotente) – requiere PK/UNIQUE en fechahora
                cur.executemany(
                    """
                    INSERT INTO dolar (fechahora, valor)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE valor = VALUES(valor)
                    """,
                    rows
                )
            conn.commit()
            msg = f"Insertados/actualizados: {len(rows)}"
            logger.info(msg)
            return {"statusCode": 200, "body": msg}
        finally:
            conn.close()

    except Exception as e:
        logger.exception("Error general en s3_to_rds_handler: %s", e)
        return {"statusCode": 500, "body": f"Error: {e}"}
