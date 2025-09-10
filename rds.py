# rds.py
import os
import json
import logging
import urllib.parse

import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Recomendado: una única conexión por invocación
def _get_conn():
    return pymysql.connect(
        host=os.environ["RDS_HOST"],
        user=os.environ["RDS_USER"],
        password=os.environ["RDS_PASSWORD"],
        database=os.environ["RDS_DB_NAME"],
        autocommit=False,  # hacemos commit manual
        cursorclass=pymysql.cursors.Cursor,
    )

def _load_json_from_s3(bucket: str, key: str):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    # Decodificación defensiva
    for enc in ("utf-8", "iso-8859-1"):
        try:
            return json.loads(body.decode(enc))
        except UnicodeDecodeError:
            continue
    # Si no fue problema de encoding, relanza el error original
    return json.loads(body.decode("utf-8"))

def _insert_if_not_exists(cursor, fechahora, valor):
    """
    Evita duplicados sin tocar el esquema:
    - Consulta si ya existe ese fechahora.
    - Inserta solo si no está.
    """
    cursor.execute("SELECT 1 FROM dolar WHERE fechahora = %s LIMIT 1", (fechahora,))
    if cursor.fetchone():
        logger.info("Registro existente. Skip fechahora=%s", fechahora)
        return False

    cursor.execute(
        "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)",
        (fechahora, valor),
    )
    return True

def s3_to_rds_handler(event, context):
    """
    Handler para evento S3 (ObjectCreated). Procesa todos los Records del evento.
    Requisitos:
    - Objeto JSON con una lista y el primer elemento contiene 'fechahora' y 'valor'.
    """
    records = event.get("Records", []) or []
    logger.info("SOURCE=S3_EVENT records=%d", len(records))

    # Nada que hacer
    if not records:
        return {"statusCode": 200, "body": json.dumps("No records")}

    # Conexión a RDS (una por invocación)
    try:
        conn = _get_conn()
        cursor = conn.cursor()
    except Exception as e:
        logger.error("Error conectando a RDS: %s", e)
        return {"statusCode": 500, "body": json.dumps(f"RDS connection error: {e}")}

    processed = 0
    skipped = 0
    errors = 0

    try:
        for rec in records:
            try:
                bucket_name = rec["s3"]["bucket"]["name"]
                raw_key = rec["s3"]["object"]["key"]
                file_key = urllib.parse.unquote_plus(raw_key)

                # Procesa solo JSON (por si el trigger no tiene filtro)
                if not file_key.lower().endswith(".json"):
                    logger.info("Skip non-JSON key=%s", file_key)
                    skipped += 1
                    continue

                logger.info("Nuevo archivo key=%s bucket=%s", file_key, bucket_name)

                data = _load_json_from_s3(bucket_name, file_key)

                # Estructura esperada: lista con primer elemento {'fechahora':..., 'valor':...}
                if not isinstance(data, list) or not data:
                    logger.warning("JSON sin datos válidos. key=%s", file_key)
                    skipped += 1
                    continue

                item = data[0]
                fechahora = item.get("fechahora")
                valor = item.get("valor")

                if fechahora is None or valor is None:
                    logger.warning("Faltan campos. key=%s data=%s", file_key, item)
                    skipped += 1
                    continue

                # Normaliza valor (por si llega como string)
                try:
                    valor = float(valor)
                except Exception:
                    logger.warning("Valor no convertible a float. key=%s valor=%s", file_key, valor)
                    skipped += 1
                    continue

                inserted = _insert_if_not_exists(cursor, fechahora, valor)
                if inserted:
                    processed += 1
                    logger.info("Insert OK fechahora=%s valor=%s", fechahora, valor)
                else:
                    skipped += 1

            except Exception as inner_e:
                errors += 1
                logger.exception("Error procesando record. key=%s err=%s", raw_key if 'raw_key' in locals() else "?", inner_e)

        conn.commit()
        msg = f"Done. processed={processed}, skipped={skipped}, errors={errors}"
        logger.info(msg)
        return {"statusCode": 200, "body": json.dumps(msg)}

    except Exception as e:
        conn.rollback()
        logger.exception("Error general. rollback. err=%s", e)
        return {"statusCode": 500, "body": json.dumps(f"Error general: {e}")}

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass
