# rds.py
import os
import json
import logging
import urllib.parse

import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def _get_conn():
    return pymysql.connect(
        host=os.environ["RDS_HOST"],
        user=os.environ["RDS_USER"],
        password=os.environ["RDS_PASSWORD"],
        database=os.environ["RDS_DB_NAME"],
        autocommit=False,
        cursorclass=pymysql.cursors.Cursor,
    )

def _load_json_from_s3(bucket: str, key: str):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    for enc in ("utf-8", "iso-8859-1"):
        try:
            return json.loads(body.decode(enc))
        except UnicodeDecodeError:
            continue
    return json.loads(body.decode("utf-8"))

# === NUEVO: extractor robusto para listas anidadas ===
def _find_record(node):
    """
    Busca recursivamente el primer dict que contenga 'fechahora' y 'valor'
    dentro de estructuras potencialmente anidadas de listas/dicts.
    Devuelve (fechahora, valor) o (None, None) si no encuentra.
    """
    # Si es dict y tiene las claves
    if isinstance(node, dict):
        if "fechahora" in node and "valor" in node:
            return node["fechahora"], node["valor"]
        # Si es dict pero no tiene las claves, intenta dentro de sus valores
        for v in node.values():
            fh, val = _find_record(v)
            if fh is not None:
                return fh, val
        return None, None

    # Si es lista/tupla, intenta cada elemento
    if isinstance(node, (list, tuple)):
        for elem in node:
            fh, val = _find_record(elem)
            if fh is not None:
                return fh, val
        return None, None

    # Tipos primitivos: no hay nada que hacer
    return None, None

def _insert_if_not_exists(cursor, fechahora, valor):
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
    records = event.get("Records", []) or []
    logger.info("SOURCE=S3_EVENT records=%d", len(records))

    if not records:
        return {"statusCode": 200, "body": json.dumps("No records")}

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

                if not file_key.lower().endswith(".json"):
                    logger.info("Skip non-JSON key=%s", file_key)
                    skipped += 1
                    continue

                logger.info("Nuevo archivo key=%s bucket=%s", file_key, bucket_name)

                data = _load_json_from_s3(bucket_name, file_key)

                # === USAR EXTRACTOR ROBUSTO ===
                fechahora, valor = _find_record(data)
                if fechahora is None or valor is None:
                    logger.warning("No se encontraron campos 'fechahora' y 'valor'. key=%s; sample=%s",
                                   file_key, str(data)[:500])
                    skipped += 1
                    continue

                # Normaliza valor
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
                logger.exception("Error procesando record. key=%s err=%s",
                                 raw_key if 'raw_key' in locals() else "?", inner_e)

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
