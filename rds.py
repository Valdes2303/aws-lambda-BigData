# rds.py
import json
import os
import pymysql
import boto3

def handler(event, context):
    print("Iniciando el procesamiento del evento de S3...")
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        print(f"Nuevo archivo '{file_key}' en el bucket '{bucket_name}'.")

        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        try:
            datos_json = json.loads(obj['Body'].read().decode('utf-8'))
        except UnicodeDecodeError:
            print("Error de decodificación UTF-8, intentando con ISO-8859-1.")
            datos_json = json.loads(obj['Body'].read().decode('iso-8859-1'))
        
        if isinstance(datos_json, list) and len(datos_json) > 0:
            primer_elemento = datos_json[0]
            fechahora = primer_elemento.get('fechahora')
            valor = primer_elemento.get('valor')
        else:
            print("El archivo JSON no contiene datos válidos.")
            return {'statusCode': 400, 'body': json.dumps('El archivo JSON no contiene datos válidos.')}

        conn = pymysql.connect(
            host=os.environ['RDS_HOST'],
            user=os.environ['RDS_USER'],
            password=os.environ['RDS_PASSWORD'],
            database=os.environ['RDS_DB_NAME']
        )
        cursor = conn.cursor()
        sql = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
        cursor.execute(sql, (fechahora, valor))
        conn.commit()
        print(f"Datos insertados en la base de datos: fechahora={fechahora}, valor={valor}")
        cursor.close()
        conn.close()
        return {'statusCode': 200, 'body': json.dumps('Datos procesados y guardados en RDS exitosamente!')}
    except Exception as e:
        print(f"Ocurrió un error en el procesamiento de S3: {e}")
        return {'statusCode': 500, 'body': json.dumps(f'Error en el procesamiento de S3: {e}')}