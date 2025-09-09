import requests
import json
import datetime
import time
import boto3
import pymysql
import os
from utils import get_dolar_data

URL_API = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"

def handler(event, context):
    """
    Función principal de la Lambda para descargar el valor del dólar
    y guardarlo en S3.
    """
    print("Iniciando la descarga del valor del dólar...")

    try:
        # 1. Realizar la solicitud HTTP a la API
        response = requests.get(URL_API)
        response.raise_for_status()  # Lanza una excepción para errores HTTP

        # 2. Obtener los datos de la respuesta
        datos = response.json()
        print("Datos obtenidos exitosamente.")

        # 3. Preparar el nombre del archivo
        # Generar un timestamp para el nombre del archivo
        timestamp = int(time.time())
        nombre_archivo = f"dolar-{timestamp}.json"
        
        # 4. Guardar el archivo en S3
        s3 = boto3.client('s3')
        BUCKET_NAME = "dolar-raw-0723" 
        
        # Convertir los datos a formato JSON y guardarlos en S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=nombre_archivo,
            Body=json.dumps(datos, indent=4)
        )

        print(f"Archivo '{nombre_archivo}' guardado en el bucket '{BUCKET_NAME}'.")

        return {
            'statusCode': 200,
            'body': json.dumps('Datos del dólar descargados y guardados en S3 exitosamente!')
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Error al hacer la solicitud a la API: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error al hacer la solicitud a la API: {e}')
        }
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Ocurrió un error inesperado: {e}')
        }
        
def s3_to_rds_handler(event, context):
    """
    Función de Lambda que se activa con un evento de S3 para procesar
    archivos JSON y guardar los datos en una base de datos RDS.
    """
    print("Iniciando el procesamiento del evento de S3...")

    # Obtener el nombre del bucket y la clave del archivo del evento de S3
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    print(f"Nuevo archivo '{file_key}' en el bucket '{bucket_name}'.")

    try:
        # 1. Descargar el archivo de S3
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        datos_json = json.loads(obj['Body'].read().decode('utf-8'))
        
        # 2. Extraer los datos relevantes del JSON
        # Los datos del Banco de la República vienen en una lista de diccionarios
        fechahora = datos_json[0].get('fechahora')
        valor = datos_json[0].get('valor')
        
        # 3. Conectarse a la base de datos RDS
        conn = pymysql.connect(
            host=os.environ['RDS_HOST'],
            user=os.environ['RDS_USER'],
            password=os.environ['RDS_PASSWORD'],
            database=os.environ['RDS_DB_NAME']
        )
        cursor = conn.cursor()

        # 4. Insertar los datos en la tabla 'dolar'
        sql = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
        cursor.execute(sql, (fechahora, valor))
        conn.commit()

        print(f"Datos insertados en la base de datos: fechahora={fechahora}, valor={valor}")
        
        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps('Datos procesados y guardados en RDS exitosamente!')
        }
        
    except Exception as e:
        print(f"Ocurrió un error en el procesamiento de S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error en el procesamiento de S3: {e}')
        }