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
            'body': json.dumps('Datos del dólar descargados y guardados en S3 exitosamente')
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
        