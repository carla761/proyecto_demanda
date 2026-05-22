import json
import os
import boto3
import urllib3
from datetime import datetime
from botocore.exceptions import ClientError

# --- 1. CONFIGURACIÓN ---
API_KEY = os.environ.get("CALENDARIFIC_KEY", "Xb162V3SZvCnbe7DKGgVvPVhn8kQNAFX")
COUNTRY = "ES"
NOMBRE_BUCKET = os.environ.get("BUCKET_NAME", "cag-proyecto-demanda-electrica-bd")

# Inicializamos los clientes fuera del handler para optimizar rendimiento
s3 = boto3.client('s3')
http = urllib3.PoolManager()

def archivo_existe_en_s3(bucket, key):
    """Comprueba si el archivo ya está en AWS para no repetir trabajo."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def get_festivos(year):
    """Llama a la API usando urllib3 y devuelve los datos en la memoria RAM."""
    url = "https://calendarific.com/api/v2/holidays"
    
    # Construimos los parámetros de la URL de forma segura
    campos = {
        "api_key": API_KEY,
        "country": COUNTRY,
        "year": str(year)
    }
    
    try:
        # En urllib3 los parámetros se pasan directamente en el método request
        resp = http.request('GET', url, fields=campos)
        
        if resp.status == 200:
            return json.loads(resp.data.decode('utf-8'))
        else:
            print(f"Error en API para el año {year}: Código {resp.status}")
            return None
    except Exception as e:
        print(f"❌ Error en la petición HTTP para el año {year}: {e}")
        return None

def lambda_handler(event, context):
    print("=== INICIANDO INGESTA DE FESTIVOS (HISTÓRICO + INCREMENTAL ANUAL) ===")
    archivos_subidos = 0
    
    # 🌟 CAMBIO CLAVE: Calculamos dinámicamente el año actual para que sea incremental automático
    anio_actual = datetime.now().year
    
    # Iterará desde 2013 hasta el año en curso en el que te encuentres
    for year in range(2013, anio_actual + 1):
        # Definir ruta destino particionada en S3 exactamente igual a tu diseño
        s3_key = f"bronce/festivos/year={year}/festivos_{year}.json"
        
        # 1. Comprobar S3 antes de gastar llamada a la API
        if archivo_existe_en_s3(NOMBRE_BUCKET, s3_key):
            print(f"-> Omitiendo {year} (Ya existe en S3)")
            continue
            
        # 2. Descargar a la memoria RAM
        print(f"-> Extrayendo año {year} de Calendarific...")
        data = get_festivos(year)
        
        # 3. Inyectar en S3 directamente desde la variable 'data'
        if data:
            try:
                s3.put_object(
                    Bucket=NOMBRE_BUCKET,
                    Key=s3_key,
                    Body=json.dumps(data, ensure_ascii=False, indent=2),
                    ContentType='application/json'
                )
                print(f"   [OK] Subido a S3: {s3_key}")
                archivos_subidos += 1
            except Exception as e:
                print(f"❌ Error al subir el año {year} a S3: {e}")
                
    mensaje_final = f"Proceso finalizado. Se subieron {archivos_subidos} años nuevos a S3."
    print(f"\n{mensaje_final}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(mensaje_final)
    }