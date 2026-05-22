import json
import os
import boto3
import urllib3
from datetime import datetime, timedelta

# Inicializamos los clientes fuera del handler
s3 = boto3.client('s3')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    print("=== INICIANDO INGESTA INCREMENTAL: DEMANDA REE ===")
    
    # 1. Recuperar nombre del bucket
    NOMBRE_BUCKET = os.environ.get('BUCKET_NAME', 'cag-proyecto-demanda-electrica-bd')
    
    # 2. Calcular las fechas (El día de ayer)
    ayer = datetime.now() - timedelta(days=1)
    
    # Formateamos para la URL (T00:00 a T23:59) y para las carpetas S3
    inicio = ayer.strftime("%Y-%m-%dT00:00")
    fin = ayer.strftime("%Y-%m-%dT23:59")
    fecha_str = ayer.strftime("%Y-%m-%d") # Nombre del archivo
    year = ayer.strftime("%Y")
    month = ayer.strftime("%m")
    
    # 3. Llamada a la API pública de REE (Igual que tu script)
    url = f"https://apidatos.ree.es/es/datos/balance/balance-electrico?start_date={inicio}&end_date={fin}&time_trunc=day"
    
    try:
        print(f"-> Consultando API para el día: {fecha_str}")
        response = http.request('GET', url)
        
        if response.status != 200:
            print(f"Error en la API (Código {response.status})")
            return {"statusCode": response.status, "body": "Error en API REE"}
            
        data = json.loads(response.data.decode('utf-8'))
        
    except Exception as e:
        print(f"Error HTTP: {str(e)}")
        return {"statusCode": 500, "body": str(e)}

    # 4. Lógica de parseo EXACTA a tu script histórico
    datos_dia = []
    for grupo in data.get("included", []):
        if grupo.get("type") == "Demanda":
            for sub in grupo["attributes"].get("content", []):
                if sub.get("type") == "Demanda en b.c.":
                    valores = sub["attributes"]["values"]
                    datos_dia = [
                        {"datetime": v["datetime"], "value": float(v["value"])}
                        for v in valores
                    ]

    if not datos_dia:
        print("No se encontró la métrica 'Demanda en b.c.' en el JSON.")
        return {"statusCode": 404, "body": "Falta clave en JSON"}

    # 5. Guardar en S3 respetando tu Particionado Medallón
    s3_key = f"bronce/demanda/year={year}/month={month}/{fecha_str}.json"
    
    try:
        print(f"-> Subiendo archivo a {s3_key}...")
        s3.put_object(
            Bucket=NOMBRE_BUCKET,
            Key=s3_key,
            Body=json.dumps(datos_dia, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        print("Ingesta incremental completada con éxito.")
        return {"statusCode": 200, "body": "Éxito"}
        
    except Exception as e:
        print(f"Error al guardar en S3: {str(e)}")
        return {"statusCode": 500, "body": str(e)}