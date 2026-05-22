import os
import requests
import subprocess
import shutil
import boto3
import calendar
from bs4 import BeautifulSoup
from botocore.exceptions import ClientError

# --- 1. CONFIGURACIÓN ---
URL = "https://datosclima.es/Aemet2013/DescargaDatos.html"
CARPETA_RAR = "aemet_rar"
CARPETA_XLS = "aemet_xls"
NOMBRE_BUCKET = BUCKET_NAME

# Crear carpetas temporales en tu disco duro si no existen
os.makedirs(CARPETA_RAR, exist_ok=True)
os.makedirs(CARPETA_XLS, exist_ok=True)

# Cliente S3
s3 = boto3.client('s3')

# --- 2. FUNCIONES AUXILIARES ---
def archivo_existe_en_s3(bucket, key):
    """Comprueba si el archivo ya está en AWS para no subirlo dos veces."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def obtener_links_rar():
    """Hace web scraping para sacar todos los links de descarga .rar"""
    print("Buscando archivos .rar en la web...")
    html = requests.get(URL).text
    soup = BeautifulSoup(html, "html.parser")
    links = []
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".rar"):
            links.append("https://datosclima.es/Aemet2013/" + href)
            
    print(f"   -> Encontrados {len(links)} meses históricos.")
    return links

def descargar_y_extraer_rars(links):
    """Descarga los RAR, repara meses incompletos calculando los días exactos y extrae los XLS."""
    print("Descargando y extrayendo (lógica de calendario exacto activada)...")
    
    for i, url in enumerate(links):
        es_ultimo_mes = (i == len(links) - 1)
        
        nombre_rar = url.split("/")[-1]
        ruta_rar = os.path.join(CARPETA_RAR, nombre_rar)
        nombre_mes = nombre_rar.replace(".rar", "") # Ejemplo: "Aemet2024-05"
        carpeta_mes = os.path.join(CARPETA_XLS, nombre_mes)
        
        # Averiguar cuántos días DEBERÍA tener este mes exactamente
        try:
            partes = nombre_mes.replace("Aemet", "").split("-")
            year_int = int(partes[0])
            month_int = int(partes[1])
            dias_esperados = calendar.monthrange(year_int, month_int)[1]
        except Exception:
            dias_esperados = 31 # Seguro por si hay nombres raros
        
        # Contar cuántos Excel hay realmente descargados en local
        cantidad_xls = 0
        if os.path.exists(carpeta_mes):
            cantidad_xls = len([f for f in os.listdir(carpeta_mes) if f.endswith('.xls')])
        
        # Lógica anti-fallos: Si no es el mes actual y ya tiene los días exactos, saltamos.
        if not es_ultimo_mes and cantidad_xls >= dias_esperados:
             continue
             
        # Mensajes de consola
        if es_ultimo_mes:
            print(f" -> Actualizando mes en curso: {nombre_mes}")
        else:
            print(f" -> Descargando/Reparando: {nombre_mes} (Tiene {cantidad_xls} / Faltan hasta {dias_esperados})")
             
        # Descarga del RAR
        r = requests.get(url)
        if r.status_code == 200:
            with open(ruta_rar, "wb") as f:
                f.write(r.content)
        else:
            print(f"Error al descargar {nombre_rar} de la web.")
            continue
            
        # Extracción forzada con unar
        os.makedirs(carpeta_mes, exist_ok=True)
        subprocess.run(["unar", "-f", "-o", carpeta_mes, ruta_rar], check=False, stdout=subprocess.DEVNULL)
        
        # Limpieza: Mover XLS y eliminar subcarpetas dobles
        for root, dirs, files in os.walk(carpeta_mes):
            for f in files:
                if f.lower().endswith(".xls"):
                    origen = os.path.join(root, f)
                    destino = os.path.join(carpeta_mes, f)
                    if origen != destino:
                        shutil.move(origen, destino)

def subir_xls_a_s3():
    """Sube la estructura de carpetas a Amazon S3 respetando la Capa Bronce."""
    print("Subiendo archivos XLS a Amazon S3...")
    archivos_subidos = 0
    
    for root, dirs, files in os.walk(CARPETA_XLS):
        for file in files:
            if file.lower().endswith(".xls"):
                ruta_local = os.path.join(root, file)
                
                # Extraer año y mes. Ejemplo de file: "Aemet2024-05-02.xls"
                nombre_sin_ext = file.replace(".xls", "").replace(".XLS", "")
                partes = nombre_sin_ext.split("-")
                
                try:
                    year = partes[0][-4:]
                    month = partes[1]
                except IndexError:
                    continue 
                
                # Definir ruta destino en S3
                s3_key = f"bronce/clima/year={year}/month={month}/{file}"
                
                # Subir solo si no existe ya en S3
                if not archivo_existe_en_s3(NOMBRE_BUCKET, s3_key):
                    s3.upload_file(ruta_local, NOMBRE_BUCKET, s3_key)
                    archivos_subidos += 1
                    
    print(f"\nProceso histórico finalizado. Se subieron {archivos_subidos} archivos XLS nuevos a S3.")

# --- 3. EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    links_a_descargar = obtener_links_rar()
    descargar_y_extraer_rars(links_a_descargar)
    subir_xls_a_s3()