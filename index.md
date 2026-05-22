# Proyecto Integrador: Predictor de Demanda Eléctrica

Repositorio principal para el proyecto de integración de datos eléctricos y meteorológicos. Este pipeline implementa una arquitectura Medallón para la ingesta, limpieza y preparación de datos orientada a modelos predictivos.

## Organización de Archivos

### 1. Ingesta (Capa Bronce - AWS)
Scripts de automatización en AWS Lambda para la ingesta incremental:
- [incremental-demanda.py](/AWS_Lambda/incremental-demanda.py): Consulta diaria a la API de REE (ESIOS).
- [incremental-festivos.py](/AWS_Lambda/incremental-festivos.py): Ingesta histórica y automatizada de festivos mediante Calendarific.

### 2. Procesamiento (Capa Plata y Oro)
Scripts para la normalización y Feature Engineering mediante Apache Spark:
- [Arquitectura-Medallón.ipynb](/Arquitectura-Medallón.ipynb): Pipeline de transformación y cruce de datos.
- [incremental-clima.py](/Scripts/incremental-clima.py): Scraping y procesamiento de históricos AEMET.

### 3. Visualización y Análisis
- [Visualización-Datos.ipynb](/Visualización-Datos.ipynb): Análisis exploratorio (EDA) y Dashboard con Altair.

### 4. Memoria Técnica
- [Proyecto_demanda.pdf](/Proyecto_demanda.pdf): Documentación técnica del proyecto bajo metodología CRISP-DM.




