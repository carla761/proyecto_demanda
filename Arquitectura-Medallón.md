# Proyecto Integrador: Predictor de demanda para redes eléctricas
## Ingeniería de Datos - Arquitectura Medallón (Capas Bronce, Plata y Oro)
**Curso de Especialización en IA y Big Data - IES San Andrés**

Este cuaderno contiene el pipeline central de procesamiento utilizando **Apache Spark** y el ecosistema **AWS**. Aquí se documenta e implementa el flujo de datos completo: la ingesta y almacenamiento en crudo (Capa Bronce), la limpieza y normalización de formatos (Capa Plata), y el cruce final con *Feature Engineering* (Capa Oro) para generar el dataset predictivo maestro.

## 0.Configuración inicial y Test


```python
!pip install boto3
```

    Requirement already satisfied: boto3 in /usr/local/lib/python3.10/site-packages (1.43.10)
    Requirement already satisfied: s3transfer<0.18.0,>=0.17.0 in /usr/local/lib/python3.10/site-packages (from boto3) (0.17.0)
    Requirement already satisfied: botocore<1.44.0,>=1.43.10 in /usr/local/lib/python3.10/site-packages (from boto3) (1.43.10)
    Requirement already satisfied: jmespath<2.0.0,>=0.7.1 in /usr/local/lib/python3.10/site-packages (from boto3) (1.1.0)
    Requirement already satisfied: python-dateutil<3.0.0,>=2.1 in /usr/local/lib/python3.10/site-packages (from botocore<1.44.0,>=1.43.10->boto3) (2.9.0.post0)
    Requirement already satisfied: urllib3!=2.2.0,<3,>=1.25.4 in /usr/local/lib/python3.10/site-packages (from botocore<1.44.0,>=1.43.10->boto3) (2.6.3)
    Requirement already satisfied: six>=1.5 in /usr/local/lib/python3.10/site-packages (from python-dateutil<3.0.0,>=2.1->botocore<1.44.0,>=1.43.10->boto3) (1.17.0)
    [33mWARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv[0m[33m
    [0m
    [1m[[0m[34;49mnotice[0m[1;39;49m][0m[39;49m A new release of pip is available: [0m[31;49m23.0.1[0m[39;49m -> [0m[32;49m26.1.1[0m
    [1m[[0m[34;49mnotice[0m[1;39;49m][0m[39;49m To update, run: [0m[32;49mpip install --upgrade pip[0m



```python
import os
import boto3
from pyspark.sql import SparkSession

# Test AWS
try:
    s3 = boto3.client('s3')
    print("Conexión a AWS S3 exitosa.")
except Exception as e:
    print("Error en AWS:", e)

# Iniciar Spark
spark = (SparkSession.builder
         .appName("Proyecto_Integrador_Demanda")
         .master("spark://spark-master:7077")
         .getOrCreate()
        )
print("SparkSession iniciada correctamente.")
```

    Conexión a AWS S3 exitosa.


    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    26/05/22 15:08:17 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


    SparkSession iniciada correctamente.



```python
respuesta= s3.list_buckets()

# Recorremos la lista de diccionarios que nos da AWS
for bucket in respuesta['Buckets']:
    nombre = bucket['Name']
    print(f"Nombre del bucket: {nombre}")
```

    Nombre del bucket: aws-logs-211125762907-us-east-1
    Nombre del bucket: cag-bd-iessanandres-practicas-ut6
    Nombre del bucket: cag-iessanandres-bd-ia
    Nombre del bucket: cag-proyecto-demanda-bd
    Nombre del bucket: cag-proyecto-demanda-electrica-bd
    Nombre del bucket: smartagro-raw-data-cag


    26/05/22 15:08:29 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors



```python
BUCKET_NAME = "cag-proyecto-demanda-electrica-bd"
s3.create_bucket(Bucket=BUCKET_NAME)
```




    {'ResponseMetadata': {'RequestId': 'N2V06XKDP4EYSHQD',
      'HostId': 'kyoFZ5/86YPR8ZwYwBZhKEXlCeu2yq4ijxI/iALrb2NXEyTbeLMk1zW95Rnl/MNS1PeLotaUVII=',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amz-id-2': 'kyoFZ5/86YPR8ZwYwBZhKEXlCeu2yq4ijxI/iALrb2NXEyTbeLMk1zW95Rnl/MNS1PeLotaUVII=',
       'x-amz-request-id': 'N2V06XKDP4EYSHQD',
       'date': 'Tue, 19 May 2026 17:55:34 GMT',
       'location': '/cag-proyecto-demanda-electrica-bd',
       'x-amz-bucket-arn': 'arn:aws:s3:::cag-proyecto-demanda-electrica-bd',
       'content-length': '0',
       'server': 'AmazonS3'},
      'RetryAttempts': 0},
     'Location': '/cag-proyecto-demanda-electrica-bd',
     'BucketArn': 'arn:aws:s3:::cag-proyecto-demanda-electrica-bd'}




```python
!apt-get install -y --no-install-recommends unar
```

    Reading package lists... Done
    Building dependency tree... Done
    Reading state information... Done
    The following additional packages will be installed:
      binutils binutils-common binutils-x86-64-linux-gnu bzip2 dpkg dpkg-dev
      fontconfig gnustep-base-common gnustep-base-runtime gnustep-common
      gnustep-multiarch gnutls-bin graphviz libabsl20240722 libann0 libaom3
      libavahi-client3 libavahi-common-data libavahi-common3 libavif16 libbinutils
      libcdt5 libcgraph6 libctf-nobfd0 libctf0 libdatrie1 libdav1d7 libdbus-1-3
      libde265-0 libdeflate0 libdpkg-perl libevent-2.1-7t64 libfribidi0 libgav1-1
      libgc1 libgcrypt20 libgd3 libgnustep-base1.31 libgnutls-dane0t64
      libgnutls30t64 libgomp1 libgpg-error0 libgprofng0 libgts-0.7-5t64 libgvc6
      libgvpr2 libheif-plugin-dav1d libheif-plugin-libde265 libheif1
      libimagequant0 libjansson4 libjbig0 liblab-gamut1 liblerc4 libltdl7 libobjc4
      libpango-1.0-0 libpangocairo-1.0-0 libpangoft2-1.0-0 libpathplan4
      librav1e0.7 libsframe1 libsharpyuv0 libsvtav1enc2 libthai-data libthai0
      libtiff6 libunbound8 libwavpack1 libwebp7 libxml2 libxslt1.1 libyuv0 make
      patch tzdata tzdata-legacy xz-utils
    Suggested packages:
      binutils-doc gprofng-gui binutils-gold bzip2-doc debian-keyring
      debian-tag2upload-keyring gsfonts graphviz-doc sq | sqop | rsop | gosop
      | pgpainless-cli | gpg-sq | gnupg gcc | c-compiler git bzr rng-tools
      libgd-tools dns-root-data libheif-plugin-ffmpegdec libheif-plugin-jpegdec
      libheif-plugin-jpegenc libheif-plugin-j2kdec libheif-plugin-j2kenc
      libheif-plugin-kvazaar libheif-plugin-rav1e libheif-plugin-svtenc make-doc
      ed diffutils-doc
    Recommended packages:
      build-essential gcc | c-compiler fakeroot sq | sqop | rsop | gosop
      | pgpainless-cli | gpg-sq | gnupg libalgorithm-merge-perl fonts-liberation
      dbus libfile-fcntllock-perl liblocale-gettext-perl libgpg-error-l10n
      libgts-bin libheif-plugin-x265 libheif-plugin-aomenc
    The following NEW packages will be installed:
      binutils binutils-common binutils-x86-64-linux-gnu bzip2 dpkg-dev fontconfig
      gnustep-base-common gnustep-base-runtime gnustep-common gnustep-multiarch
      gnutls-bin graphviz libabsl20240722 libann0 libaom3 libavahi-client3
      libavahi-common-data libavahi-common3 libavif16 libbinutils libcdt5
      libcgraph6 libctf-nobfd0 libctf0 libdatrie1 libdav1d7 libdbus-1-3 libde265-0
      libdeflate0 libdpkg-perl libevent-2.1-7t64 libfribidi0 libgav1-1 libgc1
      libgcrypt20 libgd3 libgnustep-base1.31 libgnutls-dane0t64 libgomp1
      libgpg-error0 libgprofng0 libgts-0.7-5t64 libgvc6 libgvpr2
      libheif-plugin-dav1d libheif-plugin-libde265 libheif1 libimagequant0
      libjansson4 libjbig0 liblab-gamut1 liblerc4 libltdl7 libobjc4 libpango-1.0-0
      libpangocairo-1.0-0 libpangoft2-1.0-0 libpathplan4 librav1e0.7 libsframe1
      libsharpyuv0 libsvtav1enc2 libthai-data libthai0 libtiff6 libunbound8
      libwavpack1 libwebp7 libxml2 libxslt1.1 libyuv0 make patch tzdata-legacy
      unar xz-utils
    The following packages will be upgraded:
      dpkg libgnutls30t64 tzdata
    3 upgraded, 76 newly installed, 0 to remove and 27 not upgraded.
    Need to get 33.4 MB of archives.
    After this operation, 108 MB of additional disk space will be used.
    Get:1 http://deb.debian.org/debian trixie/main amd64 dpkg amd64 1.22.22 [1537 kB]
    Get:2 http://deb.debian.org/debian trixie/main amd64 tzdata all 2026b-0+deb13u1 [264 kB]
    Get:3 http://deb.debian.org/debian trixie/main amd64 bzip2 amd64 1.0.8-6 [40.5 kB]
    Get:4 http://deb.debian.org/debian trixie/main amd64 xz-utils amd64 5.8.1-1 [660 kB]
    Get:5 http://deb.debian.org/debian trixie/main amd64 libsframe1 amd64 2.44-3 [78.4 kB]
    Get:6 http://deb.debian.org/debian trixie/main amd64 binutils-common amd64 2.44-3 [2509 kB]
    Get:7 http://deb.debian.org/debian trixie/main amd64 libbinutils amd64 2.44-3 [534 kB]
    Get:8 http://deb.debian.org/debian trixie/main amd64 libgprofng0 amd64 2.44-3 [808 kB]
    Get:9 http://deb.debian.org/debian trixie/main amd64 libctf-nobfd0 amd64 2.44-3 [156 kB]
    Get:10 http://deb.debian.org/debian trixie/main amd64 libctf0 amd64 2.44-3 [88.6 kB]
    Get:11 http://deb.debian.org/debian trixie/main amd64 libjansson4 amd64 2.14-2+b3 [39.8 kB]
    Get:12 http://deb.debian.org/debian trixie/main amd64 binutils-x86-64-linux-gnu amd64 2.44-3 [1014 kB]
    Get:13 http://deb.debian.org/debian trixie/main amd64 binutils amd64 2.44-3 [265 kB]
    Get:14 http://deb.debian.org/debian trixie/main amd64 libdpkg-perl all 1.22.22 [651 kB]
    Get:15 http://deb.debian.org/debian trixie/main amd64 patch amd64 2.8-2 [134 kB]
    Get:16 http://deb.debian.org/debian trixie/main amd64 make amd64 4.4.1-2 [463 kB]
    Get:17 http://deb.debian.org/debian trixie/main amd64 dpkg-dev all 1.22.22 [1337 kB]
    Get:18 http://deb.debian.org/debian trixie/main amd64 fontconfig amd64 2.15.0-2.3 [463 kB]
    Get:19 http://deb.debian.org/debian trixie/main amd64 gnustep-common amd64 2.9.3-6 [122 kB]
    Get:20 http://deb.debian.org/debian trixie/main amd64 tzdata-legacy all 2026b-0+deb13u1 [183 kB]
    Get:21 http://deb.debian.org/debian trixie/main amd64 gnustep-base-common all 1.31.1-3 [337 kB]
    Get:22 http://deb.debian.org/debian trixie/main amd64 libann0 amd64 1.1.2+doc-9+b1 [25.1 kB]
    Get:23 http://deb.debian.org/debian trixie/main amd64 libcdt5 amd64 2.42.4-3 [40.3 kB]
    Get:24 http://deb.debian.org/debian trixie/main amd64 libcgraph6 amd64 2.42.4-3 [64.0 kB]
    Get:25 http://deb.debian.org/debian trixie/main amd64 libaom3 amd64 3.12.1-1 [1871 kB]
    Get:26 http://deb.debian.org/debian trixie/main amd64 libdav1d7 amd64 1.5.1-1 [559 kB]
    Get:27 http://deb.debian.org/debian trixie/main amd64 libabsl20240722 amd64 20240722.0-4 [492 kB]
    Get:28 http://deb.debian.org/debian trixie/main amd64 libgav1-1 amd64 0.19.0-3+b1 [353 kB]
    Get:29 http://deb.debian.org/debian trixie/main amd64 librav1e0.7 amd64 0.7.1-9+b2 [946 kB]
    Get:30 http://deb.debian.org/debian trixie/main amd64 libsvtav1enc2 amd64 2.3.0+dfsg-1 [2489 kB]
    Get:31 http://deb.debian.org/debian trixie/main amd64 libyuv0 amd64 0.0.1904.20250204-1 [174 kB]
    Get:32 http://deb.debian.org/debian trixie/main amd64 libavif16 amd64 1.2.1-1.2 [133 kB]
    Get:33 http://deb.debian.org/debian trixie/main amd64 libsharpyuv0 amd64 1.5.0-0.1 [116 kB]
    Get:34 http://deb.debian.org/debian trixie/main amd64 libheif-plugin-dav1d amd64 1.19.8-1 [11.7 kB]
    Get:35 http://deb.debian.org/debian trixie/main amd64 libde265-0 amd64 1.0.15-1+b3 [189 kB]
    Get:36 http://deb.debian.org/debian trixie/main amd64 libheif-plugin-libde265 amd64 1.19.8-1 [15.3 kB]
    Get:37 http://deb.debian.org/debian trixie/main amd64 libheif1 amd64 1.19.8-1 [520 kB]
    Get:38 http://deb.debian.org/debian trixie/main amd64 libgomp1 amd64 14.2.0-19 [137 kB]
    Get:39 http://deb.debian.org/debian trixie/main amd64 libimagequant0 amd64 2.18.0-1+b2 [35.2 kB]
    Get:40 http://deb.debian.org/debian trixie/main amd64 libdeflate0 amd64 1.23-2 [47.3 kB]
    Get:41 http://deb.debian.org/debian trixie/main amd64 libjbig0 amd64 2.1-6.1+b2 [32.1 kB]
    Get:42 http://deb.debian.org/debian trixie/main amd64 liblerc4 amd64 4.0.0+ds-5 [183 kB]
    Get:43 http://deb.debian.org/debian trixie/main amd64 libwebp7 amd64 1.5.0-0.1 [318 kB]
    Get:44 http://deb.debian.org/debian trixie/main amd64 libtiff6 amd64 4.7.0-3+deb13u2 [345 kB]
    Get:45 http://deb.debian.org/debian trixie/main amd64 libgd3 amd64 2.3.3-13 [126 kB]
    Get:46 http://deb.debian.org/debian trixie/main amd64 libgts-0.7-5t64 amd64 0.7.6+darcs121130-5.2+b1 [160 kB]
    Get:47 http://deb.debian.org/debian trixie/main amd64 libltdl7 amd64 2.5.4-4 [416 kB]
    Get:48 http://deb.debian.org/debian trixie/main amd64 libfribidi0 amd64 1.0.16-1 [26.5 kB]
    Get:49 http://deb.debian.org/debian trixie/main amd64 libthai-data all 0.1.29-2 [168 kB]
    Get:50 http://deb.debian.org/debian trixie/main amd64 libdatrie1 amd64 0.2.13-3+b1 [38.1 kB]
    Get:51 http://deb.debian.org/debian trixie/main amd64 libthai0 amd64 0.1.29-2+b1 [49.4 kB]
    Get:52 http://deb.debian.org/debian trixie/main amd64 libpango-1.0-0 amd64 1.56.3-1 [226 kB]
    Get:53 http://deb.debian.org/debian trixie/main amd64 libpangoft2-1.0-0 amd64 1.56.3-1 [55.6 kB]
    Get:54 http://deb.debian.org/debian trixie/main amd64 libpangocairo-1.0-0 amd64 1.56.3-1 [35.7 kB]
    Get:55 http://deb.debian.org/debian trixie/main amd64 libpathplan4 amd64 2.42.4-3 [42.6 kB]
    Get:56 http://deb.debian.org/debian trixie/main amd64 libgvc6 amd64 2.42.4-3 [686 kB]
    Get:57 http://deb.debian.org/debian trixie/main amd64 libgvpr2 amd64 2.42.4-3 [192 kB]
    Get:58 http://deb.debian.org/debian trixie/main amd64 liblab-gamut1 amd64 2.42.4-3 [198 kB]
    Get:59 http://deb.debian.org/debian trixie/main amd64 graphviz amd64 2.42.4-3 [621 kB]
    Get:60 http://deb.debian.org/debian trixie/main amd64 gnustep-multiarch amd64 2.9.3-6 [88.5 kB]
    Get:61 http://deb.debian.org/debian trixie/main amd64 libgnutls30t64 amd64 3.8.9-3+deb13u3 [1467 kB]
    Get:62 http://deb.debian.org/debian trixie/main amd64 libevent-2.1-7t64 amd64 2.1.12-stable-10+b1 [182 kB]
    Get:63 http://deb.debian.org/debian trixie/main amd64 libunbound8 amd64 1.22.0-2+deb13u2 [598 kB]
    Get:64 http://deb.debian.org/debian trixie/main amd64 libgnutls-dane0t64 amd64 3.8.9-3+deb13u3 [456 kB]
    Get:65 http://deb.debian.org/debian trixie/main amd64 gnutls-bin amd64 3.8.9-3+deb13u3 [693 kB]
    Get:66 http://deb.debian.org/debian trixie/main amd64 libavahi-common-data amd64 0.8-16 [112 kB]
    Get:67 http://deb.debian.org/debian trixie/main amd64 libavahi-common3 amd64 0.8-16 [44.2 kB]
    Get:68 http://deb.debian.org/debian trixie/main amd64 libdbus-1-3 amd64 1.16.2-2 [178 kB]
    Get:69 http://deb.debian.org/debian trixie/main amd64 libavahi-client3 amd64 0.8-16 [48.4 kB]
    Get:70 http://deb.debian.org/debian trixie/main amd64 libgc1 amd64 1:8.2.8-1 [247 kB]
    Get:71 http://deb.debian.org/debian trixie/main amd64 libobjc4 amd64 14.2.0-19 [43.3 kB]
    Get:72 http://deb.debian.org/debian trixie/main amd64 libxml2 amd64 2.12.7+dfsg+really2.9.14-2.1+deb13u2 [698 kB]
    Get:73 http://deb.debian.org/debian trixie/main amd64 libgpg-error0 amd64 1.51-4 [82.1 kB]
    Get:74 http://deb.debian.org/debian trixie/main amd64 libgcrypt20 amd64 1.11.0-7 [843 kB]
    Get:75 http://deb.debian.org/debian trixie/main amd64 libxslt1.1 amd64 1.1.35-1.2+deb13u3 [233 kB]
    Get:76 http://deb.debian.org/debian trixie/main amd64 libgnustep-base1.31 amd64 1.31.1-3 [1671 kB]
    Get:77 http://deb.debian.org/debian trixie/main amd64 gnustep-base-runtime amd64 1.31.1-3 [466 kB]
    Get:78 http://deb.debian.org/debian trixie/main amd64 libwavpack1 amd64 5.8.1-1 [92.0 kB]
    Get:79 http://deb.debian.org/debian trixie/main amd64 unar amd64 1.10.8+ds1-9 [1299 kB]
    Fetched 33.4 MB in 2s (13.8 MB/s)
    debconf: unable to initialize frontend: Dialog
    debconf: (No usable dialog-like program is installed, so the dialog based frontend cannot be used. at /usr/share/perl5/Debconf/FrontEnd/Dialog.pm line 79, <STDIN> line 79.)
    debconf: falling back to frontend: Readline
    Extracting templates from packages: 100%
    Preconfiguring packages ...
    (Reading database ... 45796 files and directories currently installed.)
    Preparing to unpack .../dpkg_1.22.22_amd64.deb ...
    Unpacking dpkg (1.22.22) over (1.22.21) ...
    Setting up dpkg (1.22.22) ...
    (Reading database ... 45796 files and directories currently installed.)
    Preparing to unpack .../00-tzdata_2026b-0+deb13u1_all.deb ...
    Unpacking tzdata (2026b-0+deb13u1) over (2025b-4+deb13u1) ...
    Selecting previously unselected package bzip2.
    Preparing to unpack .../01-bzip2_1.0.8-6_amd64.deb ...
    Unpacking bzip2 (1.0.8-6) ...
    Selecting previously unselected package xz-utils.
    Preparing to unpack .../02-xz-utils_5.8.1-1_amd64.deb ...
    Unpacking xz-utils (5.8.1-1) ...
    Selecting previously unselected package libsframe1:amd64.
    Preparing to unpack .../03-libsframe1_2.44-3_amd64.deb ...
    Unpacking libsframe1:amd64 (2.44-3) ...
    Selecting previously unselected package binutils-common:amd64.
    Preparing to unpack .../04-binutils-common_2.44-3_amd64.deb ...
    Unpacking binutils-common:amd64 (2.44-3) ...
    Selecting previously unselected package libbinutils:amd64.
    Preparing to unpack .../05-libbinutils_2.44-3_amd64.deb ...
    Unpacking libbinutils:amd64 (2.44-3) ...
    Selecting previously unselected package libgprofng0:amd64.
    Preparing to unpack .../06-libgprofng0_2.44-3_amd64.deb ...
    Unpacking libgprofng0:amd64 (2.44-3) ...
    Selecting previously unselected package libctf-nobfd0:amd64.
    Preparing to unpack .../07-libctf-nobfd0_2.44-3_amd64.deb ...
    Unpacking libctf-nobfd0:amd64 (2.44-3) ...
    Selecting previously unselected package libctf0:amd64.
    Preparing to unpack .../08-libctf0_2.44-3_amd64.deb ...
    Unpacking libctf0:amd64 (2.44-3) ...
    Selecting previously unselected package libjansson4:amd64.
    Preparing to unpack .../09-libjansson4_2.14-2+b3_amd64.deb ...
    Unpacking libjansson4:amd64 (2.14-2+b3) ...
    Selecting previously unselected package binutils-x86-64-linux-gnu.
    Preparing to unpack .../10-binutils-x86-64-linux-gnu_2.44-3_amd64.deb ...
    Unpacking binutils-x86-64-linux-gnu (2.44-3) ...
    Selecting previously unselected package binutils.
    Preparing to unpack .../11-binutils_2.44-3_amd64.deb ...
    Unpacking binutils (2.44-3) ...
    Selecting previously unselected package libdpkg-perl.
    Preparing to unpack .../12-libdpkg-perl_1.22.22_all.deb ...
    Unpacking libdpkg-perl (1.22.22) ...
    Selecting previously unselected package patch.
    Preparing to unpack .../13-patch_2.8-2_amd64.deb ...
    Unpacking patch (2.8-2) ...
    Selecting previously unselected package make.
    Preparing to unpack .../14-make_4.4.1-2_amd64.deb ...
    Unpacking make (4.4.1-2) ...
    Selecting previously unselected package dpkg-dev.
    Preparing to unpack .../15-dpkg-dev_1.22.22_all.deb ...
    Unpacking dpkg-dev (1.22.22) ...
    Selecting previously unselected package fontconfig.
    Preparing to unpack .../16-fontconfig_2.15.0-2.3_amd64.deb ...
    Unpacking fontconfig (2.15.0-2.3) ...
    Selecting previously unselected package gnustep-common.
    Preparing to unpack .../17-gnustep-common_2.9.3-6_amd64.deb ...
    Unpacking gnustep-common (2.9.3-6) ...
    Selecting previously unselected package tzdata-legacy.
    Preparing to unpack .../18-tzdata-legacy_2026b-0+deb13u1_all.deb ...
    Unpacking tzdata-legacy (2026b-0+deb13u1) ...
    Selecting previously unselected package gnustep-base-common.
    Preparing to unpack .../19-gnustep-base-common_1.31.1-3_all.deb ...
    Unpacking gnustep-base-common (1.31.1-3) ...
    Selecting previously unselected package libann0.
    Preparing to unpack .../20-libann0_1.1.2+doc-9+b1_amd64.deb ...
    Unpacking libann0 (1.1.2+doc-9+b1) ...
    Selecting previously unselected package libcdt5:amd64.
    Preparing to unpack .../21-libcdt5_2.42.4-3_amd64.deb ...
    Unpacking libcdt5:amd64 (2.42.4-3) ...
    Selecting previously unselected package libcgraph6:amd64.
    Preparing to unpack .../22-libcgraph6_2.42.4-3_amd64.deb ...
    Unpacking libcgraph6:amd64 (2.42.4-3) ...
    Selecting previously unselected package libaom3:amd64.
    Preparing to unpack .../23-libaom3_3.12.1-1_amd64.deb ...
    Unpacking libaom3:amd64 (3.12.1-1) ...
    Selecting previously unselected package libdav1d7:amd64.
    Preparing to unpack .../24-libdav1d7_1.5.1-1_amd64.deb ...
    Unpacking libdav1d7:amd64 (1.5.1-1) ...
    Selecting previously unselected package libabsl20240722:amd64.
    Preparing to unpack .../25-libabsl20240722_20240722.0-4_amd64.deb ...
    Unpacking libabsl20240722:amd64 (20240722.0-4) ...
    Selecting previously unselected package libgav1-1:amd64.
    Preparing to unpack .../26-libgav1-1_0.19.0-3+b1_amd64.deb ...
    Unpacking libgav1-1:amd64 (0.19.0-3+b1) ...
    Selecting previously unselected package librav1e0.7:amd64.
    Preparing to unpack .../27-librav1e0.7_0.7.1-9+b2_amd64.deb ...
    Unpacking librav1e0.7:amd64 (0.7.1-9+b2) ...
    Selecting previously unselected package libsvtav1enc2:amd64.
    Preparing to unpack .../28-libsvtav1enc2_2.3.0+dfsg-1_amd64.deb ...
    Unpacking libsvtav1enc2:amd64 (2.3.0+dfsg-1) ...
    Selecting previously unselected package libyuv0:amd64.
    Preparing to unpack .../29-libyuv0_0.0.1904.20250204-1_amd64.deb ...
    Unpacking libyuv0:amd64 (0.0.1904.20250204-1) ...
    Selecting previously unselected package libavif16:amd64.
    Preparing to unpack .../30-libavif16_1.2.1-1.2_amd64.deb ...
    Unpacking libavif16:amd64 (1.2.1-1.2) ...
    Selecting previously unselected package libsharpyuv0:amd64.
    Preparing to unpack .../31-libsharpyuv0_1.5.0-0.1_amd64.deb ...
    Unpacking libsharpyuv0:amd64 (1.5.0-0.1) ...
    Selecting previously unselected package libheif-plugin-dav1d:amd64.
    Preparing to unpack .../32-libheif-plugin-dav1d_1.19.8-1_amd64.deb ...
    Unpacking libheif-plugin-dav1d:amd64 (1.19.8-1) ...
    Selecting previously unselected package libde265-0:amd64.
    Preparing to unpack .../33-libde265-0_1.0.15-1+b3_amd64.deb ...
    Unpacking libde265-0:amd64 (1.0.15-1+b3) ...
    Selecting previously unselected package libheif-plugin-libde265:amd64.
    Preparing to unpack .../34-libheif-plugin-libde265_1.19.8-1_amd64.deb ...
    Unpacking libheif-plugin-libde265:amd64 (1.19.8-1) ...
    Selecting previously unselected package libheif1:amd64.
    Preparing to unpack .../35-libheif1_1.19.8-1_amd64.deb ...
    Unpacking libheif1:amd64 (1.19.8-1) ...
    Selecting previously unselected package libgomp1:amd64.
    Preparing to unpack .../36-libgomp1_14.2.0-19_amd64.deb ...
    Unpacking libgomp1:amd64 (14.2.0-19) ...
    Selecting previously unselected package libimagequant0:amd64.
    Preparing to unpack .../37-libimagequant0_2.18.0-1+b2_amd64.deb ...
    Unpacking libimagequant0:amd64 (2.18.0-1+b2) ...
    Selecting previously unselected package libdeflate0:amd64.
    Preparing to unpack .../38-libdeflate0_1.23-2_amd64.deb ...
    Unpacking libdeflate0:amd64 (1.23-2) ...
    Selecting previously unselected package libjbig0:amd64.
    Preparing to unpack .../39-libjbig0_2.1-6.1+b2_amd64.deb ...
    Unpacking libjbig0:amd64 (2.1-6.1+b2) ...
    Selecting previously unselected package liblerc4:amd64.
    Preparing to unpack .../40-liblerc4_4.0.0+ds-5_amd64.deb ...
    Unpacking liblerc4:amd64 (4.0.0+ds-5) ...
    Selecting previously unselected package libwebp7:amd64.
    Preparing to unpack .../41-libwebp7_1.5.0-0.1_amd64.deb ...
    Unpacking libwebp7:amd64 (1.5.0-0.1) ...
    Selecting previously unselected package libtiff6:amd64.
    Preparing to unpack .../42-libtiff6_4.7.0-3+deb13u2_amd64.deb ...
    Unpacking libtiff6:amd64 (4.7.0-3+deb13u2) ...
    Selecting previously unselected package libgd3:amd64.
    Preparing to unpack .../43-libgd3_2.3.3-13_amd64.deb ...
    Unpacking libgd3:amd64 (2.3.3-13) ...
    Selecting previously unselected package libgts-0.7-5t64:amd64.
    Preparing to unpack .../44-libgts-0.7-5t64_0.7.6+darcs121130-5.2+b1_amd64.deb ...
    Unpacking libgts-0.7-5t64:amd64 (0.7.6+darcs121130-5.2+b1) ...
    Selecting previously unselected package libltdl7:amd64.
    Preparing to unpack .../45-libltdl7_2.5.4-4_amd64.deb ...
    Unpacking libltdl7:amd64 (2.5.4-4) ...
    Selecting previously unselected package libfribidi0:amd64.
    Preparing to unpack .../46-libfribidi0_1.0.16-1_amd64.deb ...
    Unpacking libfribidi0:amd64 (1.0.16-1) ...
    Selecting previously unselected package libthai-data.
    Preparing to unpack .../47-libthai-data_0.1.29-2_all.deb ...
    Unpacking libthai-data (0.1.29-2) ...
    Selecting previously unselected package libdatrie1:amd64.
    Preparing to unpack .../48-libdatrie1_0.2.13-3+b1_amd64.deb ...
    Unpacking libdatrie1:amd64 (0.2.13-3+b1) ...
    Selecting previously unselected package libthai0:amd64.
    Preparing to unpack .../49-libthai0_0.1.29-2+b1_amd64.deb ...
    Unpacking libthai0:amd64 (0.1.29-2+b1) ...
    Selecting previously unselected package libpango-1.0-0:amd64.
    Preparing to unpack .../50-libpango-1.0-0_1.56.3-1_amd64.deb ...
    Unpacking libpango-1.0-0:amd64 (1.56.3-1) ...
    Selecting previously unselected package libpangoft2-1.0-0:amd64.
    Preparing to unpack .../51-libpangoft2-1.0-0_1.56.3-1_amd64.deb ...
    Unpacking libpangoft2-1.0-0:amd64 (1.56.3-1) ...
    Selecting previously unselected package libpangocairo-1.0-0:amd64.
    Preparing to unpack .../52-libpangocairo-1.0-0_1.56.3-1_amd64.deb ...
    Unpacking libpangocairo-1.0-0:amd64 (1.56.3-1) ...
    Selecting previously unselected package libpathplan4:amd64.
    Preparing to unpack .../53-libpathplan4_2.42.4-3_amd64.deb ...
    Unpacking libpathplan4:amd64 (2.42.4-3) ...
    Selecting previously unselected package libgvc6.
    Preparing to unpack .../54-libgvc6_2.42.4-3_amd64.deb ...
    Unpacking libgvc6 (2.42.4-3) ...
    Selecting previously unselected package libgvpr2:amd64.
    Preparing to unpack .../55-libgvpr2_2.42.4-3_amd64.deb ...
    Unpacking libgvpr2:amd64 (2.42.4-3) ...
    Selecting previously unselected package liblab-gamut1:amd64.
    Preparing to unpack .../56-liblab-gamut1_2.42.4-3_amd64.deb ...
    Unpacking liblab-gamut1:amd64 (2.42.4-3) ...
    Selecting previously unselected package graphviz.
    Preparing to unpack .../57-graphviz_2.42.4-3_amd64.deb ...
    Unpacking graphviz (2.42.4-3) ...
    Selecting previously unselected package gnustep-multiarch:amd64.
    Preparing to unpack .../58-gnustep-multiarch_2.9.3-6_amd64.deb ...
    Unpacking gnustep-multiarch:amd64 (2.9.3-6) ...
    Preparing to unpack .../59-libgnutls30t64_3.8.9-3+deb13u3_amd64.deb ...
    Unpacking libgnutls30t64:amd64 (3.8.9-3+deb13u3) over (3.8.9-3+deb13u2) ...
    Selecting previously unselected package libevent-2.1-7t64:amd64.
    Preparing to unpack .../60-libevent-2.1-7t64_2.1.12-stable-10+b1_amd64.deb ...
    Unpacking libevent-2.1-7t64:amd64 (2.1.12-stable-10+b1) ...
    Selecting previously unselected package libunbound8:amd64.
    Preparing to unpack .../61-libunbound8_1.22.0-2+deb13u2_amd64.deb ...
    Unpacking libunbound8:amd64 (1.22.0-2+deb13u2) ...
    Selecting previously unselected package libgnutls-dane0t64:amd64.
    Preparing to unpack .../62-libgnutls-dane0t64_3.8.9-3+deb13u3_amd64.deb ...
    Unpacking libgnutls-dane0t64:amd64 (3.8.9-3+deb13u3) ...
    Selecting previously unselected package gnutls-bin.
    Preparing to unpack .../63-gnutls-bin_3.8.9-3+deb13u3_amd64.deb ...
    Unpacking gnutls-bin (3.8.9-3+deb13u3) ...
    Selecting previously unselected package libavahi-common-data:amd64.
    Preparing to unpack .../64-libavahi-common-data_0.8-16_amd64.deb ...
    Unpacking libavahi-common-data:amd64 (0.8-16) ...
    Selecting previously unselected package libavahi-common3:amd64.
    Preparing to unpack .../65-libavahi-common3_0.8-16_amd64.deb ...
    Unpacking libavahi-common3:amd64 (0.8-16) ...
    Selecting previously unselected package libdbus-1-3:amd64.
    Preparing to unpack .../66-libdbus-1-3_1.16.2-2_amd64.deb ...
    Unpacking libdbus-1-3:amd64 (1.16.2-2) ...
    Selecting previously unselected package libavahi-client3:amd64.
    Preparing to unpack .../67-libavahi-client3_0.8-16_amd64.deb ...
    Unpacking libavahi-client3:amd64 (0.8-16) ...
    Selecting previously unselected package libgc1:amd64.
    Preparing to unpack .../68-libgc1_1%3a8.2.8-1_amd64.deb ...
    Unpacking libgc1:amd64 (1:8.2.8-1) ...
    Selecting previously unselected package libobjc4:amd64.
    Preparing to unpack .../69-libobjc4_14.2.0-19_amd64.deb ...
    Unpacking libobjc4:amd64 (14.2.0-19) ...
    Selecting previously unselected package libxml2:amd64.
    Preparing to unpack .../70-libxml2_2.12.7+dfsg+really2.9.14-2.1+deb13u2_amd64.deb ...
    Unpacking libxml2:amd64 (2.12.7+dfsg+really2.9.14-2.1+deb13u2) ...
    Selecting previously unselected package libgpg-error0:amd64.
    Preparing to unpack .../71-libgpg-error0_1.51-4_amd64.deb ...
    Unpacking libgpg-error0:amd64 (1.51-4) ...
    Selecting previously unselected package libgcrypt20:amd64.
    Preparing to unpack .../72-libgcrypt20_1.11.0-7_amd64.deb ...
    Unpacking libgcrypt20:amd64 (1.11.0-7) ...
    Selecting previously unselected package libxslt1.1:amd64.
    Preparing to unpack .../73-libxslt1.1_1.1.35-1.2+deb13u3_amd64.deb ...
    Unpacking libxslt1.1:amd64 (1.1.35-1.2+deb13u3) ...
    Selecting previously unselected package libgnustep-base1.31:amd64.
    Preparing to unpack .../74-libgnustep-base1.31_1.31.1-3_amd64.deb ...
    Unpacking libgnustep-base1.31:amd64 (1.31.1-3) ...
    Selecting previously unselected package gnustep-base-runtime.
    Preparing to unpack .../75-gnustep-base-runtime_1.31.1-3_amd64.deb ...
    Unpacking gnustep-base-runtime (1.31.1-3) ...
    Selecting previously unselected package libwavpack1:amd64.
    Preparing to unpack .../76-libwavpack1_5.8.1-1_amd64.deb ...
    Unpacking libwavpack1:amd64 (5.8.1-1) ...
    Selecting previously unselected package unar.
    Preparing to unpack .../77-unar_1.10.8+ds1-9_amd64.deb ...
    Unpacking unar (1.10.8+ds1-9) ...
    Setting up libgnutls30t64:amd64 (3.8.9-3+deb13u3) ...
    Setting up libsharpyuv0:amd64 (1.5.0-0.1) ...
    Setting up libaom3:amd64 (3.12.1-1) ...
    Setting up fontconfig (2.15.0-2.3) ...
    Regenerating fonts cache... done.
    Setting up gnustep-common (2.9.3-6) ...
    Setting up liblerc4:amd64 (4.0.0+ds-5) ...
    Setting up libgpg-error0:amd64 (1.51-4) ...
    Setting up libdatrie1:amd64 (0.2.13-3+b1) ...
    Setting up liblab-gamut1:amd64 (2.42.4-3) ...
    Setting up binutils-common:amd64 (2.44-3) ...
    Setting up libdeflate0:amd64 (1.23-2) ...
    Setting up libctf-nobfd0:amd64 (2.44-3) ...
    Setting up libevent-2.1-7t64:amd64 (2.1.12-stable-10+b1) ...
    Setting up libgcrypt20:amd64 (1.11.0-7) ...
    Setting up libgomp1:amd64 (14.2.0-19) ...
    Setting up libabsl20240722:amd64 (20240722.0-4) ...
    Setting up bzip2 (1.0.8-6) ...
    Setting up libjbig0:amd64 (2.1-6.1+b2) ...
    Setting up libsframe1:amd64 (2.44-3) ...
    Setting up libjansson4:amd64 (2.14-2+b3) ...
    Setting up tzdata (2026b-0+deb13u1) ...
    debconf: unable to initialize frontend: Dialog
    debconf: (No usable dialog-like program is installed, so the dialog based frontend cannot be used. at /usr/share/perl5/Debconf/FrontEnd/Dialog.pm line 79.)
    debconf: falling back to frontend: Readline
    
    Current default time zone: 'Etc/UTC'
    Local time is now:      Tue May 19 18:18:11 UTC 2026.
    Universal Time is now:  Tue May 19 18:18:11 UTC 2026.
    Run 'dpkg-reconfigure tzdata' if you wish to change it.
    
    Setting up libunbound8:amd64 (1.22.0-2+deb13u2) ...
    Setting up libgnutls-dane0t64:amd64 (3.8.9-3+deb13u3) ...
    Setting up make (4.4.1-2) ...
    Setting up libsvtav1enc2:amd64 (2.3.0+dfsg-1) ...
    Setting up gnustep-multiarch:amd64 (2.9.3-6) ...
    Setting up libpathplan4:amd64 (2.42.4-3) ...
    Setting up libavahi-common-data:amd64 (0.8-16) ...
    Setting up libann0 (1.1.2+doc-9+b1) ...
    Setting up libdbus-1-3:amd64 (1.16.2-2) ...
    Setting up xz-utils (5.8.1-1) ...
    update-alternatives: using /usr/bin/xz to provide /usr/bin/lzma (lzma) in auto mode
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzma.1.gz because associated file /usr/share/man/man1/xz.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/unlzma.1.gz because associated file /usr/share/man/man1/unxz.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzcat.1.gz because associated file /usr/share/man/man1/xzcat.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzmore.1.gz because associated file /usr/share/man/man1/xzmore.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzless.1.gz because associated file /usr/share/man/man1/xzless.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzdiff.1.gz because associated file /usr/share/man/man1/xzdiff.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzcmp.1.gz because associated file /usr/share/man/man1/xzcmp.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzgrep.1.gz because associated file /usr/share/man/man1/xzgrep.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzegrep.1.gz because associated file /usr/share/man/man1/xzegrep.1.gz (of link group lzma) doesn't exist
    update-alternatives: warning: skip creation of /usr/share/man/man1/lzfgrep.1.gz because associated file /usr/share/man/man1/xzfgrep.1.gz (of link group lzma) doesn't exist
    Setting up libfribidi0:amd64 (1.0.16-1) ...
    Setting up libimagequant0:amd64 (2.18.0-1+b2) ...
    Setting up patch (2.8-2) ...
    Setting up libgav1-1:amd64 (0.19.0-3+b1) ...
    Setting up libgc1:amd64 (1:8.2.8-1) ...
    Setting up libltdl7:amd64 (2.5.4-4) ...
    Setting up libdpkg-perl (1.22.22) ...
    Setting up libwebp7:amd64 (1.5.0-0.1) ...
    Setting up libdav1d7:amd64 (1.5.1-1) ...
    Setting up libwavpack1:amd64 (5.8.1-1) ...
    Setting up libtiff6:amd64 (4.7.0-3+deb13u2) ...
    Setting up librav1e0.7:amd64 (0.7.1-9+b2) ...
    Setting up libthai-data (0.1.29-2) ...
    Setting up libgts-0.7-5t64:amd64 (0.7.6+darcs121130-5.2+b1) ...
    Setting up libcdt5:amd64 (2.42.4-3) ...
    Setting up libcgraph6:amd64 (2.42.4-3) ...
    Setting up libbinutils:amd64 (2.44-3) ...
    Setting up libde265-0:amd64 (1.0.15-1+b3) ...
    Setting up libyuv0:amd64 (0.0.1904.20250204-1) ...
    Setting up libxml2:amd64 (2.12.7+dfsg+really2.9.14-2.1+deb13u2) ...
    Setting up libctf0:amd64 (2.44-3) ...
    Setting up gnutls-bin (3.8.9-3+deb13u3) ...
    Setting up libavif16:amd64 (1.2.1-1.2) ...
    Setting up libavahi-common3:amd64 (0.8-16) ...
    Setting up libobjc4:amd64 (14.2.0-19) ...
    Setting up tzdata-legacy (2026b-0+deb13u1) ...
    Setting up libthai0:amd64 (0.1.29-2+b1) ...
    Setting up libgprofng0:amd64 (2.44-3) ...
    Setting up gnustep-base-common (1.31.1-3) ...
    Setting up libgvpr2:amd64 (2.42.4-3) ...
    Setting up libxslt1.1:amd64 (1.1.35-1.2+deb13u3) ...
    Setting up libavahi-client3:amd64 (0.8-16) ...
    Setting up binutils-x86-64-linux-gnu (2.44-3) ...
    Setting up libpango-1.0-0:amd64 (1.56.3-1) ...
    Setting up binutils (2.44-3) ...
    Setting up dpkg-dev (1.22.22) ...
    Setting up libgnustep-base1.31:amd64 (1.31.1-3) ...
    Setting up libpangoft2-1.0-0:amd64 (1.56.3-1) ...
    Setting up libpangocairo-1.0-0:amd64 (1.56.3-1) ...
    Setting up libheif-plugin-dav1d:amd64 (1.19.8-1) ...
    Setting up libheif-plugin-libde265:amd64 (1.19.8-1) ...
    Setting up libheif1:amd64 (1.19.8-1) ...
    Setting up libgd3:amd64 (2.3.3-13) ...
    Setting up libgvc6 (2.42.4-3) ...
    Setting up graphviz (2.42.4-3) ...
    Setting up gnustep-base-runtime (1.31.1-3) ...
    Setting up unar (1.10.8+ds1-9) ...
    Processing triggers for libc-bin (2.41-12+deb13u1) ...



```python
!pip install pandas
```

    Requirement already satisfied: pandas in /usr/local/lib/python3.10/site-packages (2.3.3)
    Requirement already satisfied: pytz>=2020.1 in /usr/local/lib/python3.10/site-packages (from pandas) (2026.2)
    Requirement already satisfied: numpy>=1.22.4 in /usr/local/lib/python3.10/site-packages (from pandas) (2.2.6)
    Requirement already satisfied: python-dateutil>=2.8.2 in /usr/local/lib/python3.10/site-packages (from pandas) (2.9.0.post0)
    Requirement already satisfied: tzdata>=2022.7 in /usr/local/lib/python3.10/site-packages (from pandas) (2025.3)
    Requirement already satisfied: six>=1.5 in /usr/local/lib/python3.10/site-packages (from python-dateutil>=2.8.2->pandas) (1.17.0)
    [33mWARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv[0m[33m
    [0m
    [1m[[0m[34;49mnotice[0m[1;39;49m][0m[39;49m A new release of pip is available: [0m[31;49m23.0.1[0m[39;49m -> [0m[32;49m26.1.1[0m
    [1m[[0m[34;49mnotice[0m[1;39;49m][0m[39;49m To update, run: [0m[32;49mpip install --upgrade pip[0m


## 1. Capa Bronce - Carga Historica -> S3

### 1.1 Fuente ESIOS


```python
import json
import requests
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# --- CONFIGURACIÓN S3 ---
NOMBRE_BUCKET = BUCKET_NAME
s3 = boto3.client('s3')

def existe_en_s3(bucket, key):
    """
    Comprueba si un archivo ya existe en S3 para evitar descargas duplicadas.
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def guardar_mes_s3(mes_id, datos_mes):
    """
    Sube el JSON del mes directamente a S3 aplicando la arquitectura Medallón.
    """
    year, month = mes_id.split("-")
    s3_key = f"bronce/demanda/year={year}/month={month}/{mes_id}.json"
    
    s3.put_object(
        Bucket=NOMBRE_BUCKET,
        Key=s3_key,
        Body=json.dumps(datos_mes, ensure_ascii=False, indent=2),
        ContentType='application/json'
    )
    return s3_key

def obtener_demanda_json(inicio, fin):
    """
    Extrae los datos de la API de ESIOS.
    """
    url = "https://apidatos.ree.es/es/datos/balance/balance-electrico"
    params = {
        "start_date": inicio,
        "end_date": fin,
        "time_trunc": "day"
    }

    r = requests.get(url, params=params)
    if r.status_code != 200:
        print(f"Error en la API (Código {r.status_code})")
        return []
        
    data = r.json()

    for grupo in data.get("included", []):
        if grupo.get("type") == "Demanda":
            for sub in grupo["attributes"].get("content", []):
                if sub.get("type") == "Demanda en b.c.":
                    valores = sub["attributes"]["values"]
                    return [
                        {"datetime": v["datetime"], "value": float(v["value"])}
                        for v in valores
                    ]
    return []

def descargar_demanda_json_desde_2013():
    """
    Itera desde 2013 hasta la actualidad, comprobando y subiendo datos a S3.
    """
    fecha_inicio = datetime(2013, 1, 1)
    fecha_fin = datetime.now()
    fecha_actual = fecha_inicio

    nuevos_descargados = 0

    while fecha_actual < fecha_fin:
        inicio = fecha_actual.strftime("%Y-%m-%dT00:00")
        fin_mes = (fecha_actual + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)
        fin = min(fin_mes, fecha_fin).strftime("%Y-%m-%dT23:59")
        mes_id = inicio[:7]  # Formato: YYYY-MM

        # Definimos la ruta que tendrá en S3 para comprobar si ya existe
        year, month = mes_id.split("-")
        s3_key = f"bronce/demanda/year={year}/month={month}/{mes_id}.json"
        
        if existe_en_s3(NOMBRE_BUCKET, s3_key):
            print(f"Omitiendo demanda de {mes_id} (Ya existe en S3)")
        else:
            print(f"Descargando demanda de {mes_id}...")
            datos_mes = obtener_demanda_json(inicio, fin)
            
            if datos_mes:
                guardar_mes_s3(mes_id, datos_mes)
                nuevos_descargados += 1

        # Avanzar al siguiente mes
        fecha_actual = (fecha_actual + timedelta(days=32)).replace(day=1)
        
    print(f"\nProceso histórico finalizado. Se subieron {nuevos_descargados} meses nuevos a la Capa Bronce.")

# --- EJECUCIÓN ---
if __name__ == "__main__":
    descargar_demanda_json_desde_2013()
```

    Descargando demanda de 2013-01...
    Descargando demanda de 2013-02...
    Descargando demanda de 2013-03...
    Descargando demanda de 2013-04...
    Descargando demanda de 2013-05...
    Descargando demanda de 2013-06...
    Descargando demanda de 2013-07...
    Descargando demanda de 2013-08...
    Descargando demanda de 2013-09...
    Descargando demanda de 2013-10...
    Descargando demanda de 2013-11...
    Descargando demanda de 2013-12...
    Descargando demanda de 2014-01...
    Descargando demanda de 2014-02...
    Descargando demanda de 2014-03...
    Descargando demanda de 2014-04...
    Descargando demanda de 2014-05...
    Descargando demanda de 2014-06...
    Descargando demanda de 2014-07...
    Descargando demanda de 2014-08...
    Descargando demanda de 2014-09...
    Descargando demanda de 2014-10...
    Descargando demanda de 2014-11...
    Descargando demanda de 2014-12...
    Descargando demanda de 2015-01...
    Descargando demanda de 2015-02...
    Descargando demanda de 2015-03...
    Descargando demanda de 2015-04...
    Descargando demanda de 2015-05...
    Descargando demanda de 2015-06...
    Descargando demanda de 2015-07...
    Descargando demanda de 2015-08...
    Descargando demanda de 2015-09...
    Descargando demanda de 2015-10...
    Descargando demanda de 2015-11...
    Descargando demanda de 2015-12...
    Descargando demanda de 2016-01...
    Descargando demanda de 2016-02...
    Descargando demanda de 2016-03...
    Descargando demanda de 2016-04...
    Descargando demanda de 2016-05...
    Descargando demanda de 2016-06...
    Descargando demanda de 2016-07...
    Descargando demanda de 2016-08...
    Descargando demanda de 2016-09...
    Descargando demanda de 2016-10...
    Descargando demanda de 2016-11...
    Descargando demanda de 2016-12...
    Descargando demanda de 2017-01...
    Descargando demanda de 2017-02...
    Descargando demanda de 2017-03...
    Descargando demanda de 2017-04...
    Descargando demanda de 2017-05...
    Descargando demanda de 2017-06...
    Descargando demanda de 2017-07...
    Descargando demanda de 2017-08...
    Descargando demanda de 2017-09...
    Descargando demanda de 2017-10...
    Descargando demanda de 2017-11...
    Descargando demanda de 2017-12...
    Descargando demanda de 2018-01...
    Descargando demanda de 2018-02...
    Descargando demanda de 2018-03...
    Descargando demanda de 2018-04...
    Descargando demanda de 2018-05...
    Descargando demanda de 2018-06...
    Descargando demanda de 2018-07...
    Descargando demanda de 2018-08...
    Descargando demanda de 2018-09...
    Descargando demanda de 2018-10...
    Descargando demanda de 2018-11...
    Descargando demanda de 2018-12...
    Descargando demanda de 2019-01...
    Descargando demanda de 2019-02...
    Descargando demanda de 2019-03...
    Descargando demanda de 2019-04...
    Descargando demanda de 2019-05...
    Descargando demanda de 2019-06...
    Descargando demanda de 2019-07...
    Descargando demanda de 2019-08...
    Descargando demanda de 2019-09...
    Descargando demanda de 2019-10...
    Descargando demanda de 2019-11...
    Descargando demanda de 2019-12...
    Descargando demanda de 2020-01...
    Descargando demanda de 2020-02...
    Descargando demanda de 2020-03...
    Descargando demanda de 2020-04...
    Descargando demanda de 2020-05...
    Descargando demanda de 2020-06...
    Descargando demanda de 2020-07...
    Descargando demanda de 2020-08...
    Descargando demanda de 2020-09...
    Descargando demanda de 2020-10...
    Descargando demanda de 2020-11...
    Descargando demanda de 2020-12...
    Descargando demanda de 2021-01...
    Descargando demanda de 2021-02...
    Descargando demanda de 2021-03...
    Descargando demanda de 2021-04...
    Descargando demanda de 2021-05...
    Descargando demanda de 2021-06...
    Descargando demanda de 2021-07...
    Descargando demanda de 2021-08...
    Descargando demanda de 2021-09...
    Descargando demanda de 2021-10...
    Descargando demanda de 2021-11...
    Descargando demanda de 2021-12...
    Descargando demanda de 2022-01...
    Descargando demanda de 2022-02...
    Descargando demanda de 2022-03...
    Descargando demanda de 2022-04...
    Descargando demanda de 2022-05...
    Descargando demanda de 2022-06...
    Descargando demanda de 2022-07...
    Descargando demanda de 2022-08...
    Descargando demanda de 2022-09...
    Descargando demanda de 2022-10...
    Descargando demanda de 2022-11...
    Descargando demanda de 2022-12...
    Descargando demanda de 2023-01...
    Descargando demanda de 2023-02...
    Descargando demanda de 2023-03...
    Descargando demanda de 2023-04...
    Descargando demanda de 2023-05...
    Descargando demanda de 2023-06...
    Descargando demanda de 2023-07...
    Descargando demanda de 2023-08...
    Descargando demanda de 2023-09...
    Descargando demanda de 2023-10...
    Descargando demanda de 2023-11...
    Descargando demanda de 2023-12...
    Descargando demanda de 2024-01...
    Descargando demanda de 2024-02...
    Descargando demanda de 2024-03...
    Descargando demanda de 2024-04...
    Descargando demanda de 2024-05...
    Descargando demanda de 2024-06...
    Descargando demanda de 2024-07...
    Descargando demanda de 2024-08...
    Descargando demanda de 2024-09...
    Descargando demanda de 2024-10...
    Descargando demanda de 2024-11...
    Descargando demanda de 2024-12...
    Descargando demanda de 2025-01...
    Descargando demanda de 2025-02...
    Descargando demanda de 2025-03...
    Descargando demanda de 2025-04...
    Descargando demanda de 2025-05...
    Descargando demanda de 2025-06...
    Descargando demanda de 2025-07...
    Descargando demanda de 2025-08...
    Descargando demanda de 2025-09...
    Descargando demanda de 2025-10...
    Descargando demanda de 2025-11...
    Descargando demanda de 2025-12...
    Descargando demanda de 2026-01...
    Descargando demanda de 2026-02...
    Descargando demanda de 2026-03...
    Descargando demanda de 2026-04...
    Descargando demanda de 2026-05...
    
    Proceso histórico finalizado. Se subieron 161 meses nuevos a la Capa Bronce.


### 1.2 Fuente histórico de clima


```python
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
```

    Buscando archivos .rar en la web...
       -> Encontrados 155 meses históricos.
    Descargando y extrayendo (lógica de calendario exacto activada)...
     -> Descargando/Reparando: Aemet2013-05 (Tiene 25 / Faltan hasta 31)
     -> Descargando/Reparando: Aemet2013-11 (Tiene 29 / Faltan hasta 30)
     -> Descargando/Reparando: Aemet2014-04 (Tiene 29 / Faltan hasta 30)
     -> Descargando/Reparando: Aemet2014-09 (Tiene 29 / Faltan hasta 30)
     -> Descargando/Reparando: Aemet2016-07 (Tiene 30 / Faltan hasta 31)
     -> Descargando/Reparando: Aemet2017-02 (Tiene 27 / Faltan hasta 28)
     -> Descargando/Reparando: Aemet2020-03 (Tiene 28 / Faltan hasta 31)
     -> Actualizando mes en curso: Aemet2026-03
    Subiendo archivos XLS a Amazon S3...
    
    Proceso histórico finalizado. Se subieron 4159 archivos XLS nuevos a S3.


### 1.3 Fuente de Festivos


```python
import requests
import json
import boto3

# --- 1.CONFIGURACIÓN ---
API_KEY = "Xb162V3SZvCnbe7DKGgVvPVhn8kQNAFX"
COUNTRY = "ES"
NOMBRE_BUCKET = BUCKET_NAME

# Cliente S3
s3 = boto3.client('s3')

# --- 2. FUNCIONES AUXILIARES ---
def archivo_existe_en_s3(bucket, key):
    """Comprueba si el archivo ya está en AWS para no repetir trabajo."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def get_festivos(year):
    """Llama a la API y devuelve los datos en la memoria RAM."""
    url = "https://calendarific.com/api/v2/holidays"
    params = {
        "api_key": API_KEY,
        "country": COUNTRY,
        "year": year
    }
    resp = requests.get(url, params=params)
    if resp.status_code == 200:
        return resp.json()
    else:
        print(f"Error en API para el año {year}: {resp.status_code}")
        return None

def ingesta_directa_s3():
    print("Iniciando ingesta a S3...")
    archivos_subidos = 0
    
    for year in range(2013, 2027):
        # Definir ruta destino particionada en S3
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
            s3.put_object(
                Bucket=NOMBRE_BUCKET,
                Key=s3_key,
                Body=json.dumps(data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            print(f" Subido a S3: {s3_key}")
            archivos_subidos += 1
            
    print(f"\nProceso histórico finalizado. Se subieron {archivos_subidos} años nuevos a S3.")

# --- 3. EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    ingesta_directa_s3()
```

    Iniciando ingesta a S3...
    -> Omitiendo 2013 (Ya existe en S3)
    -> Omitiendo 2014 (Ya existe en S3)
    -> Omitiendo 2015 (Ya existe en S3)
    -> Omitiendo 2016 (Ya existe en S3)
    -> Omitiendo 2017 (Ya existe en S3)
    -> Omitiendo 2018 (Ya existe en S3)
    -> Omitiendo 2019 (Ya existe en S3)
    -> Omitiendo 2020 (Ya existe en S3)
    -> Omitiendo 2021 (Ya existe en S3)
    -> Omitiendo 2022 (Ya existe en S3)
    -> Omitiendo 2023 (Ya existe en S3)
    -> Omitiendo 2024 (Ya existe en S3)
    -> Omitiendo 2025 (Ya existe en S3)
    -> Omitiendo 2026 (Ya existe en S3)
    
    Proceso histórico finalizado. Se subieron 0 años nuevos a S3.


## 2. Capa plata: Limpieza de datos

### 2.2 Fuente ESIOS 


```python
import os
import shutil
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round

# --- CONFIGURACIÓN ---
BUCKET_NAME = "cag-proyecto-demanda-electrica-bd"
CARPETA_TEMP = "descargas_bronce_demanda"
CARPETA_PLATA = "plata/demanda_esios"
s3 = boto3.client('s3')

def procesar_plata_demanda():
    print("=== INICIANDO CAPA PLATA: DEMANDA ELÉCTRICA ===")
    if os.path.exists(CARPETA_TEMP): shutil.rmtree(CARPETA_TEMP)
    os.makedirs(CARPETA_TEMP, exist_ok=True)
    
    print("-> Descargando JSONs desde S3...")
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="bronce/demanda/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                s3.download_file(BUCKET_NAME, obj["Key"], os.path.join(CARPETA_TEMP, obj["Key"].split("/")[-1]))

    print("-> Limpiando con Apache Spark...")
    spark = SparkSession.builder.appName("Plata_Demanda").master("local[*]").getOrCreate()
    
    try:
        # Lectura multilínea activada para tu JSON
        df_crudo = spark.read.option("multiline", "true").json(f"{CARPETA_TEMP}/*.json")
        
        # Como es diario, solo sacamos fecha y megavatios totales
        df_limpio = df_crudo.select(
            to_date(col("datetime")).alias("fecha"),
            round(col("value").cast("float"), 2).alias("megavatios_dia")
        ).dropDuplicates()

        print(f"-> Guardando Parquet en '{CARPETA_PLATA}'...")
        df_limpio.coalesce(1).write.mode("overwrite").parquet(CARPETA_PLATA)
        print(f"¡Éxito! Registros guardados: {df_limpio.count()}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()
        if os.path.exists(CARPETA_TEMP): shutil.rmtree(CARPETA_TEMP)

if __name__ == "__main__":
    procesar_plata_demanda()
```

    === INICIANDO CAPA PLATA: DEMANDA ELÉCTRICA ===
    -> Descargando JSONs desde S3...
    -> Limpiando con Apache Spark...


                                                                                    

    -> Guardando Parquet en 'plata/demanda_esios'...


                                                                                    

    ¡Éxito! Registros guardados: 4887


#### 2.1.1 Subida a S3


```python
import os
import boto3

# --- CONFIGURACIÓN ---
BUCKET_NAME = "cag-proyecto-demanda-electrica-bd"
CARPETA_LOCAL_PLATA = "plata/demanda_esios"  
PREFIX_S3 = "plata/demanda/"                  

def subir_plata_a_s3():
    print("=== SUBIENDO CAPA PLATA A AMAZON S3 ===")
    s3 = boto3.client('s3')
    
    if not os.path.exists(CARPETA_LOCAL_PLATA):
        print(f"No se encuentra la carpeta local {CARPETA_LOCAL_PLATA}")
        return

    archivos_subidos = 0
    # Recorremos los archivos que ha creado Spark (.parquet, _SUCCESS, etc.)
    for root, dirs, files in os.walk(CARPETA_LOCAL_PLATA):
        for file in files:
            if file.endswith(".parquet") and not file.startswith("."):
                ruta_local = os.path.join(root, file)
            
                # Creamos la ruta de destino en S3
                ruta_s3 = os.path.join(PREFIX_S3, "demanda_limpia.parquet").replace("\\", "/")
                
                print(f"-> Subiendo {file} a S3...")
                s3.upload_file(ruta_local, BUCKET_NAME, ruta_s3)
                archivos_subidos += 1

    print(f"¡Capa Plata respaldada en S3 con éxito! Se han subido {archivos_subidos} archivos.")

if __name__ == "__main__":
    subir_plata_a_s3()
```

    === SUBIENDO CAPA PLATA A AMAZON S3 ===
    -> Subiendo part-00000-4fab0318-47df-4b13-9776-67ba2a4522f3-c000.snappy.parquet a S3...
    ¡Capa Plata respaldada en S3 con éxito! Se han subido 1 archivos.


### 2.2 Fuente historico clima


```python
import os
import shutil
import boto3
import io
import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date, avg, round

# --- 1. CONFIGURACIÓN ---
BUCKET_NAME = "cag-proyecto-demanda-electrica-bd"
CARPETA_TEMP = "descargas_bronce_clima"
CARPETA_PLATA = "plata/clima_final"

s3 = boto3.client('s3')

# --- 2. FUNCIONES ---
def preparar_carpetas():
    os.makedirs(CARPETA_TEMP, exist_ok=True)

def descargar_bronce():
    print("-> Descargando Excels de AEMET desde S3 (Modo Resumible)...")
    paginator = s3.get_paginator('list_objects_v2')
    archivos_nuevos = 0
    archivos_cache = 0
    
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="bronce/clima/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".xls") or obj["Key"].endswith(".xlsx"):
                nombre_archivo = obj["Key"].split("/")[-1]
                ruta_local = os.path.join(CARPETA_TEMP, nombre_archivo)
                
                if os.path.exists(ruta_local):
                    archivos_cache += 1
                    continue
                try:
                    s3.download_file(BUCKET_NAME, obj["Key"], ruta_local)
                    archivos_nuevos += 1
                except Exception as e:
                    print(f"Error temporal descargando {nombre_archivo}: {e}")
                    
    print(f"-> Descarga lista. Nuevos: {archivos_nuevos} | En caché: {archivos_cache}")

# --- Función para procesar binarios en paralelo ---
def procesar_xls_binario(datos_binarios):
    import io
    import pandas as pd
    import re
    ruta, contenido = datos_binarios
    try:
        # Extraemos la fecha del nombre del archivo (ej: Aemet2013-05-07.xls)
        match = re.search(r'(\d{4}-\d{2}-\d{2})', ruta)
        fecha_str = match.group(1) if match else None

        # Leemos el binario directamente en memoria saltando las cabeceras
        pdf = pd.read_excel(io.BytesIO(contenido), engine="xlrd", skiprows=4)
        
        pdf.rename(columns={
            'Estación': 'estacion',
            'Temperatura máxima (ºC)': 'temp_max',
            'Temperatura mínima (ºC)': 'temp_min',
            'Temperatura media (ºC)': 'temp_media',
            'Precipitación 00-24h (mm)': 'precipitacion_mm'
        }, inplace=True)

        cols = ['temp_max', 'temp_min', 'temp_media', 'precipitacion_mm']
        pdf = pdf[[c for c in cols if c in pdf.columns]]
        pdf['fecha_str'] = fecha_str
        
        # Convertimos todo a texto para que Spark no se queje de tipos mixtos
        return pdf.dropna(how='all', subset=cols).astype(str).to_dict("records")
    except Exception:
        return []

def transformar_y_guardar():
    print("-> Iniciando Spark con procesamiento distribuido de archivos...")
    spark = (
        SparkSession.builder
        .appName("Plata_Clima")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    
    try:
        rutas_archivos = f"file://{os.path.abspath(CARPETA_TEMP)}/*.xls*"
        
        print("-> Analizando Excels y limpiando textos sucios...")
        df_clima_raw = (
            spark.sparkContext
            .binaryFiles(rutas_archivos)
            .flatMap(procesar_xls_binario)
            .toDF()
        )
        
        if df_clima_raw.isEmpty():
            print("No se pudieron cargar los datos.")
            return

         #Limpieza de 'nan' y textos para matemáticas ---
        df_limpio_textos = (
            df_clima_raw
            .replace("nan", None)
            .withColumn("fecha", to_date(col("fecha_str")))
            .withColumn("temp_max_num", split(col("temp_max"), " ")[0].cast("float"))
            .withColumn("temp_min_num", split(col("temp_min"), " ")[0].cast("float"))
            .withColumn("temp_media_num", split(col("temp_media"), " ")[0].cast("float"))
            .withColumn("precipitacion_num", split(col("precipitacion_mm"), " ")[0].cast("float"))
        )

        print("-> Calculando media NACIONAL para cruzar con demanda eléctrica...")
        # Lógica de negocio
        df_final = df_limpio_textos.groupBy("fecha").agg(
            round(avg("temp_max_num"), 2).alias("temp_max_media"),
            round(avg("temp_min_num"), 2).alias("temp_min_media"),
            round(avg("temp_media_num"), 2).alias("temp_media_nacional"),
            round(avg("precipitacion_num"), 2).alias("precipitacion_media")
        ).dropna(subset=["fecha"]).orderBy("fecha")

        print(f"-> Guardando en '{CARPETA_PLATA}'...")
        if os.path.exists(CARPETA_PLATA):
            shutil.rmtree(CARPETA_PLATA)
            
        df_final.coalesce(1).write.mode("overwrite").parquet(CARPETA_PLATA)
        print(f"¡Éxito! Clima procesado: {df_final.count()} días únicos.")
        
        df_final.show(5)

    except Exception as e:
        print(f"Error crítico: {e}")
    finally:
        spark.stop()

def ejecutar_plata_clima():
    print("=== INICIANDO CAPA PLATA: CLIMA (VERSIÓN DISTRIBUIDA) ===")
    preparar_carpetas()
    descargar_bronce()
    transformar_y_guardar()

if __name__ == "__main__":
    ejecutar_plata_clima()
```

    === INICIANDO CAPA PLATA: CLIMA (VERSIÓN DISTRIBUIDA) ===
    -> Descargando Excels de AEMET desde S3 (Modo Resumible)...
    -> Descarga lista. Nuevos: 0 | En caché: 4704
    -> Iniciando Spark con procesamiento distribuido de archivos...
    -> Analizando Excels y limpiando textos sucios...


    26/05/21 21:46:46 WARN PythonRunner: Detected deadlock while completing task 0.0 in stage 0 (TID 0): Attempting to kill Python Worker
    26/05/21 21:46:50 WARN PythonRunner: Detected deadlock while completing task 0.0 in stage 1 (TID 1): Attempting to kill Python Worker
                                                                                    

    -> Calculando media NACIONAL para cruzar con demanda eléctrica...
    -> Guardando en 'plata/clima_final'...


    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!=====>      (8 + 1) / 9]
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 511 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 530 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 534 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 514 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!=====>      (8 + 1) / 9]
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 511 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 530 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 534 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 514 but only 511 available
                                                                                    

    ¡Éxito! Clima procesado: 4681 días únicos.


    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!=====>      (8 + 1) / 9]
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 511 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 530 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 520 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 534 but only 511 available
    WARNING *** File is truncated, or OLE2 MSAT is corrupt!!
    INFO: Trying to access sector 514 but only 511 available
                                                                                    

    +----------+--------------+--------------+-------------------+-------------------+
    |     fecha|temp_max_media|temp_min_media|temp_media_nacional|precipitacion_media|
    +----------+--------------+--------------+-------------------+-------------------+
    |2013-05-07|         23.62|         13.05|              18.34|               1.06|
    |2013-05-08|         23.31|         13.42|              18.37|               0.57|
    |2013-05-09|         22.01|         12.66|              17.34|               2.95|
    |2013-05-10|         21.77|         11.29|              16.53|               0.13|
    |2013-05-11|         21.47|          9.44|              15.46|               0.03|
    +----------+--------------+--------------+-------------------+-------------------+
    only showing top 5 rows
    


#### 2.2.1 Subida a S3 


```python
import os
import boto3

# --- CONFIGURACIÓN ---
BUCKET_NAME = "cag-proyecto-demanda-electrica-bd"
CARPETA_LOCAL_CLIMA = "plata/clima_final"
PREFIX_S3 = "plata/clima" 

s3 = boto3.client('s3')

def subir_clima_s3():
    print("=== INICIANDO SUBIDA DE CLIMA A S3 ===")
    archivos_subidos = 0
    
    # Escaneamos la carpeta local donde Spark guardó el Parquet
    for root, dirs, files in os.walk(CARPETA_LOCAL_CLIMA):
        for file in files:
            # Filtramos para coger solo el archivo de datos real
            if file.endswith(".parquet") and not file.startswith("."):
                ruta_local = os.path.join(root, file)
                
                # Construimos la ruta en S3 forzando el nombre limpio y estático
                ruta_s3 = os.path.join(PREFIX_S3, "clima_limpio.parquet").replace("\\", "/")
                
                print(f"-> Subiendo {file} a S3 como '{ruta_s3}'...")
                s3.upload_file(ruta_local, BUCKET_NAME, ruta_s3)
                archivos_subidos += 1
                
    if archivos_subidos > 0:
        print(f"¡Éxito! El archivo de clima ya está seguro en S3.")
    else:
        print("No se encontró ningún archivo Parquet válido para subir.")

if __name__ == "__main__":
    subir_clima_s3()
```

    === INICIANDO SUBIDA DE CLIMA A S3 ===
    -> Subiendo part-00000-85f250e2-b96b-4977-85a9-ca32b65c47e1-c000.snappy.parquet a S3 como 'plata/clima/clima_limpio.parquet'...
    ¡Éxito! El archivo de clima ya está seguro en S3.


### 2.3 Fuente festivos 


```python
import os
import sys
import json
import shutil
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# --- 1. CONFIGURACIÓN ---
BUCKET_NAME = "cag-proyecto-demanda-electrica-bd"
CARPETA_TEMP = "descargas_bronce_festivos"
CARPETA_PLATA = "plata/festivos"

s3 = boto3.client('s3')

# --- 2. FUNCIONES ---
def preparar_carpetas():
    # Mantenemos la carpeta para permitir la reanudación si hay cortes
    os.makedirs(CARPETA_TEMP, exist_ok=True)

def descargar_bronze():
    print("-> Descargando JSONs de Festivos desde S3 (Modo Resumible)...")
    paginator = s3.get_paginator('list_objects_v2')
    archivos_nuevos = 0
    archivos_cache = 0
    
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="bronce/festivos/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                nombre_archivo = obj["Key"].split("/")[-1]
                ruta_local = os.path.join(CARPETA_TEMP, nombre_archivo)
                
                # Tolerancia a fallos: saltar si ya existe
                if os.path.exists(ruta_local):
                    archivos_cache += 1
                    continue
                try:
                    s3.download_file(BUCKET_NAME, obj["Key"], ruta_local)
                    archivos_nuevos += 1
                except Exception as e:
                    print(f"Error temporal descargando {nombre_archivo}: {e}")
                    
    print(f"-> Descarga lista. Nuevos: {archivos_nuevos} | En caché: {archivos_cache}")

def transformar_y_guardar():
    print("-> Leyendo JSONs y aplanando datos con Pandas...")
    lista_df = []
    
    for archivo in os.listdir(CARPETA_TEMP):
        if archivo.endswith(".json"):
            ruta_completa = os.path.join(CARPETA_TEMP, archivo)
            with open(ruta_completa, 'r', encoding='utf-8') as f:
                try:
                    datos = json.load(f)
                    holidays = datos.get("response", {}).get("holidays", [])
                    if holidays:
                        df = pd.json_normalize(holidays)
                        lista_df.append(df)
                except Exception as e:
                    print(f"Omitiendo {archivo}: {e}")

    if not lista_df:
        print("No se encontraron datos válidos.")
        return

    df_total = pd.concat(lista_df, ignore_index=True)
    
    print("-> Aplicando filtros de negocio (Solo Festivos Reales)...")
    tipos_validos = ["National holiday", "Autonomous Community Holiday", "Common local holiday"]
    df_filtrado = df_total[df_total['primary_type'].isin(tipos_validos)].copy()
    
    # Limpiamos la fecha y las columnas
    df_filtrado['fecha_cruda'] = df_filtrado['date.iso'].astype(str).str[:10]
    df_filtrado.rename(columns={'name': 'festividad'}, inplace=True)
    df_final_pandas = df_filtrado[['fecha_cruda', 'festividad']]

    print("-> Iniciando Spark para estandarizar formato...")
    spark = (
        SparkSession.builder
        .appName("Plata_Festivos")
        .master("local[*]")
        .getOrCreate()
    )
    
    try:
        df_spark = spark.createDataFrame(df_final_pandas)
        
        # Formato fecha y eliminamos duplicados en caso de que coincidan dos fiestas
        df_limpio = df_spark.withColumn("fecha", to_date(col("fecha_cruda"))) \
                            .drop("fecha_cruda") \
                            .dropDuplicates(["fecha"])

        print(f"-> Guardando en '{CARPETA_PLATA}'...")
        if os.path.exists(CARPETA_PLATA):
            shutil.rmtree(CARPETA_PLATA)
            
        df_limpio.coalesce(1).write.mode("overwrite").parquet(CARPETA_PLATA)
        print(f"¡Éxito! Festivos procesados: {df_limpio.count()} días marcados.")
        
        df_limpio.show(5, truncate=False)

    except Exception as e:
        print(f"Error crítico en Spark: {e}")
    finally:
        spark.stop()

def ejecutar_plata_festivos():
    print("=== INICIANDO CAPA PLATA: FESTIVOS (VERSIÓN DEFINITIVA) ===")
    preparar_carpetas()
    descargar_bronze()
    transformar_y_guardar()

if __name__ == "__main__":
    ejecutar_plata_festivos()
```

    === INICIANDO CAPA PLATA: FESTIVOS (VERSIÓN DEFINITIVA) ===
    -> Descargando JSONs de Festivos desde S3 (Modo Resumible)...
    -> Descarga lista. Nuevos: 14 | En caché: 0
    -> Leyendo JSONs y aplanando datos con Pandas...
    -> Aplicando filtros de negocio (Solo Festivos Reales)...
    -> Iniciando Spark para estandarizar formato...
    -> Guardando en 'plata/festivos'...


                                                                                    

    ¡Éxito! Festivos procesados: 497 días marcados.
    +---------------------------+----------+
    |festividad                 |fecha     |
    +---------------------------+----------+
    |New Year's Day             |2013-01-01|
    |Epiphany                   |2013-01-06|
    |Epiphany Observed          |2013-01-07|
    |Day of Andalucía           |2013-02-28|
    |Day of the Balearic Islands|2013-03-01|
    +---------------------------+----------+
    only showing top 5 rows
    


#### 2.3.1 Subida a S3


```python
import os
import boto3

# --- CONFIGURACIÓN ---
BUCKET_NAME = "cag-proyecto-demanda-electrica-bd"
CARPETA_LOCAL_FESTIVOS = "plata/festivos"
PREFIX_S3 = "plata/festivos" 

s3 = boto3.client('s3')

def subir_festivos_s3():
    print("=== INICIANDO SUBIDA DE FESTIVOS A S3 ===")
    archivos_subidos = 0
    
    # Escaneamos la carpeta local donde Spark guardó el Parquet
    for root, dirs, files in os.walk(CARPETA_LOCAL_FESTIVOS):
        for file in files:
            # Filtramos para coger solo el archivo de datos real (ignoramos _SUCCESS, crc, etc.)
            if file.endswith(".parquet") and not file.startswith("."):
                ruta_local = os.path.join(root, file)
                
                # Construimos la ruta en S3 forzando el nombre limpio y estático
                ruta_s3 = os.path.join(PREFIX_S3, "festivos_limpios.parquet").replace("\\", "/")
                
                print(f"-> Subiendo {file} a S3 como '{ruta_s3}'...")
                s3.upload_file(ruta_local, BUCKET_NAME, ruta_s3)
                archivos_subidos += 1
                
    if archivos_subidos > 0:
        print(f"¡Éxito! El tablón de festivos ya está seguro en S3.")
    else:
        print("No se encontró ningún archivo Parquet válido para subir.")

if __name__ == "__main__":
    subir_festivos_s3()
```

    === INICIANDO SUBIDA DE FESTIVOS A S3 ===
    -> Subiendo part-00000-428d78ef-b929-47be-8e8a-141ebdb5686d-c000.snappy.parquet a S3 como 'plata/festivos/festivos_limpios.parquet'...
    ¡Éxito! El tablón de festivos ya está seguro en S3.


## 3. Capa oro


```python
import os
import sys
import shutil
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, dayofweek, month, year, round, quarter, lag, lit
from pyspark.sql.window import Window

# --- 1. CONFIGURACIÓN DE ENTORNO ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- 2. CONFIGURACIÓN DE RUTAS ---
BUCKET_NAME = "cag-proyecto-demanda-electrica-bd"

# Rutas en S3 de la Capa Plata
S3_PLATA_DEMANDA = "plata/demanda/demanda_limpia.parquet" 
S3_PLATA_CLIMA = "plata/clima/clima_limpio.parquet"
S3_PLATA_FESTIVOS = "plata/festivos/festivos_limpios.parquet"

# Rutas Locales
CARPETA_DESCARGAS = "descargas_oro_temp"
CARPETA_ORO_LOCAL = "oro/dataset_predictivo"
PREFIX_S3_ORO = "oro/dataset_predictivo.parquet"

s3 = boto3.client('s3')

def preparar_entorno():
    if os.path.exists(CARPETA_DESCARGAS):
        shutil.rmtree(CARPETA_DESCARGAS)
    os.makedirs(CARPETA_DESCARGAS, exist_ok=True)

def descargar_plata():
    print("-> Descargando las 3 fuentes limpias desde la Capa Plata en S3...")
    archivos = [
        (S3_PLATA_DEMANDA, "demanda.parquet"),
        (S3_PLATA_CLIMA, "clima.parquet"),
        (S3_PLATA_FESTIVOS, "festivos.parquet")
    ]
    
    for ruta_s3, nombre_local in archivos:
        ruta_destino = os.path.join(CARPETA_DESCARGAS, nombre_local)
        try:
            s3.download_file(BUCKET_NAME, ruta_s3, ruta_destino)
            print(f"   [OK] {nombre_local} descargado.")
        except Exception as e:
            print(f"   [ERROR] No se pudo descargar {ruta_s3}: {e}")
            sys.exit(1) # Si falta una pata, paramos el proceso

def crear_capa_oro():
    print("-> Levantando Spark para el cruce final...")
    spark = (
        SparkSession.builder
        .appName("Capa_Oro")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    
    try:
        # 1. Leer las tres tablas y renombrar la variable objetivo
        df_demanda = spark.read.parquet(os.path.join(CARPETA_DESCARGAS, "demanda.parquet")) \
                          .withColumnRenamed("megavatios_dia", "demanda")
                          
        df_clima = spark.read.parquet(os.path.join(CARPETA_DESCARGAS, "clima.parquet"))
        df_festivos = spark.read.parquet(os.path.join(CARPETA_DESCARGAS, "festivos.parquet"))

        print("-> Realizando LEFT JOIN y Feature Engineering...")
        
        # 2. Cruce Maestro
        df_oro = df_demanda.join(df_clima, "fecha", "left") \
                           .join(df_festivos, "fecha", "left")
        
        # Definimos la ventana temporal para poder calcular el consumo de hace 7 días
        # Le decimos explícitamente que todo va a una misma partición
        windowSpec = Window.partitionBy(lit(1)).orderBy("fecha")
        
        # 3. Feature Engineering (Creación de TODAS las variables del Diccionario de Datos)
        df_oro = df_oro \
            .withColumn("es_festivo", when(col("festividad").isNotNull(), 1).otherwise(0)) \
            .withColumn("festividad", when(col("festividad").isNull(), "Laborable/Normal").otherwise(col("festividad"))) \
            .withColumn("dia_semana", dayofweek(col("fecha"))) \
            .withColumn("mes", month(col("fecha"))) \
            .withColumn("anio", year(col("fecha"))) \
            .withColumn("trimestre", quarter(col("fecha"))) \
            .withColumn("es_fin_de_semana", when(col("dia_semana").isin(1, 7), 1).otherwise(0)) \
            .withColumn("rango_termico", round(col("temp_max_media") - col("temp_min_media"), 2)) \
            .withColumn("demanda_lag_7", lag("demanda", 7).over(windowSpec))
            
        # 4. Limpieza final: Ordenamos y quitamos la primera semana que genera nulos en el lag_7
        df_oro = df_oro.orderBy("fecha").filter(col("demanda_lag_7").isNotNull())

        print(f"-> Guardando Dataset Predictivo en '{CARPETA_ORO_LOCAL}'...")
        if os.path.exists(CARPETA_ORO_LOCAL):
            shutil.rmtree(CARPETA_ORO_LOCAL)
            
        df_oro.coalesce(1).write.mode("overwrite").parquet(CARPETA_ORO_LOCAL)
        
        total_filas = df_oro.count()
        print(f"¡Capa Oro finalizada! Tablón maestro generado con {total_filas} días.")
        
        # Mostramos esquema final y una muestra de los datos incluyendo las nuevas variables
        df_oro.printSchema()
        # Muestra todas las columnas, 5 filas, y sin recortar el texto
        df_oro.show(5, truncate=False)
        
    except Exception as e:
        print(f"Error crítico en el cruce de Spark: {e}")
    finally:
        spark.stop()

def subir_oro_s3():
    print("=== SUBIENDO DATASET PREDICTIVO A S3 ===")
    # Buscamos el archivo generado y lo subimos con nombre limpio
    for file in os.listdir(CARPETA_ORO_LOCAL):
        if file.endswith(".parquet") and not file.startswith("."):
            ruta_local = os.path.join(CARPETA_ORO_LOCAL, file)
            print(f"-> Subiendo a S3 como '{PREFIX_S3_ORO}'...")
            s3.upload_file(ruta_local, BUCKET_NAME, PREFIX_S3_ORO)
            print("¡PROYECTO DE INGESTA COMPLETADO!")
            break

def ejecutar_capa_oro():
    print("=== INICIANDO CAPA ORO: DATASET MAESTRO ===")
    preparar_entorno()
    descargar_plata()
    crear_capa_oro()
    subir_oro_s3()

if __name__ == "__main__":
    ejecutar_capa_oro()
```

    === INICIANDO CAPA ORO: DATASET MAESTRO ===
    -> Descargando las 3 fuentes limpias desde la Capa Plata en S3...
       [OK] demanda.parquet descargado.
       [OK] clima.parquet descargado.
       [OK] festivos.parquet descargado.
    -> Levantando Spark para el cruce final...
    -> Realizando LEFT JOIN y Feature Engineering...
    -> Guardando Dataset Predictivo en 'oro/dataset_predictivo'...
    ¡Capa Oro finalizada! Tablón maestro generado con 4880 días.
    root
     |-- fecha: date (nullable = true)
     |-- demanda: float (nullable = true)
     |-- temp_max_media: double (nullable = true)
     |-- temp_min_media: double (nullable = true)
     |-- temp_media_nacional: double (nullable = true)
     |-- precipitacion_media: double (nullable = true)
     |-- festividad: string (nullable = true)
     |-- es_festivo: integer (nullable = false)
     |-- dia_semana: integer (nullable = true)
     |-- mes: integer (nullable = true)
     |-- anio: integer (nullable = true)
     |-- trimestre: integer (nullable = true)
     |-- es_fin_de_semana: integer (nullable = false)
     |-- rango_termico: double (nullable = true)
     |-- demanda_lag_7: float (nullable = true)
    
    +----------+---------+--------------+--------------+-------------------+-------------------+----------------+----------+----------+---+----+---------+----------------+-------------+-------------+
    |fecha     |demanda  |temp_max_media|temp_min_media|temp_media_nacional|precipitacion_media|festividad      |es_festivo|dia_semana|mes|anio|trimestre|es_fin_de_semana|rango_termico|demanda_lag_7|
    +----------+---------+--------------+--------------+-------------------+-------------------+----------------+----------+----------+---+----+---------+----------------+-------------+-------------+
    |2013-01-08|811354.8 |NULL          |NULL          |NULL               |NULL               |Laborable/Normal|0         |3         |1  |2013|1        |0               |NULL         |581423.44    |
    |2013-01-09|826810.06|NULL          |NULL          |NULL               |NULL               |Laborable/Normal|0         |4         |1  |2013|1        |0               |NULL         |730434.56    |
    |2013-01-10|815089.56|NULL          |NULL          |NULL               |NULL               |Laborable/Normal|0         |5         |1  |2013|1        |0               |NULL         |764201.4     |
    |2013-01-11|802819.2 |NULL          |NULL          |NULL               |NULL               |Laborable/Normal|0         |6         |1  |2013|1        |0               |NULL         |760902.0     |
    |2013-01-12|719353.75|NULL          |NULL          |NULL               |NULL               |Laborable/Normal|0         |7         |1  |2013|1        |1               |NULL         |684806.2     |
    +----------+---------+--------------+--------------+-------------------+-------------------+----------------+----------+----------+---+----+---------+----------------+-------------+-------------+
    only showing top 5 rows
    
    === SUBIENDO DATASET PREDICTIVO A S3 ===
    -> Subiendo a S3 como 'oro/dataset_predictivo.parquet'...
    ¡PROYECTO DE INGESTA COMPLETADO!



```python

```
