"""
Este script se empleará para almacenar las rutas de los ficheros de datos, así como los nombres de las distintas bases de datos,
tanto de SQL como de MongoDB, las distintas colecciones y las credenciales necesarias para poder hacer las conexiones correctamente.
También se decidirán los ficheros que se van a cargar en las bases de datos empleando load_data.py o inserta_dataset.py

Autores: Jorge Carnicero Príncipe y Andrés Gil Vicente
Grupo: 2º A IMAT
Proyecto: Proyecto Final 2024/2025 - Bases de Datos

"""
  
############################################################################################################################################

# CONFIGURACIÓN DE CONEXIÓN A MYSQL --> CAMBIAR SI ES NECESARIO O SE EMPLEA OTRA RUTA
USER_SQL = "Jorge"                
PASSWORD_SQL = "carnicero1"          
NOMBRE_BASE_DATOS_SQL = "Reviews_Pr_Final" 
NOMBRES_TABLAS_SQL = ["personas", "productos", "tipos_producto", "review"]

############################################################################################################################################

# CONFIGURACIÓN DE CONEXIÓN A MONGODB --> CAMBIAR SI ES NECESARIO O SE EMPLEA OTRA RUTA
CONNECTION_STRING = "mongodb://localhost:27017"     
NOMBRE_BASE_DATOS_MONGO_DB = NOMBRE_BASE_DATOS_SQL
COLECCION_MONGODB = "reviews"

############################################################################################################################################

# RUTAS DE LOS FICHEROS DE DATOS EMPLEADOS --> CAMBIAR SI ES NECESARIO O SE EMPLEA OTRA RUTA
NOMBRE_CARPETA = "data"
RUTA_DIGITAL_MUSIC = f"{NOMBRE_CARPETA}/Digital_Music_5.json"
RUTA_MUSICAL_INSTRUMENTS = f"{NOMBRE_CARPETA}/Musical_Instruments_5.json"
RUTA_TOYS_GAMES = f"{NOMBRE_CARPETA}/Toys_and_Games_5.json"
RUTA_VIDEO_GAMES = f"{NOMBRE_CARPETA}/Video_Games_5.json"
RUTA_AMAZON_INSTANT_VIDEO = f"{NOMBRE_CARPETA}/Amazon_Instant_Video_5.json"
RUTA_CELL_PHONES = f"{NOMBRE_CARPETA}/Cell_Phones_and_Accessories_5.json"
RUTA_CLOTHING_SHOES = f"{NOMBRE_CARPETA}/Clothing_Shoes_and_Jewelry_5.json"
RUTA_GROCERY = f"{NOMBRE_CARPETA}/Grocery_and_Gourmet_Food_5.json"
RUTA_OFFICE_PRODUCTS = f"{NOMBRE_CARPETA}/Office_Products_5.json"
RUTA_PET_SUPPLIES = f"{NOMBRE_CARPETA}/Pet_Supplies_5.json"
RUTA_SPORTS_OUTDOORS = f"{NOMBRE_CARPETA}/Sports_and_Outdoors_5.json"

############################################################################################################################################

# FICHEROS UTILIZADOS EN EL load_data.py
FICHEROS_DATOS_LOAD_DATA = [

    RUTA_DIGITAL_MUSIC, 
    RUTA_MUSICAL_INSTRUMENTS, 
    RUTA_TOYS_GAMES,
    RUTA_VIDEO_GAMES
]

# FICHEROS UTILIZADOS EN EL inserta_dataset.py
FICHEROS_DATOS_INSERTA_DATASET = [
    
    RUTA_AMAZON_INSTANT_VIDEO, 
    RUTA_CELL_PHONES, 
    RUTA_CLOTHING_SHOES, 
    RUTA_GROCERY, 
    RUTA_OFFICE_PRODUCTS, 
    RUTA_PET_SUPPLIES, 
    RUTA_SPORTS_OUTDOORS
]

############################################################################################################################################

# CONFIGURACIÓN DE CONEXIÓN A NEO4J --> CAMBIAR SI ES NECESARIO O SE EMPLEA OTRA RUTA
URI_NEO4J = "bolt://localhost:7687"
USER_NEO4J = "neo4j"
PASSWORD_NEO4J = "carnicero1"

############################################################################################################################################

# Tamaño de los lotes para la inserción de los ficheros de datos en las bases de datos
BATCH_SIZE = 5000

############################################################################################################################################







