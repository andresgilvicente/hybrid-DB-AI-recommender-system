"""
Este script se empleará para cargar toda la infraestructura inicial de datos a partir de los distintos ficheros proporcionados, 
creando así distintas bases de datos, tanto de MongoDB como de SQL, con sus respectivas tablas y colecciones. Las variables con 
los nombres de las rutas de los ficheros, nombres de las bases, credenciales para hacer las conexiones y demás detalles, se importan 
desde el fichero de configuracion.py, donde pueden ser fácilmente modificadas. 

Además, cabe destacar que el procesado de los ficheros se hace línea a línea y que en caso de que un dato ya se haya procesado 
anteriormente, es decir, que ya existe en la base de datos, entonces se actualizará la información referente a ese dato con la nueva 
información encontrada. Esto se hará siempre que la nueva información no sea un valor nulo. De esta forma gestionamos correctamente la 
aparición de valores repetidos, sin que haya duplicados incoherentes y permitiendo actualización de los datos, además de escalabilidad.

En caso de cualquier error, se recogerán las excepciones para que el programa no termine de forma abrupta en ningún caso.

Autores: Jorge Carnicero Príncipe y Andrés Gil Vicente
Grupo: 2º A IMAT
Proyecto: Proyecto Final 2024/2025 - Bases de Datos

"""

############################################################################################################################################

# Importamos las librerías necesarias
from configuracion import*
import json
from pymongo import MongoClient
from pymongo.database import Database
from pymysql.connections import Connection
import pymysql
from datetime import datetime
import re
from pymysql.cursors import Cursor
from typing import List, Tuple

############################################################################################################################################

# Variables globales empleadas para controlar los identificadores únicos a la hora de hacer las inserciones
dicc_ids_personas = {}
dicc_ids_productos = {} # clave (asin, tipo_producto) valor id_producto
dicc_ids_tipos_producto = {}
contador_persona = 0
contador_producto = 0
contador_tipo_producto = 0
id_review = 0

############################################################################################################################################

# EXTRACCIÓN DEL TIPO DE PRODUCTO A PARTIR DEL NOMBRE DEL FICHERO DE DATOS
def extraer_tipo_producto(nombre_fichero:str)-> str:
    """

    Función que recibe una ruta de un fichero y extrae el tipo de producto al que corresponden los datos
    de dicho archivo. Se emplea para ello regex.

    Args:
        nombre_fichero (str): ruta del archivo de datos, incluyendo posibles carpetas externas y demás.

    Returns:
        tipo_producto (str): cadena de caracteres extraída mediante regex, que representa el tipo de producto
        al que corresponden los datos del fichero.
    
    """
    # Definimos nuestro patrón de regex
    patron_products = r".*\/(.*)_.*"

    # Extraemos del nombre del fichero, el tipo de producto
    tipo_producto = re.match(pattern=patron_products, string=nombre_fichero).group(1)  

    return tipo_producto

# FORMATEO DE LA FECHA PARA SER DE TIPO DATE (YYYY-MM-DD)
def formatear_fecha(fecha_antigua:str)-> str:
    """

    Esta función formatea las fechas que vienen con una estructura no compatible con los tipos de datos de fehca de SQL,
    para convertirlas a un formato que ya puede ser utilizado como tipo de dato date (YYYY-MM-DD)

    Ejemplo: 
                "07 9, 2012" se convertirá en "2012-07-09"
    
    Args:
        fecha_antigua (str): fecha con un formato que no nos sirve y queremos modificar

    Returns:
        fecha_formateada (str): fecha que ya tiene un formato de tipo date (YYYY-MM-DD)
    
    
    """

    if fecha_antigua:
        # Convertimos al formato YYYY-MM-DD
        fecha_formateada = datetime.strptime(fecha_antigua, "%m %d, %Y").strftime("%Y-%m-%d")  

        # Devolvemos la fecha ya bien formateada, para poder ser un tipo de dato date en SQL
        return fecha_formateada
    
    else:
        return None

############################################################################################################################################

# CREACIÓN DE LA CONEXIÓN CON pymysql
def conectar_mysql()-> Connection:
    """

    Esta función intenta crear una conexión con una base de datos MySQL usando los datos
    proporcionados (usuario y contraseña) y el host "localhost". En caso de que falle la conexión,
    se captura la excepción y se imprime un mensaje de error.

    Args:
        None

    Returns:
        - Devuelve un objeto de tipo pymysql.connections.Connection si la conexión es exitosa.
        - Devuelve None si ocurre un error durante la conexión.    
    
    """
    try:
        conexion = pymysql.connect(
        host="localhost",
        user=USER_SQL,              # el usuario
        password=PASSWORD_SQL)      # tu constraseña
        
        return conexion 
    
    except Exception as error:
        print(f"Error al conectar a MySQL: {error}")  # Prevenimos excepciones, así que en caso de error mostramos dicho error
        return None

# CREACIÓN DE BASE DE DATOS SQL
def create_database_sql(conexion:Connection)-> None:
    """

    La función recibe una conexión activa a MySQL y, utilizando un cursor, ejecuta las sentencias SQL necesarias para:
        1. Crear una base de datos con el nombre especificado en la variable global nombre_base_datos.
        2. Seleccionar esa base de datos como activa con USE nombre_base_datos.

    Si ocurre un error durante la creación de la base de datos o al seleccionarla, se captura la excepción y se imprime el mensaje
    de error correspondiente.

    Args:
        conexion (pymysql.connections.Connection): Objeto de conexión activo a MySQL, obtenido previamente con "pymysql.connect()"

    Returns:
        None: Esta función no devuelve ningún valor. Solo se encarga de la creación y selección de la base de datos en el servidor.
    
    """
    try:
        cursor = conexion.cursor()

        # Eliminamos la base de datos en caso de que ya existiera
        cursor.execute(f"DROP DATABASE IF EXISTS {str(NOMBRE_BASE_DATOS_SQL)};")

        # Creamos la base de datos
        sql = "CREATE DATABASE " + str(NOMBRE_BASE_DATOS_SQL)
        cursor.execute(sql)

        sql = "USE " + str(NOMBRE_BASE_DATOS_SQL)
        cursor.execute(sql)

        # Informamos de que la crecación de la base de datos ha ido bien.
        print(f"\nBase de datos SQL: \"{NOMBRE_BASE_DATOS_SQL}\" creada con éxito.")

        # Cerramos el cursor
        cursor.close()

    except Exception as error:
        print(f"\nError al crear la base de datos SQL: {error}")

# CREACIÓN DE TABLAS SQL
def create_tables_sql(conexion:Connection)-> None:
    """

    Esta función crea las tabla de SQL en la base de datos estipulada, mediante la conexión que se le pasa como argumento.

    Args:
        conexion (pymysql.connections.Connection): Objeto de conexión activo a MySQL, obtenido previamente con "pymysql.connect()".

    Returns:
        None: La función no devuelve ningún valor. Si se ejecuta con éxito, crea las tablas en la base de datos. En caso de error, 
        imprime el mensaje de error correspondiente.
    
    """
    try:
        cursor = conexion.cursor()

        ################## TABLA PERSONAS ##################

        query_tabla_personas = """

            CREATE TABLE Personas (

                id_persona INT NOT NULL PRIMARY KEY,
                reviewerID VARCHAR(250) NOT NULL,
                reviewerName VARCHAR(250)

            );"""
        
        # Tabla PERSONAS
        cursor.execute(query_tabla_personas)

        print("\nTabla de SQL: \"Personas\" creada con éxito.")

        ################## TABLA TIPOS DE PRODUCTO ##################

        query_tabla_tipos_producto = """

            CREATE TABLE Tipos_producto (

                tipo_producto INT NOT NULL PRIMARY KEY,
                nombre_tipo_producto VARCHAR(250) NOT NULL

            );"""
        
        # Tabla Tipos de Producto 
        cursor.execute(query_tabla_tipos_producto)

        print("\nTabla de SQL: \"Tipos de producto\" creada con éxito.")   

        ################## TABLA PRODUCTOS ##################

        query_tabla_productos = """

            CREATE TABLE Productos (

                id_producto INT NOT NULL PRIMARY KEY,
                asin VARCHAR(20) NOT NULL,
                tipo_producto INT NOT NULL,
                FOREIGN KEY (tipo_producto) REFERENCES Tipos_producto (tipo_producto) ON DELETE CASCADE

            );"""
        
        # Tabla Productos
        cursor.execute(query_tabla_productos)

        print("\nTabla de SQL: \"Productos\" creada con éxito.")

        ################## TABLA REVIEW ##################

        query_tabla_review = """

            CREATE TABLE Review (

                id_review INT NOT NULL PRIMARY KEY,
                id_persona INT NOT NULL,
                id_producto INT NOT NULL,
                overall INT NOT NULL,
                unixReviewTime BIGINT,
                reviewTime date,
                FOREIGN KEY (id_persona) REFERENCES Personas (id_persona) ON DELETE CASCADE,
                FOREIGN KEY (id_producto) REFERENCES Productos (id_producto) ON DELETE CASCADE

            );"""
        
        # Tabla REVIEW 
        cursor.execute(query_tabla_review)

        print("\nTabla de SQL: \"Review\" creada con éxito.")        
        
        # Cerramos el cursor
        cursor.close()

    except Exception as error:
        print(f"\nError al crear la tabla SQL: {error}")

# INSERCIÓN DATOS POR LOTES A LA BASE DE DATOS DE MYSQL 
def insertar_lote_sql(cursor: Cursor, query: str, valores: List[Tuple])-> None:
    """

    Inserta datos en SQL en lotes. Emplea executemany.

    Args:
        cursor: Objeto creado a través de la conexión de MySQL para ejecutar consultas o hacer inserciones.
        query (str): Es la consulta SQL que queremos ejecutar (es de inserción en este caso)
        valores (list): lista de tuplas a insertar mediante la query SQL


    Returns: 
        None. Solo se hace la ejecución de la inserción pero no se devuelve nada.

    """

    if valores:
        cursor.executemany(query, valores)

############################################################################################################################################

# CREACIÓN DE LA CONEXIÓN CON MONGODB Y SU DATABASE
def get_database_mongo(database:str)-> Database:

    """

    Funcion para obtener la conexión a la base de datos de MongoDB. 
    En caso de que ya exista, la borramos y la volvemos a crear.

    Args:
        database (str): el nombre de la base de datos
    Returns:
        client[database]: la base de datos a la que nos estamos conectando, tras crearla

    """
    
    # Creamos la conexion empleando mongoClient
    client = MongoClient(CONNECTION_STRING)

    # Extraemos todos los nombres de las bases de datos que ya existen en la conexión
    nombres_databases = client.list_database_names()

    # En caso de que ya exista la base de datos que vamos a crear, la borramos y luego la creamos otra vez
    if database in nombres_databases:
        client.drop_database(database)

    # Devolvemos la conexion de la base de datos que queremos
    print(f"\nBase de datos MongoDB: \"{database}\" creada con éxito.")
    return client[database]

############################################################################################################################################

# INSERCIÓN DE LOS DATOS DE UN FICHERO A LAS DISTINTAS BASES DE DATOS
def insertar_datos_global(file_in:str, sql_conexion:Connection, mongodb_database:Database, batch_size:int)-> None:
    """

    Función que se encarga de la inserción de datos procedentes de un único fichero, pero que se distribuyen en distintas bases de datos,
    de SQL y de MongoDB, y también en distintas tablas y colecciones. La lectura del fichero se hace línea a línea, donde para crear un 
    objeto de tipo JSON usamos json.loads(linea).

    Args:
        file_in (str): ruta del fichero de entrada de datos.
        mongodb_database: Objeto de MongoClient.
        sql_conexion (pymysql.connections.Connection): Objeto de conexión activo a MySQL, obtenido previamente con "pymysql.connect()".
        batch_size (int): tamaño de un lote de datos, controla cada cuanto tenemos que hacer inserciones.

    Returns:
        None. No devuelve nada, solo hace las inserciones correspondientes en las bases de datos indicadas.
    
    """
    
    # Variables globales empleadas para controlar los identificadores únicos
    global dicc_ids_personas, dicc_ids_productos, dicc_ids_tipos_producto, contador_persona, contador_producto, contador_tipo_producto, id_review

    # Inicializamos las listas vacías donde vamos a ir cargando los lotes de datos
    valores_insertar_review = []
    valores_insertar_personas = []
    valores_insertar_productos = []
    valores_insertar_tipos_producto = []
    documentos_insertar_mongo = []

    # Inicializamos el cursor
    cursor = sql_conexion.cursor()

    mongo_db_collection = mongodb_database[COLECCION_MONGODB]
    
    # Abrimos el fichero de datos en modo lectura
    with open(file_in, "r") as file:
                    
        # Extraemos el nombre del tipo de producto a partir del nombre del fichero de datos
        nombre_tipo_producto = extraer_tipo_producto(nombre_fichero=file_in)

        # Iteramos por cada línea del fichero
        for linea in file:

            # Extraemos el diccionario donde tenemos los datos de una review
            data = json.loads(linea)
            
            # Accedemos a cada campo del diccionario, ponemos None en caso de que ese campo no se encuentre en el diccionario
            reviewerID = data.get("reviewerID", None)
            asin = data.get("asin", None)
            reviewerName = data.get("reviewerName", None)
            helpful = data.get("helpful", [None, None])
            reviewText = data.get("reviewText", None)
            overall = data.get("overall", None)
            summary = data.get("summary", None)
            unixReviewTime = data.get("unixReviewTime", None)
            reviewTime = formatear_fecha(data.get("reviewTime", None))  # Formateamos la fecha al formato date (YYYY-MM-DD)

            # Asignación de IDs únicos con contadores y diccionarios
            if reviewerID not in dicc_ids_personas:
                dicc_ids_personas[reviewerID] = contador_persona  # si el valor no está en el diccionario lo guardamos
                id_persona = contador_persona  # cogemos el id a partir del contador
                contador_persona += 1  # actualizamos el contador
            else:
                # si el valor ya está en el diccionario, sacamos el id de ahí
                id_persona = dicc_ids_personas[reviewerID]  
            
            # Tenemos en cuenta que puede haber el mismo asin para distintos tipos de productos
            if (asin, nombre_tipo_producto) not in dicc_ids_productos:
                dicc_ids_productos[(asin, nombre_tipo_producto )] = contador_producto  # si el valor no está en el diccionario lo guardamos
                id_producto = contador_producto  # cogemos el id a partir del contador
                contador_producto += 1  # actualizamos el contador
            else:
                # si el valor ya está en el diccionario, sacamos el id de ahí
                id_producto = dicc_ids_productos[(asin, nombre_tipo_producto)]
            
            if nombre_tipo_producto not in dicc_ids_tipos_producto:
                dicc_ids_tipos_producto[nombre_tipo_producto] = contador_tipo_producto  # si el valor no está en el diccionario lo guardamos
                id_tipo_producto = contador_tipo_producto  # cogemos el id a partir del contador
                contador_tipo_producto += 1 # actualizamos el contador
            else:
                # si el valor ya está en el diccionario, sacamos el id de ahí
                id_tipo_producto = dicc_ids_tipos_producto[nombre_tipo_producto] 
            
            # Añadimos a las listas de tuplas, las tuplas creadas para que sean posteriormente insertadas (SQL)
            valores_insertar_personas.append((id_persona, reviewerID, reviewerName))
            valores_insertar_productos.append((id_producto, asin, id_tipo_producto))
            valores_insertar_tipos_producto.append((id_tipo_producto, nombre_tipo_producto))
            valores_insertar_review.append((id_review, id_persona, id_producto, overall, unixReviewTime, reviewTime))
            
            # Añadimos a la lista de documentos, el diccionario con los campos correspondientes (MongoDB)
            documento = {

                "_id": id_review,
                "helpful": helpful,
                "reviewText": reviewText,
                "summary": summary
            }

            # Pero solo añadimos los campos no nulos del diccionario
            documentos_insertar_mongo.append({k: v for k, v in documento.items() if v is not None})

            # Cada vez que leemos una línea es una nueva review así que siempre actualizamos el contador de ids de reviews
            id_review += 1
            
            # Inserción por lotes, solo si las listas ya tienen el tamaño deseado 
            # Añadimos el ON DUPLICATE KEY UPDATE, para que si ya existe la PRIMARY KEY, se actualicen el resto de campos de esa entrada de la tabla
            if len(valores_insertar_review) >= batch_size:
                insertar_lote_sql(
                    cursor=cursor, 
                    query="""
                                INSERT INTO Personas (id_persona, reviewerID, reviewerName)
                                VALUES (%s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    reviewerID = IF(VALUES(reviewerID) IS NOT NULL, VALUES(reviewerID), reviewerID),
                                    reviewerName = IF(VALUES(reviewerName) IS NOT NULL, VALUES(reviewerName), reviewerName);

                        """, 
                    valores=valores_insertar_personas)
                
                insertar_lote_sql(
                    cursor=cursor, 
                    query="""
                                INSERT INTO Tipos_producto (tipo_producto, nombre_tipo_producto)
                                VALUES (%s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    nombre_tipo_producto = IF(VALUES(nombre_tipo_producto) IS NOT NULL, VALUES(nombre_tipo_producto), nombre_tipo_producto);

                        """, 
                    valores=valores_insertar_tipos_producto)
                
                insertar_lote_sql(
                    cursor=cursor, 
                    query="""
                                INSERT INTO Productos (id_producto, asin, tipo_producto)
                                VALUES (%s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    asin = IF(VALUES(asin) IS NOT NULL, VALUES(asin), asin), 
                                    tipo_producto = IF(VALUES(tipo_producto) IS NOT NULL, VALUES(tipo_producto), tipo_producto);
                        """, 
                    valores=valores_insertar_productos)
                
                insertar_lote_sql(
                    cursor=cursor, 
                    query="""
                                INSERT INTO Review (id_review, id_persona, id_producto, overall, unixReviewTime, reviewTime)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    overall = IF(VALUES(overall) IS NOT NULL, VALUES(overall), overall), 
                                    unixReviewTime = IF(VALUES(unixReviewTime) IS NOT NULL, VALUES(unixReviewTime), unixReviewTime), 
                                    reviewTime = IF(VALUES(reviewTime) IS NOT NULL, VALUES(reviewTime), reviewTime);
                        """, 
                        valores=valores_insertar_review)
                
                mongo_db_collection.insert_many(documentos_insertar_mongo)
                
                # Limpiar listas después de la inserción
                valores_insertar_review.clear()
                valores_insertar_personas.clear()
                valores_insertar_productos.clear()
                valores_insertar_tipos_producto.clear()
                documentos_insertar_mongo.clear()
                
    # Inserción final si quedan datos en las listas y no se ha completado un lote
    if valores_insertar_review:

        insertar_lote_sql(
                    cursor=cursor, 
                    query="""
                                INSERT INTO Personas (id_persona, reviewerID, reviewerName)
                                VALUES (%s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    reviewerID = IF(VALUES(reviewerID) IS NOT NULL, VALUES(reviewerID), reviewerID),
                                    reviewerName = IF(VALUES(reviewerName) IS NOT NULL, VALUES(reviewerName), reviewerName);
                        """, 
                    valores=valores_insertar_personas)
                
        insertar_lote_sql(
            cursor=cursor, 
            query="""
                        INSERT INTO Tipos_producto (tipo_producto, nombre_tipo_producto)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE 
                            nombre_tipo_producto = IF(VALUES(nombre_tipo_producto) IS NOT NULL, VALUES(nombre_tipo_producto), nombre_tipo_producto);

                """, 
                valores=valores_insertar_tipos_producto)
        
        insertar_lote_sql(
            cursor=cursor, 
            query="""
                        INSERT INTO Productos (id_producto, asin, tipo_producto)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            asin = VALUES(asin), 
                            tipo_producto = VALUES(tipo_producto);
                """, 
            valores=valores_insertar_productos)
        
        insertar_lote_sql(
            cursor=cursor, 
            query="""
                        INSERT INTO Review (id_review, id_persona, id_producto, overall, unixReviewTime, reviewTime)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            overall = IF(VALUES(overall) IS NOT NULL, VALUES(overall), overall), 
                            unixReviewTime = IF(VALUES(unixReviewTime) IS NOT NULL, VALUES(unixReviewTime), unixReviewTime), 
                            reviewTime = IF(VALUES(reviewTime) IS NOT NULL, VALUES(reviewTime), reviewTime);
                """, 
                valores=valores_insertar_review)
        
        mongo_db_collection.insert_many(documentos_insertar_mongo)
    
    # Guardamos los cambios tras las inserciones
    sql_conexion.commit()

    # Cerramos el cursor
    cursor.close()

# EJECUCIÓN DEL PROCESO COMPLETO
def main()-> None:
    """

    Función principal del script, que se encarga de llamar al resto de funciones para desarrollar el flujo completo del programa de 
    cargado e inserción de datos en las bases de datos correspondientes. Va mostrando avisos por la terminal, según se van completando
    los distintos procesos.

    Args:
        None
    
    Returns:
        None
    
    """

    # Nos conectamos a la base de datos de MongoDB
    dbname = get_database_mongo(database=NOMBRE_BASE_DATOS_MONGO_DB)

    # Nos conectamos a MySQL
    conexion = conectar_mysql()

    # En caso de que nos hayamos podido conectar a MySQL
    if conexion:

        # Creamos la Base de Datos SQL
        create_database_sql(conexion)

        # Creamos las tablas SQL (inicialmente vacías)
        create_tables_sql(conexion)

        # Iteramos sobre todos los ficheros
        for fichero in FICHEROS_DATOS_LOAD_DATA:
            
            # Insertamoslos datos en el fichero correspondiente
            insertar_datos_global(file_in=fichero, sql_conexion=conexion, mongodb_database=dbname, batch_size=BATCH_SIZE)

            # Avisamos al usuario de que todo ha ido bien
            print(f"\nYa hemos cargado los datos del fichero: \"{fichero}\" en la base de datos SQL: \"{NOMBRE_BASE_DATOS_SQL}\".")

            # Avisamos al usuario de que todo ha ido bien
            print(f"\nYa hemos cargado los datos del fichero: \"{fichero}\" en la colección \"{COLECCION_MONGODB}\" de la base de datos MongoDb : \"{NOMBRE_BASE_DATOS_MONGO_DB}\".")
              
    # Cerramos la conexión MySQl
    conexion.close()

############################################################################################################################################

if __name__=="__main__":

    try:
        # Llamamos a la función principal del archivo para que desarrolle el proceso completo
        main()

    except Exception as e:
        # Controlamos posibles excepciones
        print(f"\n>>>>>>>>>>>>>>>> ERROR: {e}")

