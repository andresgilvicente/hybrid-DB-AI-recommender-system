"""

Este script se empleará para insertar en las bases de datos, uno o más ficheros nuevos, distintos de los que se insertaron 
en el script de load_data.py . El programa está diseñado para que se puedan insertar tantos ficheros nuevos como se 
desee, si se desea se puede configurar desde las variables globales de configuracion.py . 

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
import pymysql
from load_data import extraer_tipo_producto, formatear_fecha, insertar_lote_sql
from menu_visualizacion import get_database_mongo, ejecutar_consulta_sql
from pymysql.connections import Connection
from pymongo.database import Database

############################################################################################################################################

# EXTRACCIÓN NÚMERO IDENTIFICADOR DEL TIPO DE PRODUCTO DADO EL NOMBRE DEL TIPO DE PRODUCTO
def extraer_numero_tipo_producto(conexion:Connection, nombre_tipo_producto:str)-> int:
    """
    Extrae el identificador numérico asociado a un tipo de producto, dado su nombre. Esta función consulta la base de datos 
    para obtener el valor del campo "tipo_producto" correspondiente al nombre del tipo de producto proporcionado. Es útil cuando 
    queremos trabajar con claves numéricas internas a partir de nombres descriptivos.

    Args:
        conexion: Objeto de conexión a la base de datos, necesario para ejecutar la consulta SQL.
        nombre_tipo_producto (str): Nombre del tipo de producto cuyo identificador numérico se desea obtener.

    Returns:
        int: El identificador numérico (clave primaria) del tipo de producto correspondiente.
    
    """
    # Buscamos cúal es el identificador numérico del tipo de producto que corresponde con este nombre de tipo de producto
    query_idx = f"""

        SELECT tipo_producto
        FROM tipos_producto
        WHERE nombre_tipo_producto = %s

    """  

    # Como los identificadores son únicos, con ordenar por ese campo descendientemente y coger el primero nos sirve
    id_numerico = int(ejecutar_consulta_sql(conexion=conexion, sql=query_idx, args=[nombre_tipo_producto])[0][0])

    return id_numerico

# EXTRACCIÓN NUEVO ID (DE PRODUCTO, TIPO PRODUCTO O REVIEW)
def crear_nuevo_id_numerico(conexion:Connection, tabla:str, nombre_columna:str)->int:
    """
    Genera un nuevo identificador numérico para una tabla de base de datos, incrementando el valor más alto existente. Esta función se 
    conecta a una base de datos a través de la conexión proporcionada, consulta el valor más alto del identificador especificado en una
    tabla y devuelve un nuevo ID incrementado en una unidad.

    Args:
        conexion: Objeto de conexión a la base de datos. Se utiliza para ejecutar la consulta SQL.
        tabla (str): Nombre de la tabla en la base de datos de la cual se extraerá el identificador más alto.
        nombre_columna (str): Nombre de la columna que contiene los identificadores numéricos.

    Returns:
        int: Un nuevo identificador numérico, que es el valor más alto actual más uno.
    
    """
    # Buscamos cúal es el identificador numérico de persona más alto (el último), para continuar con esa numeración
    query_idx = f"""

            SELECT {nombre_columna}
            FROM {tabla}
            ORDER BY {nombre_columna} DESC
            LIMIT 1

    """  

    # Como los identificadores son únicos, con ordenar por ese campo descendientemente y coger el primero nos sirve
    ultimo_id = int(ejecutar_consulta_sql(conexion=conexion, sql=query_idx)[0][0])

    # El nuevo identificador saldrá de sumar 1 al último identificador que existía en la base de datos
    nuevo_id = ultimo_id + 1

    return nuevo_id

############################################################################################################################################

# INSERCIÓN DEL NUEVO FICHERO
def insertar_dataset(file_in:str, sql_conexion:Connection, mongodb_database:Database, batch_size:int)-> None:
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
    # Inicializamos el cursor
    cursor = sql_conexion.cursor()

    # Seleccionamos la colección de MongoDb
    mongo_db_collection = mongodb_database[COLECCION_MONGODB]

    # Inicializamos las listas vacías donde vamos a ir cargando los lotes de datos
    valores_insertar_review = []
    valores_insertar_personas = []
    valores_insertar_productos = []
    documentos_insertar_mongo = []

    # Inicializamos diccionarios para almacenar tuplas de datos ya existentes en la base de datos
    personas_cargadas = cargar_datos_usuarios(conexion=sql_conexion)  # estructura -> reviewerID: (id_persona, reviewerName)
    productos_cargados = cargar_datos_productos(conexion=sql_conexion)  # estructura -> asin: (id_producto, tipo_producto)

    # Extraemos el nombre del tipo de producto a partir del nombre del fichero de datos
    nombre_tipo_producto = extraer_tipo_producto(nombre_fichero=file_in) 

    # Extraemos el nuevo id de tipo de producto
    nuevo_id_tipo_producto = crear_nuevo_id_numerico(conexion=sql_conexion, tabla=NOMBRES_TABLAS_SQL[2], nombre_columna="tipo_producto")

    # Esta inserción es única ya que todos las reviews del fichero son del mismo tipo de producto (viene dado por el nombre del fichero)
    valores_insertar_tipos_producto = [(nuevo_id_tipo_producto, nombre_tipo_producto)]

    insertar_lote_sql(
                cursor=cursor, 
                query="""
                            INSERT INTO Tipos_producto (tipo_producto, nombre_tipo_producto)
                            VALUES (%s, %s)
                            ON DUPLICATE KEY UPDATE 
                                nombre_tipo_producto = IF(VALUES(nombre_tipo_producto) IS NOT NULL, VALUES(nombre_tipo_producto), nombre_tipo_producto);

                    """, 
                    valores=valores_insertar_tipos_producto)

    # Extraemos el nuevo identificador para las reviews, luego lo iremos actualizando de 1 en 1 por cada fila
    nuevo_id_review = crear_nuevo_id_numerico(conexion=sql_conexion, tabla=NOMBRES_TABLAS_SQL[3], nombre_columna="id_review")

    # Extraemos el nuevo identificador numerico para las personas, luego lo iremos actualizando cuando corresponda
    nuevo_id_usuario = crear_nuevo_id_numerico(conexion=sql_conexion, tabla=NOMBRES_TABLAS_SQL[0], nombre_columna="id_persona")

    # Extraemos el nuevo identificador numerico para los productos, luego lo iremos actualizando cuando corresponda
    nuevo_id_producto = crear_nuevo_id_numerico(conexion=sql_conexion, tabla=NOMBRES_TABLAS_SQL[1], nombre_columna="id_producto")

    # Abrimos el fichero de datos en modo lectura
    with open(file_in, "r") as file:
                    
        # Iteramos por cada línea del fichero
        for linea in file:

            # Extraemos el diccionario donde tenemos los datos de una review
            data = json.loads(linea)

            # Accedemos a cada campo del diccionario de data, ponemos None en caso de que ese campo no se encuentre en el diccionario
            reviewerID = data.get("reviewerID", None)
            asin = data.get("asin", None)
            reviewerName = data.get("reviewerName", None)
            helpful = data.get("helpful", [None, None])
            reviewText = data.get("reviewText", None)
            overall = data.get("overall", None)
            summary = data.get("summary", None)
            unixReviewTime = data.get("unixReviewTime", None)
            reviewTime = formatear_fecha(data.get("reviewTime", None))  # Formateamos la fecha al formato date (YYYY-MM-DD)

            # Solo hacemos inserciones si la persona no existe ya en la BBDD
            if reviewerID not in personas_cargadas:

                # Lo añadimos a la lista de inserción
                valores_insertar_personas.append((nuevo_id_usuario, reviewerID, reviewerName))  

                # Lo añadimos al diccionario
                personas_cargadas[reviewerID] = (nuevo_id_usuario, reviewerName)

                # ID que vamos a usar para insertar en las reviews
                id_persona_insertar_en_review = nuevo_id_usuario

                # Actualizamos el identificador, para el siguiente producto nuevo
                nuevo_id_usuario += 1

            else:
                # ID que vamos a usar para insertar en las reviews
                id_persona_insertar_en_review = personas_cargadas[reviewerID][0]  # accedemos al dicc: reviewerID: (id_persona, reviewerName)

            # Solo hacemos inserciones si el producto no existe ya en la BBDD
            if asin not in productos_cargados:

                # Lo añadimos a la lista de inserción
                valores_insertar_productos.append((nuevo_id_producto, asin, nuevo_id_tipo_producto))  

                # Lo añadimos al diccionario
                productos_cargados[asin] = (nuevo_id_producto, nuevo_id_tipo_producto)

                # ID que vamos a usar para insertar en las reviews
                id_producto_insertar_en_review = nuevo_id_producto

                # Actualizamos el identificador, para la siguiente persona nueva
                nuevo_id_producto += 1

            else:
                # ID que vamos a usar para insertar en las reviews
                id_producto_insertar_en_review = productos_cargados[asin][0]  # accedemos al dicc: asin: (id_producto, tipo_producto)

            # En la tabla de reviews siempre insertamos, pase lo que pase
            valores_insertar_review.append((nuevo_id_review, id_persona_insertar_en_review, id_producto_insertar_en_review, overall, unixReviewTime, reviewTime))

            # Añadimos a la lista de documentos, el diccionario con los campos correspondientes (MongoDB)
            documento = {

                "_id": nuevo_id_review,
                "helpful": helpful,
                "reviewText": reviewText,
                "summary": summary
            }

            # Pero solo vamos a querer insertar los campos no nulos del diccionario
            documentos_insertar_mongo.append({k: v for k, v in documento.items() if v is not None})

            # Para la review, actualizamos el contador (identificador). Para cada fila nueva se suma 1
            nuevo_id_review += 1

            # Inserción por lotes, solo si las listas ya tienen el tamaño deseado 
            # Añadimos el ON DUPLICATE KEY UPDATE, para que si ya existe la PRIMARY KEY, se actualicen el resto de campos de esa entrada de la tabla
            if len(valores_insertar_personas) >= batch_size or len(valores_insertar_productos) >= batch_size or len(valores_insertar_review) >= batch_size :

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
                
                
                # Limpiar listas después de la inserción
                valores_insertar_personas.clear()

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
                
                # Limpiar listas después de la inserción
                valores_insertar_productos.clear()
                
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
                documentos_insertar_mongo.clear()

        # Inserción final si quedan datos en las listas y no se ha completado un lote
        if len(valores_insertar_personas) or len(valores_insertar_productos) or len(valores_insertar_review):
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
        
    # Guardamos los cambios tras las inserciones
    sql_conexion.commit()

    # Cerramos el cursor
    cursor.close()

############################################################################################################################################

# CARGADO DE TODOS LOS USUARIOS DE LA BASE DE DATOS
def cargar_datos_usuarios(conexion:Connection)-> dict:
    """

    Carga los datos de los usuarios desde la base de datos y los organiza en un diccionario.

    Esta función extrae todos los registros de la tabla "personas" y construye un diccionario 
    donde cada clave es el "reviewerID" y el valor asociado es una tupla con el "id_persona" y el "reviewerName.

    Args:
        conexion: Objeto de conexión a la base de datos. Se utiliza para ejecutar la consulta SQL.

    Returns:
        dict: Diccionario con los datos de los usuarios. 
              Formato: {reviewerID: (id_persona, reviewerName)}
    
    """

    # Query que extrae todos los usuarios existentes en la base de datos
    query = """
            SELECT *
            FROM personas
    """
    # Ejecutamos la consulta de búsqueda
    resultados = ejecutar_consulta_sql(conexion=conexion, sql=query) 

    # Almacenamos en un diccionario todos los datos, siendo la clave el reviewerID
    dicc_personas = {res[1]: (res[0], res[2]) for res in resultados}  # estructura -> reviewerID: (id_persona, reviewerName)

    # Devolvemos el diccionario de las personas
    return dicc_personas

# CARGADO DE TODOS LOS PRODUCTOS DE LA BASE DE DATOS
def cargar_datos_productos(conexion:Connection)-> dict:
    """

    Carga los datos de todos los productos almacenados en la base de datos y los organiza en un diccionario.

    Esta función consulta la tabla "productos" para recuperar todos los registros y construir un diccionario 
    donde cada clave es el identificador externo del producto ("asin"), y su valor asociado es una tupla con 
    el "id_producto" interno y el "tipo_producto".

    Args:
        conexion: Objeto de conexión a la base de datos. Necesario para ejecutar la consulta SQL.

    Returns:
        dict: Diccionario con los productos extraídos de la base de datos.
              Formato: {asin: (id_producto, tipo_producto)}
    
    """

    # Query que extrae todos los productos existentes en la base de datos
    query = """
            SELECT *
            FROM productos
    """
    # Ejecutamos la consulta de búsqueda
    resultados = ejecutar_consulta_sql(conexion=conexion, sql=query) 

    # Almacenamos en un diccionario todos los datos, siendo la clave el asin
    dicc_productos = {res[1]: (res[0], res[2]) for res in resultados} #  estructura -> asin: (id_producto, tipo_producto)

    # Devolvemos el diccionario de las productos
    return dicc_productos

############################################################################################################################################

# FUNCIÓN MAIN PARA EJECUTAR EL PROCESO COMPLETO
def main():
    """
    
    Función principal del script, que se encarga de ejecutar el proceso completo.

    Args:
        None.

    Returns:
        None
    
    """

    # Conexión a la base de datos SQL 
    conexion_mysql = pymysql.connect(host="localhost", user=USER_SQL, password=PASSWORD_SQL, database= NOMBRE_BASE_DATOS_SQL)

    # Avisamos de que la conexión ha ido bien
    print(f"\nNos hemos conectado a la base de datos SQL: \"{NOMBRE_BASE_DATOS_SQL}\" con éxito. ")
    
    # Conexión a la base de datos MongoDB
    dbname = get_database_mongo(NOMBRE_BASE_DATOS_MONGO_DB)
    
    # Avisamos de que la conexión ha ido bien
    print(f"\nNos hemos conectado a la base de datos MongoDB: \"{NOMBRE_BASE_DATOS_MONGO_DB}\" con éxito. ")


    # En caso de que nos hayamos podido conectar a MySQL
    if conexion_mysql:

        # Iteramos sobre todos los ficheros nuevos que queremos insertar
        for fichero in FICHEROS_DATOS_INSERTA_DATASET: 

            # Insertamoslos datos en el fichero correspondiente
            insertar_dataset(file_in=fichero, sql_conexion=conexion_mysql, mongodb_database=dbname, batch_size=BATCH_SIZE)

            # Avisamos al usuario de que todo ha ido bien
            print(f"\nYa hemos cargado los datos del fichero: \"{fichero}\" en la base de datos SQL: \"{NOMBRE_BASE_DATOS_SQL}\".")

            # Avisamos al usuario de que todo ha ido bien
            print(f"\nYa hemos cargado los datos del fichero: \"{fichero}\" en la colección \"{COLECCION_MONGODB}\" de la base de datos MongoDb : \"{NOMBRE_BASE_DATOS_MONGO_DB}\".")
              
    # Cerramos la conexión MySQl
    conexion_mysql.close()

############################################################################################################################################

if __name__=="__main__":

    try:
        # Llamamos a la función principal del archivo para que desarrolle el proceso completo
        main()

    except Exception as e:
        # Controlamos posibles excepciones
        print(f"\n>>>>>>>>>>>>>>>> ERROR: {e}")
























