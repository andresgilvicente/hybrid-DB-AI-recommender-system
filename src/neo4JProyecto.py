"""
Este script se empleará para ofrecer una interfaz al usuario y que este pueda ejecutar distintas consultas en nuestras bases de datos.
En función de los distintos filtros que aplique el usuario en sus búsquedas, se cargarán los resultados de dicha consulta en Neo4J y 
se avisará por pantalla al usuario para que pueda acceder a la base de datos de Neo4J y visualizar dichos resultados. Además de que en
el informe final del proyecto se adjuntarán todas las imágenes referentes a este archivo para que se pueda verificar el correcto 
funcionamiento del programa.

En caso de cualquier error, se recogerán las excepciones para que el programa no termine de forma abrupta en ningún caso.

Autores: Jorge Carnicero Príncipe y Andrés Gil Vicente
Grupo: 2º A IMAT
Proyecto: Proyecto Final 2024/2025 - Bases de Datos

"""

############################################################################################################################################

# Importamos las librerías necesarias
from configuracion import*
from pymongo import MongoClient
from pymongo.database import Database
import pymysql
import numpy as np
from neo4j import GraphDatabase, Driver
import random
from colorama import Fore, Style,init
import time
import os
from threading import Thread, Event
import itertools
import sys
from pymysql.connections import Connection
from typing import List

############################################################################################################################################

def detener_animacion()-> None:
    """

    Detiene una animación de carga que se ejecuta en un hilo separado.

    Esta función verifica si el hilo de animación está activo. Si lo está,
    activa un evento de detención (`stop_event`) y espera a que el hilo finalice.
    En caso de error, se muestra un mensaje en rojo en consola.

    Args:
        None

    Globals:
        hilo_animacion_carga (threading.Thread): Hilo que ejecuta la animación.
        animacion_carga_datos: Objeto que contiene un atributo `stop_event` de tipo `threading.Event`.

    Returns:
        None: Solo detiene la animación, no devuelve ningún valor.
    
    """
    global hilo_animacion_carga, animacion_carga_datos
    try:
        if hilo_animacion_carga and hilo_animacion_carga.is_alive():
            animacion_carga_datos.stop_event.set()
            hilo_animacion_carga.join()
            print(Fore.YELLOW + "Animación detenida correctamente." + Fore.RESET)
    except Exception as e:
        print(Fore.RED + f"Error al detener la animación: {e}" + Fore.RESET)
              
# EJECUCIÓN CONSULTA SQL
def ejecutar_consulta_sql(conexion:Connection, sql:str, args:list=None)-> tuple:
    """

    Nos va a permitir ejecutar una consula sql, pasandole unicamente la consulta sql y que argumentos necesita la consulta.

    Args:
        conexion (pymysql.connections.Connection): conexion propia a la base de datos que deseamos.
        sql (str): consulta sql que contine la información que buscamos.
        args (list, optional): argumentos que necesitaremos en la consulta sql. Defaults to None.

    Returns:
        result_sql(tuple): devuelve el resultado de la consulta sql.

    """
    # Creamos un cursor
    cursor = conexion.cursor()

    # Vamos a ejecutar la consulta sql que nos pasen como argumento, y distinguimos si esa query tiene argumentos dentro o no
    if args == None:
        cursor.execute(sql)
    else:        
        cursor.execute(sql,args)

    # Recogemos todos los resultados de la consulta
    result_sql = cursor.fetchall()

    # Cerramos la conexión
    cursor.close()
    
    # Devolvemos el resultado
    return result_sql

# LIMPIEZA DE LA BASE DE DATOS DE NOE4J BORRANDO SU CONTENIDO
def limpiar_database_neo4j(driver:Driver)-> None:
    """

    Elimina todos los nodos y relaciones existentes en la base de datos.

    Args:
        driver (neo4j.Driver): Conexión activa a la base de datos de Neo4j.
    
    Returns:
        None
    
    """
    with driver.session() as session:

        # Borramos todos los nodos 
        consulta_eliminacion = "MATCH (n) DETACH DELETE n"
        session.run(consulta_eliminacion)

# CREACIÓN ÍNDICE SOBRE NODO Y ATRIBUTO DEL NODO
def crear_indice(driver:Driver, index_name:str, label:str, property_name:str)-> None:
    """

    Crea un índice en Neo4j sobre una etiqueta y propiedad específica. Esta función es reutilizable y puede ser usada para 
    crear distintos índices con diferentes parámetros.

    Argss:
        driver (neo4j.Driver): instancia del Neo4j Driver (ya conectado).
        index_name (str): nombre único para el índice.
        label (str): etiqueta del nodo sobre el que se crea el índice (por ejemplo, "Person").
        property_name (str): nombre de la propiedad del nodo a indexar (por ejemplo, "name").

    Args:
        None

    """

    # Construimos la sentencia Cypher dinámica
    query = f"""
                CREATE INDEX {index_name} IF NOT EXISTS
                FOR (n:{label})
                ON (n.{property_name})
    """

    # Ejecutamos la consulta usando una sesión
    with driver.session() as session:
        session.run(query)

# CONSEGUIR TODOS LOS USUARIOS QUE HAN HECHO REVIEW DE UN PRODUCTO (ID_PRODUCTO) CONCRETO
def conseguir_usuarios_review_un_id(conexion:Connection, id_producto:int)-> List[int]:
    """
    Obtenemos una lista con los usuarios que han hehco una review sobre un producto específico.

    Args:
        conexion (pymysql.connections.Connection): Conexión a la base de datos MySQL.
        id_producto (int): ID del producto del que queremos saber que personas le han hecho un review

    Returns:
        usuarios(list[int]): Lista de IDs de usuarios que le han hecho una review
    """

    sql = """

        SELECT DISTINCT(r.id_persona)
        FROM review r
        WHERE r.id_producto = %s;

    """
    result_sql = ejecutar_consulta_sql(conexion, sql, [id_producto])
 
    usuarios = []
    for resultado in result_sql:
        usuarios.append(resultado[0])

    return usuarios

# CREACIÓN DE LA CONEXIÓN CON PYMYSQL
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
        user=USER_SQL,              
        password=PASSWORD_SQL,      
        database=NOMBRE_BASE_DATOS_SQL)                  
        
        return conexion 
    
    except Exception as error:
        print(f"Error al conectar a MySQL: {error}")  # Prevenimos excepciones, así que en caso de error mostramos dicho error
        return None

# CREACIÓN DE LA CONEXIÓN CON MONGODB
def get_database_mongo(database: str) -> Database:
    """

    Funcion para obtener la conexión a la base de datos de MongoDB. 

    Args:
        database (str): el nombre de la base de datos

    Returns:
        client[database]: la base de datos a la que nos estamos conectando, tras crearla

    """
    
    # Creamos la conexion empleando mongoClient
    client = MongoClient(CONNECTION_STRING)

    # Devolvemos la conexion de la base de datos que queremos
    return client[database]

# ESTABLECIMIENTO DE CONEXIÓN CON NEO4J
def crear_conexion_neo4j()-> Driver:
    """
    
    Establece la conexión con la base de datos de Neo4j utilizando los parámetros de configuración definidos.

    Args: 
        None

    Returns:
        neo4j.Driver: Objeto driver que permite interactuar con la base de datos.
    
    """
    # Creamos el objeto driver
    driver = GraphDatabase.driver(URI_NEO4J, auth=(USER_NEO4J, PASSWORD_NEO4J))

    # Devolvemos el objeto driver
    return driver

# LIMPIEZA TERMINAL VSC
def limpiar_pantalla()-> None:
    """
    Limpia la terminal de Visual Studio Code

    Args:
        None.

    Returns:
        None

    """
    os.system("cls" if os.name == "nt" else "clear")

############################################################################################################################################

# FUNCIONES AUXILIARES APARTADO 4.1
def calculo_similitud_pearson(articulos_en_comun:tuple, media_u1:float, media_u2:float)-> float:
    """

    Función que se encarga de hacer el cálculo de la similitud entre dos usuarios a partir de las puntuaciones que les han dado cada uno de ellos
    a ciertos productos, siendo estos productos los que ambos han puntuado, es decir la interseccón.

    Args:
        articulos_en_comun (list): lista de tuplas con la siguiente estructura:
                (id_producto, overall_u1, overall_u2)
        media_u1 (float): media de overall que tiene el usuario 1 en total entre todas sus reviews.
        media_u2 (float): media de overall que tiene el usuario 2 en total entre todas sus reviews
    
    Returns: 
        PC (float): Correlación de Pearson. Representará la similitud entre 2 usuarios distintos.
    
    """

    # Calculamos por partes los distintos términos de la f´romula del PC
    numerador = np.sum([(articulo[1]-media_u1)*(articulo[2]-media_u2) for articulo in articulos_en_comun])
    denominador_1 = np.sqrt(np.sum([(articulo[1]-media_u1)**2 for articulo in articulos_en_comun]))
    denominador_2 = np.sqrt(np.sum([(articulo[2]-media_u2)**2 for articulo in articulos_en_comun]))

    PC = numerador / (denominador_1 * denominador_2)

    return PC

def calculo_interseccion_productos_reviewed(conexion:pymysql.connections.Connection,id_persona_1:int,id_persona_2:int)-> List[tuple]:
    """

    Esta función se encarga de extraer el conjunto de productos que ambos usuarios han reseñado, es decir, que ambos han puesto 
    reseñas sobre ese mismo producto. Se extraerán tuplas del tipo: (id_producto, overall_u1, overall_u2), y se usarán posteriormente 
    para hacer los cálculos del Coeficiente de Pearson, relacionado con la similitud entre ambos usuarios.

    Args:
        conexion (pymysql.connections.Connection): Conexión a la base de datos MySQL.
        id_persona_1 (int): id del usuario 1
        id_persona_2 (int): id del usuario 2
    
    Returns:
        result_sql (list): lista de tuplas con la estructura (id_producto, overall_u1, overall_u2)
   
    """
 
    sql = """
 
        SELECT r1.id_producto, r1.overall, r2.overall
        FROM review r1 INNER JOIN review r2 ON r1.id_producto = r2.id_producto
        WHERE r1.id_persona = %s AND r2.id_persona = %s
 
    """
    # Calculamos cuáles son los productos que ambos usuarios tienen en común y también extraemos los overalls de cada uno
    result_sql = ejecutar_consulta_sql(conexion, sql, [id_persona_1,id_persona_2])

    # Devolvemos la lista de tuplas
    return result_sql

def conseguir_media_overall_por_id(conexion:Connection, id_usuario:int)-> float:
    """

    Calcula el overall medio de las reviews hechas por un usuario específico (filtramos por su identificador)
 
    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        id_usuario (int): ID de la persona 
 
    Returns:
        media_overall (float): Media del overall del usuario.

    """
    sql = """

        SELECT AVG(overall)
        FROM review
        WHERE id_persona = %s;

    """
    # Ejecutamos la consulta
    result_sql = ejecutar_consulta_sql(conexion, sql, [id_usuario])

    # Extraemos la media del resultado
    media_overall = float(result_sql[0][0])
   
   # Devolvemos la media de overall para este usuario
    return media_overall

# APARTADO 4.1
def ap_4_1_similitudes_entre_usuarios(driver:Driver, conexion:Connection, n_usuarios:int=30)-> None:
    """

    Calcula la similitud de Pearson entre los usuarios con más reviews
    y guarda las relaciones de similitud en Neo4j entre cada par de usuarios.

    Args:
        driver (Driver): Conexión activa a la base de datos de Neo4j.
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        n_usuarios (int): Número de usuarios a comparar (por defecto, los 30 con más reviews).

    Returns:
        None. No devuelve nada sino que transfiere los datos a Neo4J directamente.

    """
    # Limpiar la base de datos en caso de tener información de acciones anteriores
    limpiar_database_neo4j(driver)
    # Creamos indices para que la busqueda sea más rápida y así los MERGES sean más rápidos
    crear_indice(driver=driver, index_name="indice_entre_usuarios", label="Usuario", property_name="id")

    # Extraemos de la base de datos los ids de los usuarios
    sql = """
   
        SELECT id_persona
        FROM review
        GROUP BY id_persona
        ORDER BY COUNT(*) DESC
        LIMIT %s;
 
    """
    # Obtenemos los identificadores de los usuarios
    result_sql = ejecutar_consulta_sql(conexion, sql, [n_usuarios])

    # Extraemos el id de los resultados de la consulta anterior
    usuarios = [resultado[0] for resultado in result_sql]
 
    # Iteramos sobre la lista de identificadores
    for i in range(len(usuarios)):

        # Pillamos el identificador del usuario que vamos a comparar con el resto de usuarios
        id_usuario1 = usuarios[i]

        # Extraemos la media de overall de las reviews del usuario 1
        media_user_1 = conseguir_media_overall_por_id(conexion, id_usuario1)

        # Iteramos para comparar este usuario con todos los demás. 
        # Empezamos en i + 1 porque la similitud entre u1 y u2 es la misma que la similitud entre u1 y u2.
        for j in range(i+1, len(usuarios)):

            # Cogemos el id del segundo usuario
            id_usuario2 = usuarios[j]

            # Extraemos la media de overall de las reviews del usuario 2
            media_user_2 = conseguir_media_overall_por_id(conexion, id_usuario2)
 
            # Buscamos los artículos sobre los que ambos usuarios han hecho reviews
            articulos_en_comun = calculo_interseccion_productos_reviewed(conexion, id_usuario1, id_usuario2)
            
            # Solo calculamos la similitud entre aquellos usuarios que tienen algún artículo en común entre sus reseñas
            if len(articulos_en_comun) > 0:

                # Calculamos el Coeficiente de Pearson (similitud entre ambos usuarios)
                PC = calculo_similitud_pearson(articulos_en_comun, media_user_1, media_user_2)

                # Consulta que vamos a ejecutar en Neo4J para crear los nodos y la relación entre ambos
                consulta = """

                        MERGE (p1:Usuario {id: $id_usuario1})
                        MERGE (p2:Usuario {id: $id_usuario2})
                        MERGE (p1)-[r1:CORRELACION]->(p2)
                        MERGE (p2)-[r2:CORRELACION]->(p1)
                        SET r1.PC = $pearson, r2.PC = $pearson

                        """
                # Abrimos la sesión de Neo4J para ejecutar la query
                with driver.session() as session:

                    # Pasamos los parámetros adecuados a la consulta de Neo4j
                    session.run(consulta, id_usuario1=id_usuario1, id_usuario2=id_usuario2, pearson=PC)
            
# APARTADO 4.2
def ap_4_2_enlaces_entre_usuarios_y_articulos(conexion:Connection, driver:Driver, tipo:str, n:int)-> bool:
    """
    Selecciona aleatoriamente n artículos de un tipo específico y crea en Neo4J los nodos de esos artículos 
    junto con los usuarios que los han puntuado. Se incluyen las relaciones con la nota y el momento de la review.
    Si no hay suficientes artículos disponibles del tipo especificado, la función devuelve True como indicador de error.

    Args:
        conexion (pymysql.connections.Connection): Conexión a la base de datos MySQL.
        driver (Driver): Conexión activa a la base de datos de Neo4j.
        tipo (str): Tipo de producto seleccionado
        n (int): Número de artículos aleatorios seleccionados por el usuario.

    Returns:
        bool: True si el número solicitado de artículos supera la cantidad disponible; False en caso contrario.
              Para indicar que ha habido un error, y que no siga ejecutando
    """
    # Limpiar la base de datos en caso de tener información de acciones anteriores
    limpiar_database_neo4j(driver)
    # Creamos indices para que la busqueda sea más rápida y así los MERGES sean más rápidos
    crear_indice(driver=driver, index_name="indice_entre_usuarios", label="Usuario", property_name="id")
    crear_indice(driver=driver, index_name="indice_entre_productos", label="Producto", property_name="id")

    with driver.session() as session:
        sql = """
        SELECT DISTINCT(p.id_producto)
        FROM productos p
        INNER JOIN tipos_producto pr ON p.tipo_producto = pr.tipo_producto
        WHERE pr.nombre_tipo_producto = %s;
        """ 
        
        # Obtener todos los ids del tipo especificado
        result_sql = list(ejecutar_consulta_sql(conexion, sql, [tipo]))
        
        # En caso de querer un número de productos mayor al disponible, le sacaremos fuera.
        if n > len(result_sql):
            return True
            
        # Obtener n ids aleatorios del tipo especificado
        random_ids = random.sample(result_sql,n)

        # Iteramos por cada producto
        for tuple_id_producto in random_ids:
            id_producto = tuple_id_producto[0]
            sql = """
                SELECT r.id_persona, r.overall, r.reviewTime
                FROM review r
                WHERE r.id_producto = %s
                """
            # Conseguimos el id_persona, overall y momento, de las personas que han valorado ese producto
            result_sql = ejecutar_consulta_sql(conexion, sql, [id_producto])

            # Iteramos respecto a los usuarios que han valorado ese producto
            for resultado in result_sql:
                usuario,overall,tiempo = resultado[0],resultado[1],resultado[2]

                consulta1 = """
                            MERGE (u:Usuario {id: $id_usuario})
                            MERGE (p:Producto {id: $id_producto})
                            MERGE (u) - [r:REVIEWED] -> (p)
                            SET r.nota = $overall, r.tiempo = $tiempo
                            """
                # Ejecutamos la consulta de MERGE creando una relación en la que el usuario a valorado ese producto, añadiendo como atributos overall y el momento
                session.run(consulta1, id_producto=id_producto, id_usuario=usuario,overall=overall,tiempo=tiempo)
    
    return False

# APARTADO 4.3
def ap_4_3_usuarios_con_multiples_tipos_articulos(conexion:Connection, driver:Driver)-> None:
    """
    Identifica los primeros 400 usuarios (ordenados por nombre) que han puntuado productos de al menos dos categorias distintas.
    Crea en Neo4J los nodos correspondientes a usuarios y tipos de producto, y enlaza cada usuario con los tipos que ha puntuado,
    incluyendo la cantidad de reviews realizadas ha cada categoría.

    Args:
        conexion (pymysql.connections.Connection): Conexión a la base de datos MySQL.
        driver (Driver): Conexión activa a la base de datos de Neo4j.

    Returns:
        None
    """
    # Limpiar la base de datos en caso de tener información de acciones anteriores
    limpiar_database_neo4j(driver)
    # Creamos indices para que la busqueda sea más rápida y así los MERGES sean más rápidos
    crear_indice(driver=driver, index_name="indice_entre_usuarios", label="Usuario", property_name="id")
    crear_indice(driver=driver, index_name="indice_entre_categorias", label="Categoria", property_name="Nombre_Categoria")

    with driver.session() as session:
        sql1 = """
        SELECT DISTINCT sub.id_persona
        FROM (
            SELECT DISTINCT r.id_persona, p.reviewerName
            FROM review r
            INNER JOIN personas p ON p.id_persona = r.id_persona
            ORDER BY p.reviewerName
            LIMIT 400
        ) AS sub
        INNER JOIN review r ON r.id_persona = sub.id_persona
        INNER JOIN productos pr ON pr.id_producto = r.id_producto
        INNER JOIN tipos_producto tp ON pr.tipo_producto = tp.tipo_producto
        GROUP BY r.id_persona
        HAVING COUNT(DISTINCT tp.tipo_producto) >= 2;
        """
        # Obtener los ids que hayan consumido más de 2 categorias
        result_sql1 = ejecutar_consulta_sql(conexion, sql1)
        
        # Iteramos por cada usuario 
        for usuario in result_sql1:
            id_usuario = usuario[0] # Id del usuario ya que es una tupla
            sql2 = """
                SELECT tp.nombre_tipo_producto as categoria, count(*)
                FROM productos pr
                INNER JOIN review r ON r.id_producto = pr.id_producto
                INNER JOIN tipos_producto tp ON pr.tipo_producto = tp.tipo_producto
                WHERE r.id_persona = %s
                GROUP BY categoria;
                """
            # Conseguimos el nombre_tipo_producto y cuantas veces a hecho una review a ese producto, por cada usuario de la lista
            result_sql2 = ejecutar_consulta_sql(conexion, sql2,args=[id_usuario])
            
            # Iteramos los resultados que serán cada tipo de producto que ha puntuado (100% tienen que ser más o igual a 2)
            for resultado in result_sql2:
                # Conseguimos la información que no interesa de cada resultado
                categoria,cantidad_rw_cat = resultado[0],resultado[1]
                consulta1 = """
                            MERGE (u:Usuario {id: $id_usuario})
                            MERGE (c:Categoria {Nombre_Categoria: $categoria})
                            MERGE (u) - [r:REVIEWED] -> (c)
                            SET r.cantidad_reviews = $cantidad_rw
                            """
                # Ejecutamos la consulta de MERGE creando una relación en la que el usuario a hecho una review a un tipo de producto, y ponemos de atributo
                # a cuantos productos de ese tipo ha hecho una review ese usuario
                session.run(consulta1, categoria=categoria, id_usuario=id_usuario,cantidad_rw=cantidad_rw_cat)

# APARTADO 4.4
def ap_4_4_articulos_populares_y_comunes(conexion:Connection, driver:Driver)-> None:
    """

    Obtener los 5 productos más populares con menos de 40 reviews, creando relaciones entre productos y usuarios que los han puntuado en Neo4J,
    y calculando cuántos artículos en común han valorado cada par de usuarios.

    Args:
        conexion (pymysql.connections.Connection): Conexión a la base de datos MySQL.
        driver (Driver): Conexión activa a la base de datos de Neo4j.

    Returns:
        None

    """
    # Limpiar la base de datos en caso de tener información de acciones anteriores
    limpiar_database_neo4j(driver)
    # Creamos indices para que la busqueda sea más rápida y así los MERGES sean más rápidos
    crear_indice(driver=driver, index_name="indice_entre_usuarios", label="Usuario", property_name="id")
    crear_indice(driver=driver, index_name="indice_entre_productos", label="Producto", property_name="id")

    # Abrimos la sesión de Neo4J
    with driver.session() as session:
        # Query de SQL a ejecutar
        sql = """

            SELECT r.id_producto, count(id_producto) as cont_reviews
            FROM review r
            GROUP BY id_producto
            HAVING cont_reviews < 40
            ORDER BY cont_reviews DESC
            LIMIT 5;

            """
        # Conseguimos los 5 artículos más populares con menos de 40 reviews
        result_sql = ejecutar_consulta_sql(conexion, sql)

        # Metemos en una lista los 5 identificadores de los productos
        id_productos = [i[0] for i in result_sql]
    
    # Iteramos por cada producto
        for id_producto in id_productos:

            # Conseguimos los usuarios que han hecho reviews de este producto
            usuarios_review = conseguir_usuarios_review_un_id(conexion, id_producto)

            # Iteramos por cada usuario
            for i in range(len(usuarios_review)):

                # Cogemos el id del usuario
                id_usuario_1 = usuarios_review[i]

                # Consulta para crear nodos de usuarios y productos, con relaciones entre ellas
                consulta1 = """

                        MERGE (u:Usuario {id: $id_usuario})
                        MERGE (p:Producto {id: $id_producto})
                        MERGE (u) - [:REVIEWED] -> (p)

                        """
                # Ejecutamos la consulta de MERGE
                session.run(consulta1, id_producto=id_producto, id_usuario=id_usuario_1)
    
                for j in range(i+1,len(usuarios_review)):
                    
                    # Cogemos el id del usuario
                    id_usuario_2 = usuarios_review[j]

                    # Calculamos los productos que tienen reviews de ambos usuarios
                    articulos_en_comun = calculo_interseccion_productos_reviewed(conexion, id_usuario_1, id_usuario_2)
    
                    # Consulta para crear nodos de personas, con relaciones entre ellas
                    consulta2 = """

                            MERGE (p1:Usuario {id: $id_usuario1})
                            MERGE (p2:Usuario {id: $id_usuario2})
                            MERGE (p1)-[r1:ARTICULOS_COMUNES]->(p2)
                            MERGE (p2)-[r2:ARTICULOS_COMUNES]->(p1)
                            SET r1.cantidad = $cantidad, r2.cantidad = $cantidad

                            """
                    # Añadimos un atributo a la relación entre los usuarios, representa la cantidad de artículos que ambos han reseñado
                    cantidad_articulos_comun = len(articulos_en_comun)
                    
                    # Ejecutamos la consulta de MERGE
                    session.run(consulta2, id_usuario1=id_usuario_1, id_usuario2=id_usuario_2, cantidad = cantidad_articulos_comun)

############################################################################################################################################

# MENUS
def menu_opciones_neo4j()-> str:
    """

    Mostrar menú de opciones para las funcionalidades implementadas en Neo4J, permite al usuario seleccionar qué acción quiere ejecutar.

    Args:
        None.

    Returns:
        str: número de la opción elegida

    """

    print("\n" + Fore.CYAN + Style.BRIGHT + "=" * 66)
    print(Fore.YELLOW + Style.BRIGHT + "              MENÚ FUNCIONALIDADES NEO4J - REVIEWS")
    print(Fore.CYAN + Style.BRIGHT + "=" * 66)

    print(Fore.GREEN + " 1 ▶  " + Fore.WHITE + "Similitudes entre usuarios (apartado 4.1)")
    print(Fore.GREEN + " 2 ▶  " + Fore.WHITE + "Enlaces entre usuarios y artículos (apartado 4.2)")
    print(Fore.GREEN + " 3 ▶  " + Fore.WHITE + "Usuarios con múltiples tipos de artículos (apartado 4.3)")
    print(Fore.GREEN + " 4 ▶  " + Fore.WHITE + "Artículos populares y artículos en común (apartado 4.4)")
    print(Fore.RED   + " 5 ▶  " + Fore.WHITE + "Salir del menú ❌")

    print(Fore.CYAN + Style.BRIGHT + "=" * 66)

    opcion = input(Fore.GREEN + Style.BRIGHT + "\n¿Qué opción quieres ejecutar? " + Fore.RESET)

    return opcion

def menu_categoria_neo4j(dict_opciones_categoria:dict) -> str:
    """

    Mostrar menu de categorias disponibles a analizar dentro de un gráfico.

    Args:
        dict_opciones_categoria (dict): contiene todas las categorias disponibles.

    Returns:
        opcion (str): número de la opción elegida.

    """
    print("\n" + Fore.CYAN + Style.BRIGHT + "=" * 50)
    print(Fore.YELLOW + Style.BRIGHT + "              MENÚ ELEGIR CATEGORÍA")
    print(Fore.CYAN + Style.BRIGHT + "="  * 50)
    
    # Printear la opción de la categoria, con el diccionario para que sea escalable por si meten más archivos
    for categoria in dict_opciones_categoria:
        if dict_opciones_categoria[categoria] != "Todos":
            print(Fore.GREEN + Style.BRIGHT + f" {categoria} ▶  " + Fore.WHITE + Style.BRIGHT + f"{dict_opciones_categoria[categoria]}")

    print(Fore.CYAN + Style.BRIGHT + "=" * 50)

    # Preguntar la opción
    opcion = input(Fore.YELLOW + Style.BRIGHT + "\n¿Qué opción quieres? " + Fore.RESET)

    # Si la opción no está disponible
    while opcion not in [str(i+1) for i in range(len(dict_opciones_categoria)-1)]:
        print(Fore.RED + Style.BRIGHT + "\n❌ OPCIÓN NO VÁLIDA ❌")
        time.sleep(0.5)
        opcion = input(Fore.YELLOW + Style.BRIGHT + "\n¿Qué opción quieres? " + Fore.RESET)

    return opcion

def pedir_num_articulos() -> int:
    """

    Pregunta al usuario cuántos artículos aleatorios desea seleccionar.
    Asegura que la respuesta es un número entero positivo.

    Args:
        None

    Returns:
        int: número de artículos

    """
    print("\n" + Fore.CYAN + Style.BRIGHT + "=" * 50)
    print(Fore.YELLOW + Style.BRIGHT + "      SELECCIÓN DEL NÚMERO DE ARTÍCULOS")
    print(Fore.CYAN + Style.BRIGHT + "=" * 50)
    # Para la opción 2 para elegir que nº de artículos quiere
    num = None
    while num is None:
        # Preguntarle los números
        entrada = input(Fore.GREEN + Style.BRIGHT + "¿Cuántos artículos aleatorios quieres seleccionar? " + Fore.RESET).strip()
        # Confirgar que es un int
        if entrada.isdigit():
            num = int(entrada)
            if num <= 0:
                print(Fore.RED + "Por favor, introduce un número entero mayor que 0." + Fore.RESET)
                num = None
        else:
            print(Fore.RED + "Entrada no válida. Introduce un número entero positivo." + Fore.RESET)

    return num

def animacion_carga_datos(mensaje="Cargando información en Neo4J")-> None:
    """
    
    Muestra una animación de carga con puntos en movimiento. Hay que ejecutar esta función en un hilo separado y 
    detenerla usando una bandera de control.

    Args:
        mensaje (str, optional): Texto que se mostrará junto con la animación. 
                                 Por defecto es "Cargando información en Neo4J".

    Returns:
        None: Esta función no devuelve ningún valor. Muestra la animación en consola hasta que se active el evento de detención.

    """
    for punto in itertools.cycle(["", ".", "..", "..."]):
        if animacion_carga_datos.stop_event.is_set():
            break
        sys.stdout.write(f"\r{Fore.YELLOW}{Style.BRIGHT}{mensaje}{punto}{Fore.RESET}   ")
        sys.stdout.flush()
        time.sleep(0.5)
    sys.stdout.write("\r" + " " * 50 + "\r")  # Limpia la línea

def menu_final_exito(mensaje: str = "¡Información cargada con éxito en Neo4J! 🚀")-> None:
    """

    Muestra un mensaje final llamativo indicando que la carga o proceso ha sido exitoso,
    e invita al usuario a revisar los datos en Neo4J.

    Args:
        mensaje (str): Mensaje personalizado que se quiere mostrar.

    Returns:
        None

    """
    # Mensaje para confirmar que ha terminado bien
    mensaje_secundario = "Ya puedes ir y explorar la visualización."

    marco = "=" * (max(len(mensaje), len(mensaje_secundario)) + 10)

    print("\n" + Fore.GREEN + marco)
    print(Fore.WHITE + Style.BRIGHT + "  ✅  " + Fore.GREEN + mensaje)
    print(Fore.CYAN + "  📊  " + mensaje_secundario)
    print(Fore.GREEN + marco + "\n")
    time.sleep(5)

def menu_final_error(mensaje: str = "Error: tamaño seleccionado")-> None:
    """
    Muestra un mensaje de error a la hora de elegir cantidad de productos

    Args:
        mensaje (str): Mensaje personalizado de error.

    Returns:
        None

    """
    mensaje_secundario = "Has introducido un tamaño que no es compatible con la cantidad de productos encontrados en esa categoría"

    marco = "=" * (max(len(mensaje), len(mensaje_secundario)) + 10)

    print("\n" + Fore.RED + marco)
    print(Fore.WHITE + Style.BRIGHT + "  ❌  " + Fore.RED + mensaje)
    print(Fore.YELLOW + "  ⚠️   " + mensaje_secundario)
    print(Fore.RED + marco + "\n")

    time.sleep(5)

############################################################################################################################################

# MAIN
def main()-> None:
    """
    
    Función principal del script, que se encarga de ejecutar el proceso completo.

    Args:
        None.

    Returns:
        None
        
    """
    global hilo_animacion_carga, animacion_carga_datos
    
    # Para restablecer los colores de los mensajes una vez termina
    init(autoreset=True)

    # Conectarnos a mysql
    conexion_mysql = conectar_mysql()
    ejecutando = True

    if conexion_mysql:
        
        # Consulta para conseguir todas las categorías disponibles y hacer que sea escalable
        sql_tipos = """
                SELECT tipo_producto,nombre_tipo_producto
                FROM tipos_producto;
                """
        result_sql_tipos = ejecutar_consulta_sql(conexion=conexion_mysql,sql=sql_tipos)

        # Creamos y completamos el diccionario con las categorías disponibles y les asociamos un número
        dict_opciones_categoria = {}
        for elemento in result_sql_tipos:
            dict_opciones_categoria[str(elemento[0] + 1)] = elemento[1]

        # Añadimos la categoría de Todos para cuando este disponible para seleccionar
        dict_opciones_categoria[str(len(result_sql_tipos)+1)] = "Todos"

        # Bucle para mostrar las opciones
        while ejecutando:
            # Creación del hilo para ir mostrando un mensaje mientras carga los datos
            animacion_carga_datos.stop_event = Event()
            animacion_carga_datos.stop_event.clear()
            hilo_animacion_carga = Thread(target=animacion_carga_datos, args=("Cargando información en Neo4J",))

            limpiar_pantalla()
            # Mostrar menu para que el usuario seleccione la opción que quiera
            opcion = menu_opciones_neo4j()

            if opcion == "1":

                limpiar_pantalla()
                # Iniciamos la anmación
                hilo_animacion_carga.start()
                # Ejecutamos el apartado deseado por la opción
                ap_4_1_similitudes_entre_usuarios(driver=driver,conexion=conexion_mysql,n_usuarios=30)
                
                # Hacemos que acabe el hilo
                animacion_carga_datos.stop_event.set()
                # Lo esperamos
                hilo_animacion_carga.join()
                limpiar_pantalla()
                # Mostramos el mensajes de que puede ir a ver la información
                menu_final_exito(mensaje="¡Información cargada con éxito en Neo4J! 🚀")

            elif opcion == "2":

                limpiar_pantalla()
                # Pedimos la categoría que quiere analizar
                tipo = menu_categoria_neo4j(dict_opciones_categoria=dict_opciones_categoria)
                limpiar_pantalla()
                # Pedimos el número de artículo
                n = pedir_num_articulos()

                # Empezamos el hilo de la animación de espera
                hilo_animacion_carga.start()

                error = ap_4_2_enlaces_entre_usuarios_y_articulos(driver=driver,conexion=conexion_mysql,tipo = dict_opciones_categoria[tipo],n=n)
                
                # Error será true si el número de articulos seleccionado es mayor al permitido
                if error:
                    # Acabamos el hilo
                    animacion_carga_datos.stop_event.set()
                    hilo_animacion_carga.join()
                    # Mensaje de que tamaño seleccionado no permitido
                    menu_final_error(mensaje= "Error: tamaño seleccionado no es posible por la cantidad de productos encontrados")

                else:
                    # Hacemos que acabe el hilo
                    animacion_carga_datos.stop_event.set()
                    # Lo esperamos
                    hilo_animacion_carga.join()
                    limpiar_pantalla()
                    # Mostramos el mensajes de que puede ir a ver la información
                    menu_final_exito(mensaje="¡Información cargada con éxito en Neo4J! 🚀")

            elif opcion == "3":

                limpiar_pantalla()
                # Iniciamos la anmación
                hilo_animacion_carga.start()
                # Ejecutamos el apartado deseado por la opción
                ap_4_3_usuarios_con_multiples_tipos_articulos(driver=driver,conexion=conexion_mysql)

                # Hacemos que acabe el hilo
                animacion_carga_datos.stop_event.set()
                # Lo esperamos
                hilo_animacion_carga.join()
                limpiar_pantalla()
                # Mostramos el mensajes de que puede ir a ver la información
                menu_final_exito(mensaje="¡Información cargada con éxito en Neo4J! 🚀")

            elif opcion == "4":

                limpiar_pantalla()
                # Iniciamos la anmación
                hilo_animacion_carga.start()
                # Ejecutamos el apartado deseado por la opción
                ap_4_4_articulos_populares_y_comunes(driver=driver,conexion=conexion_mysql)

                # Hacemos que acabe el hilo
                animacion_carga_datos.stop_event.set()
                # Lo esperamos
                hilo_animacion_carga.join()
                limpiar_pantalla()
                # Mostramos el mensajes de que puede ir a ver la información
                menu_final_exito(mensaje="¡Información cargada con éxito en Neo4J! 🚀")

            elif opcion == "5":
                # Opción de salir
                print(Fore.WHITE + Style.BRIGHT + "\nSaliendo del menú Neo4J... ¡Hasta la próxima!\n" + Fore.RESET)
                time.sleep(1)
                ejecutando = False
                limpiar_pantalla()

            else:
                # Opción no valida de acción
                print(Fore.RED + "\nOpción no válida. Intenta de nuevo." + Fore.RESET)
                time.sleep(1)
        
############################################################################################################################################

if __name__ == "__main__":
    try:
        
        # Nos conectamos a Neo4J para que podamos ejecutar correctamente el programa
        driver = crear_conexion_neo4j()

        # Llamamos a la función principal del archivo para que desarrolle el proceso completo
        main()

    # Capturamos errores y excepciones
    except KeyboardInterrupt:
        print(Fore.RED + "\n\nInterrupción detectada. Cancelando ejecución..." + Fore.RESET)
        detener_animacion()
    
    except Exception as e:
        detener_animacion()
        print("⛔ Proceso interrumpido")
        print(f"\nERROR: {e}")





