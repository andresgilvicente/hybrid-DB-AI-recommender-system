"""
Este script se empleará para desplegar una aplicación Python que permita el acceso y visualización de los datos en función de lo que desee 
un usuario. Se ofrecerá una interfaz al usuario para que decida las funcionalidades que desea utilizar, mostrándose así un gráfico referente 
a la información que este haya querido consultar en la base de datos. 

Además, se mostrarán mensajes indicativos con distintos colores para facilitar la comprensión de los usuarios y hacer que su experiencia sea 
más amena. En caso de cualquier error, se recogerán las excepciones para que el programa no termine de forma abrupta en ningún caso.

Autores: Jorge Carnicero Príncipe y Andrés Gil Vicente
Grupo: 2º A IMAT
Proyecto: Proyecto Final 2024/2025 - Bases de Datos

"""

############################################################################################################################################

# Importamos las librerías necesarias
import pymysql
import time
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo import MongoClient
from configuracion import *
import matplotlib.pyplot as plt
from itertools import zip_longest
from wordcloud import WordCloud
import os
from colorama import Fore, Style,init
from threading import Thread
from pymysql.connections import Connection

############################################################################################################################################

# FUNCIONES AUXILIARES
def get_database_mongo(database:str) -> Database:

    """

    Funcion para obtener la conexión a la base de datos de MongoDB. 

    Args:
        database (str): el nombre de la base de datos

    Returns:
        client[database]: la base de datos a la que nos estamos conectando, tras crearla

    """
    
    # Creamos la conexion empleando MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Devolvemos la conexion de la base de datos que queremos
    return client[database]

def sumar_listas(*listas:list)-> list:
    """
    Suma elemento a elemento todas las listas que se pasen como argumentos.
    Si las listas tienen diferente longitud, se rellenan con ceros.

    Args:
        listas (list): son las listas que se van a sumar.

    Returns:
        (list): lista que contiene la suma de los elementos de las listas que se pasan como argumentos.


    """
    return [sum(tupla) for tupla in zip_longest(*listas, fillvalue=0)]

def ejecutar_consulta_sql(conexion, sql:str, args:list=None)-> tuple:
    """

    Nos va a permitir ejecutar una consula sql, pasandole unicamente la consulta sql y que argumentos necesita la consulta.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
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

############################################################################################################################################

# CONSULTA 1
def consulta1_mostrar_evolucion_reviews_por_anio(conexion:Connection, tipo:str)-> None:
    """

    Vamos a obtener la "información" necesaria para representar el histograma que nos muestra la cantidad de consultas
    por cada año en función de la categoría seleccionada en tipo.
    
    Y plotearemos un histograma mostrando la cantidad de reviews por año.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        tipo (str): categoría elegida por el usuario

    Returns:
        None. Solo hace el plot.

    """

    cantidades = []

    # Consultas para conseguir el menor y mayor año disponible en la base de datos
    sql_year_menor = """
                SELECT MIN(YEAR(reviewTime))
                FROM review;
                    """
    sql_year_mayor = """
                SELECT MAX(YEAR(reviewTime))
                FROM review;
                    """
    year_menor,year_mayor = ejecutar_consulta_sql(conexion=conexion,sql=sql_year_menor)[0][0],ejecutar_consulta_sql(conexion=conexion,sql=sql_year_mayor)[0][0] 

    # Poner todos los años disponibles, podemos hacer una consulta para saberlo o algo asi ns
    years = [i for i in range(year_menor,year_mayor+1)]
    # Recorremos todos los años posibles y hacemos la consulta para cada año
    for year in years: 
        # La función realiza la consulta y nos va a devolver la cantidad de reviews en el año que le pasemos
        cantidad_reviews = conseguir_datos_reviews_año_consulta1(conexion,tipo,year)
        # Guardamos la cantidad
        cantidades.append(cantidad_reviews)
    
    # Vamos a tener 2 listas del mismo tamaño, ya que para año va a haber una cantidad
    # Plot del histograma
    plt.figure(figsize=(12, 6))
    plt.bar(years, cantidades, color="skyblue")
    plt.xlabel("Años")
    plt.ylabel("Cantidad total")
    plt.title(f"Reviews por año de {tipo}")
    plt.xticks(years)
    plt.show()
      
def conseguir_datos_reviews_año_consulta1(conexion:Connection, tipo:str, ano:int) -> int:
    """

    Obtendremos la cantidad de reviews en un año concreto de una categoría de productos, que nos los especificara el usuario

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        tipo (str): categoría elegida por el usuario
    
    Returns:
        cantidad(int): número de reviews en un año concreto de una categorá específica

    """
    # Distinguir si nos ha pedido que le demos de todos a la vez o de un tipo en concreto
    if tipo != "Todos":
        # Query que nos consigue el total de produtos en función del año y el tipo de producto
        sql ="""
        SELECT count(*)
        FROM review r
        INNER JOIN productos p ON p.id_producto = r.id_producto
        INNER JOIN tipos_producto pr ON p.tipo_producto = pr.tipo_producto
        WHERE YEAR(r.reviewTime) = %s AND pr.nombre_tipo_producto = %s;
        """
        result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql,args=[ano,tipo])

        # Guardamos la cantidad
        cantidad = result_sql[0][0]

    # En caso de que nos lo pida de todos
    elif tipo == "Todos":
        cantidad = 0
        # Lista con todos los tipos de categoría disponibles
        tipos = ["Toys_and_Games","Video_Games","Digital_Music","Musical_Instruments"]
        # Recorrer los tipos
        for tipo in tipos:
            # Vamos sumando la cantidad de cada tipo, ya que si los llamamos individual nos dan la cantidad
            cantidad += conseguir_datos_reviews_año_consulta1(conexion,tipo,ano)

    return cantidad

############################################################################################################################################

# CONSULTA 2
def consulta2_mostrar_evolucion_popularidad_articulos(conexion:Connection, tipo:str)-> None:
    """

    Conseguir la información necesaria para plotear un gráfico que nos mostrara la cantidad de artículos con un número de review específico
    pudiendo filtrar esta información en función de una categoría

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        tipo (str): categoría elegida por el usuario

    Returns:
        None. Solo hace el plot.

    """
    # Conseguimos la lista necesaria para el plot
    reviews_popularidad_sql = conseguir_popularidad_consulta2(conexion,tipo)

    plt.figure(figsize=(10, 5))
    plt.plot(reviews_popularidad_sql, linewidth=1)  # Dibuja la curva
    plt.xlabel("Artículos")
    plt.ylabel("Número de reviews")
    plt.title(f"Evolución de popularidad de {tipo}")
    plt.show()

def conseguir_popularidad_consulta2(conexion:Connection, tipo:str) -> list:
    """

    Conseguir una lista con la cantidad de reviews por cada asin, ordenada por la cantidad de reviews descendentemente.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        tipo (str): categoría elegida por el usuario.
    
    Returns:
        reviews_popularidad_sql(list): lista con la cantidad de reviews por articulo, que pertenecen a una categoría concreta.

    """
    # Filtrar en función de si quiere de todos los tipos a la vez o de un tipo en concreto
    if tipo != "Todos":
        # Query para conseguir la cantidad de reviews por cada asin, ya ordenado
        sql ="""
            SELECT count(*) as contador
            FROM review r
            INNER JOIN productos p ON p.id_producto = r.id_producto
            INNER JOIN tipos_producto pr ON pr.tipo_producto = p.tipo_producto
            WHERE pr.nombre_tipo_producto = %s
            GROUP BY asin
            ORDER BY contador DESC;
            """
        result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql,args=[tipo])

    elif tipo == "Todos":
        # Query que nos consigue todos directamente
        sql ="""
            SELECT count(*) as contador
            FROM review r
            INNER JOIN productos p ON p.id_producto = r.id_producto
            INNER JOIN tipos_producto pr ON pr.tipo_producto = p.tipo_producto
            GROUP BY asin
            ORDER BY contador DESC;
            """
        
        result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql)

    reviews_popularidad_sql = []
    # Por cada review guardamos la cantidad
    for res in result_sql:
        reviews_popularidad_sql.append(res[0])
    
    return reviews_popularidad_sql
    
############################################################################################################################################

# CONSULTA 3
def consulta3_mostrar_histograma_por_nota(conexion:Connection, tipo:str, producto:str)-> None:
    """

    Convertir la información obtenida de un diccionario con los overalls y la cantidad de reviews, filtrado por categoría o por un producto concreto.
    Luego haremos un histograma, mostrando por cada overall la cantidad de reviews que tiene

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        tipo (str): categoría elegida por el usuario
        producto (str): ASIN del producto buscado

    Returns:
        None. Solo hace el plot.

    """
    # Conseguimos un dict con la información
    dict_final_ordenado = conseguir_numero_nota_consulta3(conexion,tipo,producto)

    overalls = []
    total_reviews_overall = []
    # Recorremos el dict y pillamos la información que nos interesa guardandola en las listas
    for overall in dict_final_ordenado:
        overalls.append(overall)
        total_reviews_overall.append(dict_final_ordenado[overall])

    plt.bar(overalls, total_reviews_overall, color="skyblue")
    if tipo != None:
        plt.xlabel(f"Overall de {tipo}")
    else:
        plt.xlabel(f"Overall de {producto}")
    plt.ylabel("Reviews totales")
    plt.title("Reviews por calificación")
    plt.xticks(overalls)
    plt.show()

def conseguir_numero_nota_consulta3(conexion:Connection, tipo:str, producto:str) -> dict:
    """

    Conseguir un diccionario que relacione cada overall con la cantidad de reviews que ha recibido, pudiendo filtrar por un producto 
    en concreto o por una categoría específica.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        tipo (str): categoría elegida por el usuario.
        producto (str): ASIN del producto buscado.
    
    Returns:
        dict_overall_reviews(dict): diccionario que relacióna cada overall con su cantidad de reviews.

    """
    # Filtrar en función de si quería buscar en función del tipo de producto o en función de un producto en específico
    if tipo != None:
        # Filtrar en función de si quiere de todos los tipos a la vez o de un tipo en concreto        
        if tipo != "Todos":
            # Query para tener la cantidad de reviews en función del overall, ya ordenada
            sql ="""
                SELECT r.overall,count(*)
                FROM review r
                INNER JOIN productos p ON p.id_producto = r.id_producto
                INNER JOIN tipos_producto pr ON pr.tipo_producto = p.tipo_producto
                WHERE pr.nombre_tipo_producto = %s
                GROUP BY r.overall
                ORDER BY r.overall;
                """
            result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql,args = [tipo])

        # En caso de querer todos, simplemente no le forzaremos a filtrar por tipo
        elif tipo == "Todos":
            sql ="""
                SELECT r.overall,count(*)
                FROM review r
                INNER JOIN productos p ON p.id_producto = r.id_producto
                INNER JOIN tipos_producto pr ON pr.tipo_producto = p.tipo_producto
                GROUP BY r.overall
                ORDER BY r.overall;
                """
            result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql)
        
        dict_overall_reviews = {}
        # Recorremos el result y por cada overall le asignamos su cantidad
        for res in result_sql:
            dict_overall_reviews[res[0]] = res[1]
            
        return dict_overall_reviews

    
    elif producto != None:
        
        sql ="""
            SELECT r.overall,count(*)
            FROM review r
            INNER JOIN productos p ON p.id_producto = r.id_producto
            WHERE asin = %s
            GROUP BY r.overall
            ORDER BY r.overall;
            """
            
        result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql,args = [producto])
        
        dict_overall_reviews = {}
        for res in result_sql:
            dict_overall_reviews[res[0]] = res[1]
            
        return dict_overall_reviews
    
############################################################################################################################################

# CONSULTA 4
def consulta4_mostrar_evolucion_reviews_tiempo(conexion:Connection, tipo:str)-> None:
    """

    Convertir la información obtenida de un diccionario que nos relaciona los unixReviewTime con la cantidad de reviews hechas en ese tiempo concreto, en listas
    para luego poder mostrar un plot que nos muestre la evolución de la cantidad de reviews a medida que va avanzando el tiempo.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        tipo (str): categoría elegida por el usuario
    
    Returns:
        None. Solo hace el plot.
        
    """

    # Nos da un diccionario tiempo:cantidad_reviews
    dict_time_cantidad = conseguir_timestamp_cantidad_reviews_consulta4(conexion,tipo)
    # Recorremos el diccionario para añadirlo a listas y ser más fácil de plotear
    time = []
    cantidad = []
    contador = 0
    for tiempo in dict_time_cantidad:
        time.append(tiempo)
        # Para ir acumulando las reviews, y asi ir viendo como aumentan
        contador += dict_time_cantidad[tiempo]
        cantidad.append(contador)
    
    plt.figure(figsize=(12, 6))
    plt.plot(time, cantidad)

    plt.title(f"Evolucion de las reviews a lo largo del tiempo de todos los productos de {tipo}")
    plt.xlabel("Tiempo")
    plt.ylabel("Numero de reviews hasta ese momento")

    plt.grid(True)
    plt.tight_layout()
    plt.show()

def conseguir_timestamp_cantidad_reviews_consulta4(conexion:Connection, tipo:str) -> dict:
    """

    Conseguir la cantidad de reviews por unixReviewTime específicos. 

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        tipo (str): categoría elegida por el usuario.
    
    Returns:
        time_cantidad_sql(dict): diccionario que relaciona un unixReviewTime con la cantidad de reviews hechas en ese momento.

    """

    # Filtrar en función de si quiere de todos los tipos a la vez o de un tipo en concreto
    if tipo != "Todos":
        sql ="""
            SELECT unixReviewTime,count(*)
            FROM review r
            INNER JOIN productos p ON p.id_producto = r.id_producto
            INNER JOIN tipos_producto pr ON p.tipo_producto = pr.tipo_producto
            WHERE pr.nombre_tipo_producto = %s
            GROUP BY unixReviewTime
            ORDER BY unixReviewTime;
            """
        result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql,args=[tipo])

    # Cuando el tipo es Todos hacemos la misma query pero ahora sin filtrar por tipo de producto
    elif tipo == "Todos":
        sql ="""
            SELECT unixReviewTime,count(*)
            FROM review r
            INNER JOIN productos p ON p.id_producto = r.id_producto
            INNER JOIN tipos_producto pr ON p.tipo_producto = pr.tipo_producto
            GROUP BY unixReviewTime
            ORDER BY unixReviewTime;
            """
        result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql)
    
    time_cantidad_sql = {}
    # Vamos a asiganndo a cada unixReviewTime la cantidad de reviews
    for res in result_sql:
        time_cantidad_sql[res[0]] = res[1]
    
    # Devolvemos el diccionario
    return time_cantidad_sql

############################################################################################################################################

# CONSULTA 5
def consulta5_mostrar_histograma_reviews_por_usuario(conexion:Connection)-> None:
    """

    Plotear la cantidad de usuarios en relación al número de reviews que han hecho (cuantas personas han hecho X número de reviews).

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.

    Returns:
        None. Solo hace el plot.
        
    """
    # Nos da un diccionario tiempo:cantidad_reviews
    numero_reviews,numero_users = conseguir_usuarios_cantidad_consulta5(conexion)
    
    plt.figure(figsize=(12, 6))
    plt.bar(numero_reviews, numero_users)

    plt.title("Reviews por usuario")
    plt.xlabel("Numero de reviews")
    plt.ylabel("Numero de usuarios")

    plt.grid(True)
    plt.tight_layout()
    plt.show()

def conseguir_usuarios_cantidad_consulta5(conexion:Connection) -> tuple[list, list]:
    """

    Conseguir la información de cuantas personas han hecho X número de reviews.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
    
    Returns:
        numero_reviews(list): lista con el número de reviews.
        numero_users(list): lista con el número de usuarios.

    """
    
    # Query para obtener la cantidad de usuarios que han puesto X reviews
    sql ="""
        SELECT contador, count(*)
        FROM (SELECT count(*) as contador
            FROM review r
            INNER JOIN personas p ON p.id_persona = r.id_persona
            GROUP BY p.id_persona) sub
        GROUP BY contador
        ORDER BY contador;
        """
        
    result_sql = ejecutar_consulta_sql(conexion=conexion,sql=sql)

    # Como recibimos directamente ya el número de gente que ha hecho X reviews, las almecenamos como queremos mostrarlo en el gráfico
    numero_reviews = []
    numero_users = []
    for res in result_sql:
        numero_reviews.append(res[0])
        numero_users.append(res[1])
        
    return numero_reviews,numero_users

############################################################################################################################################

# CONSULTA 6
def consulta6_generar_nube_palabras_por_categoria(conexion:Connection, collection_name:Collection, tipo:str)-> None:
    """

    Mostrar un gráfico de palabras, sobre las palabras más comunes en el campo de summary de una categoría en específico

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        collection_name (Collection): Objeto de la colección dentro de la base de datos
        tipo (str): categoría elegida por el usuario

    Returns:
        None. Solo hace el plot.

    """
    texto = conseguir_summary_tipo_consulta6(conexion,collection_name,tipo)

    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(texto)
    plt.figure(figsize=(12, 6))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.tight_layout()
    plt.show()

def conseguir_summary_tipo_consulta6(conexion:Connection, collection_name:Collection, tipo:str) -> str:
    """

    Obtener un texto con todas las palabras de longitud mayor de 3 puestas en una review el campo de summary de una categoría 
    de producto específica.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        collection_name (Collection): Objeto de la colección dentro de la base de datos.
        tipo (str): categoría elegida por el usuario.
    
    Returns:
        texto(str): texto que separa por espacios " " las palabras de longitud mayor de 3.

    """
    
    # Query para filtrar por tipo
    sql ="""
        SELECT id_review
        FROM productos p
        INNER JOIN review r ON r.id_producto = p.id_producto
        INNER JOIN tipos_producto tp ON p.tipo_producto = tp.tipo_producto
        WHERE tp.nombre_tipo_producto = %s;
        """
    result_sql = ejecutar_consulta_sql(conexion=conexion, sql=sql, args=[tipo])

    # Lista con todos los ids que han hecho una review en esa categoría
    ids = [id_prod[0] for id_prod in result_sql]  

    # Buscamos en mongo los summary de reviews que estén en la lista de ids de reviews de la categoría deseada
    result_mongo = collection_name.find({"_id": {"$in": ids}},{"_id": 0, "summary": 1})

    palabras_filtradas = []
    # Bucle por cada review que haya encontrado
    for diccionario_summary in result_mongo:
        # Guardamos el campo de summary
        summary = diccionario_summary["summary"]
        # Comprobamos que tenga ya que es posible que no tenga
        if summary:
            # Bucle para por cada palabra que haya en nuestrp summary, filtrarla para que su longitud sea mayor de 3
            for palabra in summary.split():
                if len(palabra) > 3:
                    palabras_filtradas.append(palabra)
        
    
    # Unimos todas las palabras en un único string, ya que es lo que nos pide la librería para crear la nube de palabras
    texto = " ".join(palabras_filtradas)
            
    return texto

############################################################################################################################################

# CONSULTA 7
def consulta7_obtener_media_review_text_por_overall(conexion:Connection, collection_name:Collection, tipo:str)-> None:
    """

    Mostrar un pie chart que va a mostrar de una categoría elegida la media de characters que suelen tener las reviews en el 
    campo de reviewText en función del overall que le han dado.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        collection_name (Collection): Objeto de la colección dentro de la base de datos.
        tipo (str): categoría elegida por el usuario.

    Returns:
        None. Solo hace el plot.

    """
    dict_final_ordenado = conseguir_medias_texto_consulta7(conexion,collection_name,tipo)
    
    overalls,cantidad_caracteres = [], []
    for overall in dict_final_ordenado:
        overalls.append(overall)
        cantidad_caracteres.append(dict_final_ordenado[overall])
    
    # Etiquetas (sin porcentaje)
    labels = [f"Overall: {o}" for o in overalls]

    # Función para mostrar solo la cantidad de caracteres
    def etiquetar(pct: float, all_vals: list) -> str:
        """
        Calcula el valor absoluto de un porcentaje con base en una lista de valores.

        Args:
            pct (float): Porcentaje de un segmento
            all_vals (list): Lista de valores originales para calcular la suma total

        Returns:
            (str): String con el valor absoluto calculado y la palabra "caracteres"
        """
        valor_absoluto = int(round(pct / 100. * sum(all_vals)))
        return f"{valor_absoluto} caracteres"

    # Crear gráfico
    plt.figure(figsize=(7, 7))
    plt.pie(
        cantidad_caracteres,
        labels=labels,
        autopct=lambda pct: etiquetar(pct, cantidad_caracteres),
        startangle=90,
        textprops=dict(color="black")
    )

    plt.title(f"Media de caracteres según overall de {tipo}")
    plt.axis('equal')  # mantener forma circular
    plt.tight_layout()
    plt.show()

def conseguir_medias_texto_consulta7(conexion:Connection, collection_name:Collection, tipo:str) -> dict:
    """

    Conseguiremos un diccionario que tiene por cada overall la media de characters en el campo de reviewText, especificada una categoría de producto.

    Args:
        conexion(pymysql.connections.Connection): Conexión a la base de datos MySQL.
        collection_name(Collection): nombre de la colección.
        tipo (str): categoría elegida por el usuario.
    
    Returns:
        dict_overalls(dict): diccionario que relaciona overall con media de characters en el campo de reviewText.

    """
    
    # Query para ver cuantos overalls hay, en caso de extenderse en el futuro
    sql1 ="""
        SELECT overall
        FROM review
        GROUP BY overall
        ORDER BY overall;
        """
    result_sql1 = ejecutar_consulta_sql(conexion=conexion, sql=sql1)

    # Guardamos los overall en una lista
    overalls = [overall[0] for overall in result_sql1]  # Lista de todos los IDs

    dict_overalls = {}
    # Iteramos por cada overall
    for overall in overalls:
        # Query para obtener las id_reviews, dependiendo el overall y la categoria que nos hayan dicho
        sql2 ="""
        SELECT r.id_review
        FROM review r
        INNER JOIN productos p ON p.id_producto = r.id_producto
        INNER JOIN tipos_producto pr ON pr.tipo_producto = p.tipo_producto
        WHERE r.overall = %s AND pr.nombre_tipo_producto = %s;
        """
        result_sql2 = ejecutar_consulta_sql(conexion=conexion, sql=sql2,args=[overall,tipo])

        # Guardamos los ids en una lista
        ids_overall = [id_prod[0] for id_prod in result_sql2]

        # Hacemos una consulta a MongoDB obteniendo el reviewText de los ids que estén en nuestra lista
        result_mongo = collection_name.find({"_id": {"$in": ids_overall}},{"_id": 0, "reviewText": 1})
        # Contador para ver cuantos resultados tengo (para luego hacer la media)
        cont = 0
        for dicc_review_text in result_mongo:
            # Cogemos el campo de reviewText
            review_text = dicc_review_text["reviewText"]
            cont += 1
            # Aquí comprobamos si ya está iniciado ese overall y con un valor asignado
            if overall in dict_overalls:
                # Si ya está creado sumamos la longitud del review_text
                dict_overalls[overall] += len(review_text)
            else:
                # Si no está creado lo cremos, inicializandolo a la longitud del review_text
                dict_overalls[overall] = len(review_text)

        # Como hemos estado sumando el total de cada texto, luego lo dividimos entre el total de reviews para tener la media
        dict_overalls[overall]  = dict_overalls[overall] / cont

    return dict_overalls
        
############################################################################################################################################

# FUNCIONES GRÁFICAS Y DE INTERFAZ 
def limpiar_pantalla()-> None:
    """

    Limpia la terminal para aportar mejor estética.

    Args:
        None.

    Returns:
        None

    """
    os.system("cls" if os.name == "nt" else "clear")

def menu_opciones()-> str:
    """
    Mostrar menu de opciones de los gráfico a visualizar, permite al usuario seleccionar que gráfico quiere visualizar.

    Args:
        None.
    Returns:
        str: número de la opción elegida

    """

    print("\n" + Fore.CYAN + Style.BRIGHT + "=" * 50)
    print(Fore.YELLOW + Style.BRIGHT + "        MENÚ DE VISUALIZACIÓN DE REVIEWS")
    print(Fore.CYAN + Style.BRIGHT + "=" * 50)

    print(Fore.GREEN + " 1 ▶  " + Fore.WHITE + "Mostrar evolución de reviews por años")
    print(Fore.GREEN + " 2 ▶  " + Fore.WHITE + "Evolución de la popularidad de los artículos")
    print(Fore.GREEN + " 3 ▶  " + Fore.WHITE + "Histograma por nota (tipo o artículo individual)")
    print(Fore.GREEN + " 4 ▶  " + Fore.WHITE + "Evolución de las reviews a lo largo del tiempo")
    print(Fore.GREEN + " 5 ▶  " + Fore.WHITE + "Histograma de reviews por usuario")
    print(Fore.GREEN + " 6 ▶  " + Fore.WHITE + "Nube de palabras por categoría")
    print(Fore.GREEN + " 7 ▶  " + Fore.WHITE + "Visualización adicional - Media characters por overall y categoría")
    print(Fore.RED + " 8 ▶  " + Fore.WHITE + "Salir del programa ❌")

    print(Fore.CYAN + Style.BRIGHT + "=" * 50)

    opcion = input(Fore.GREEN + Style.BRIGHT + "\n¿Qué opción quieres? " + Fore.RESET)

    return opcion

def menu_opciones_categoria(dict_opciones_categoria:dict, todo=None)-> str:
    """

    Mostrar menu de categorias disponibles a analizar dentro de un gráfico.

    Args:
        dict_opciones_categoria (dict): contiene todas las categorias disponibles.

    Returns:
        opcion(str): número de la opción elegida.

    """
    print("\n" + Fore.CYAN + Style.BRIGHT + "=" * 50)
    print(Fore.YELLOW + Style.BRIGHT + "              MENÚ ELEGIR CATEGORÍA")
    print(Fore.CYAN + Style.BRIGHT + "="  * 50)
    
    if todo is None:
        
        for categoria in dict_opciones_categoria:
            print(Fore.GREEN + Style.BRIGHT + f" {categoria} ▶  " + Fore.WHITE + f"{dict_opciones_categoria[categoria]}")

        print(Fore.CYAN + Style.BRIGHT + "="  * 50)

        opcion = input(Fore.YELLOW + Style.BRIGHT + "\n¿Qué opción quieres? " + Fore.RESET)

        while opcion not in dict_opciones_categoria:
            print(Fore.RED + Style.BRIGHT + "\n❌ OPCIÓN NO VÁLIDA ❌")
            time.sleep(0.5)
            opcion = input(Fore.YELLOW + Style.BRIGHT + "\n¿Qué opción quieres? " + Fore.RESET)

        return opcion

    else:
        for categoria in dict_opciones_categoria:
            if dict_opciones_categoria[categoria] != "Todos":
                print(Fore.GREEN + Style.BRIGHT + f" {categoria} ▶  " + Fore.WHITE + Style.BRIGHT + f"{dict_opciones_categoria[categoria]}")

        print(Fore.CYAN + Style.BRIGHT + "=" * 50)

        opcion = input(Fore.YELLOW + Style.BRIGHT + "\n¿Qué opción quieres? " + Fore.RESET)

        while opcion not in [str(i+1) for i in range(len(dict_opciones_categoria)-1)]:
            print(Fore.RED + Style.BRIGHT + "\n❌ OPCIÓN NO VÁLIDA ❌")
            time.sleep(0.5)
            opcion = input(Fore.YELLOW + Style.BRIGHT + "\n¿Qué opción quieres? " + Fore.RESET)

        return opcion

def menu_opciones_consulta3()-> str:
    """

    Menu para la consulta 3 permitiendo elegir si prefiere filtrar por una categoría 
    o si quiere buscar por un artículo individual.

    Args:
        None.

    Returns:
        str: número de la opción elegida.

    """
    # Titulo
    print("\n" + Fore.CYAN + Style.BRIGHT + "=" * 50)
    print(Fore.YELLOW + Style.BRIGHT + "           MENÚ: ¿QUÉ QUIERES BUSCAR?")
    print(Fore.CYAN + Style.BRIGHT + "=" * 50)
    # Opciones
    print(Fore.GREEN + " 1 ▶  " + Fore.WHITE + "Buscar por categoría             📂")
    print(Fore.GREEN + " 2 ▶  " + Fore.WHITE + "Buscar por artículo individual   🔍")

    print(Fore.CYAN + Style.BRIGHT + "=" * 50)

    opcion = input(Fore.YELLOW + Style.BRIGHT + "\n¿Qué opción quieres? " + Fore.RESET)
    # En caso de elegir una opción que no es posible.
    while opcion not in ["1", "2"]:
        print(Fore.RED + Style.BRIGHT + "\n❌ OPCIÓN NO VÁLIDA ❌")
        time.sleep(0.5)
        opcion = input(Fore.YELLOW + Style.BRIGHT + "¿Qué opción quieres? " + Fore.RESET)

    return opcion

def conseguir_todos_asin(conexion_mysql:Connection)-> list:
    """

    Nos va a conseguir todos los asin disponibles en nuestra base de datos.

    Args:
        conexion_mysql (): conexión propia a la base de datos deseada.

    Returns:
        asins(list): lista con todos los asins disponibles.

    """
    # Lista de asins
    asins = []

    # Consulta para conseguir todos los asins posibles
    sql ="""
        SELECT DISTINCT(asin)
        FROM productos;
        """
        
    result_sql = ejecutar_consulta_sql(conexion=conexion_mysql,sql=sql)

    # Guardamos los asins para así luego ver si el que nos ha introducido existe o no en nuestra DataBase
    for res in result_sql:
        asins.append(res[0])
    
    return asins

def menu_elegir_producto_consulta3(conexion_mysql:Connection)-> str:
    """

    Nos va a permitir seleccionar en la consulta 3 por que artículo queremos buscar, en caso de no estar disponible 
    se le notificara al usuario.

    Args:
        conexion_mysql (): conexion propia a la base de datos deseada.

    Returns:
        producto(str): asin del producto seleccionado.

    """
    # Ver todos los asins que hay
    asins_list = conseguir_todos_asin(conexion_mysql)
    # Título
    print("\n" + Fore.CYAN + Style.BRIGHT + "=" * 50)
    print(Fore.YELLOW + Style.BRIGHT + "           MENÚ ELEGIR PRODUCTO")
    print(Fore.CYAN  + Style.BRIGHT +"=" * 50)
    # Conseguir el producto
    producto = input(Fore.GREEN + Style.BRIGHT + "\n¿Qué producto quieres (ASIN)? " + Fore.RESET).upper()

    # Comprobar que el asin / producto que nos ha dicho existe o no
    while producto not in asins_list:
        print(Fore.RED + Style.BRIGHT + "\n❌ ¡Producto no encontrado, vuelve a intentar! ❌")
        time.sleep(1.5)
        producto = input(Fore.GREEN + Style.BRIGHT + "\n¿Qué producto quieres (ASIN)? " + Fore.RESET).upper()

    return producto

def print_banner()-> None:
    """
    
    Muestra una pequeña interfaz a modo de banner. Aporta estilo y favorece la xperiencia de usuario.

    Args:
        None

    Returns:
        None
    
    """

    # Título
    os.system('cls' if os.name == 'nt' else 'clear')
    print(Fore.CYAN + Style.BRIGHT + "="*50)
    print(Fore.MAGENTA + Style.BRIGHT + "         PROYECTO DE BASES DE DATOS ")
    print(Fore.CYAN + Style.BRIGHT + "="*50 + "\n")

def animacion_puntitos(mensaje:str="Conectando a las bases de datos")-> None:
    """

    Función que forma parte de la interfaz del programa. Muestra un mensaje por terminalç

    Args:
        None.

    Returns:
        None
    
    """
    # Hacer la animación de que está cargando
    print(Fore.YELLOW + Style.BRIGHT + mensaje, end="", flush=True)
    for _ in range(3):
        time.sleep(0.2)
        print(Fore.YELLOW + Style.BRIGHT + ".", end="", flush=True)
    print(Fore.GREEN + Style.BRIGHT + " ¡Conexión establecida!\n")

def menu_carga_datos()-> None:
    """
    
    Función que llama al resto de funciones relacionadas con la interfaz de usuario.

    Args:
        None
    
    Returns:
        None
    
    """
    # Ejecutar todo el menu
    print_banner()
    animacion_puntitos("Conectando a MySQL")
    animacion_puntitos("Conectando a MongoDB")
    print(Fore.CYAN + Style.BRIGHT + "Todo listo para comenzar 😎\n")
    time.sleep(1.5)

############################################################################################################################################

# FUNCIÓN MAIN PARA EJECUTAR EL PROCESO COMPLETO
def main()-> None:
    """
    
    Función principal del script, que se encarga de ejecutar el proceso completo.

    Args:
        None.

    Returns:
        None
    
    """

    init(autoreset=True)

    # Conexión a las bases de datos
    conexion_mysql = pymysql.connect(host="localhost", user=USER_SQL, password=PASSWORD_SQL, database= NOMBRE_BASE_DATOS_SQL)
    dbname = get_database_mongo(NOMBRE_BASE_DATOS_MONGO_DB)
    collection_name = dbname[COLECCION_MONGODB]

    # Hilo para ir mostrando el menu mientras cargamos los datos
    hilo_animación_carga = Thread(target=menu_carga_datos)
    hilo_animación_carga.start()

    # En caso de que la conexión de SQL haya ido bien
    if conexion_mysql:

        # Booleano que controla el bucle principal
        encendido = True

        sql_tipos = """
                SELECT tipo_producto,nombre_tipo_producto
                FROM tipos_producto;
                """
        result_sql_tipos = ejecutar_consulta_sql(conexion=conexion_mysql,sql=sql_tipos)

        # Cargamos en un diccionario los números asociados a cada nombre de tipo de producto (interfaz, opciones para usuario, etc)
        dict_opciones_categoria = {}
        for elemento in result_sql_tipos:
            dict_opciones_categoria[str(elemento[0] + 1)] = elemento[1]

        dict_opciones_categoria[str(len(result_sql_tipos)+1)] = "Todos"
        
        # Esperar a que el hilo termine
        hilo_animación_carga.join()

        while encendido:
            limpiar_pantalla()

            # Pedimos al usuario que elija qué función desea ejecutar
            opcion = menu_opciones()

            ####################
            #    CONSULTA 1    #
            ####################
            if opcion == "1":
                limpiar_pantalla()
                opcion_categoria = menu_opciones_categoria(dict_opciones_categoria)
                consulta1_mostrar_evolucion_reviews_por_anio(conexion=conexion_mysql,tipo=dict_opciones_categoria[opcion_categoria])
            
            ####################
            #    CONSULTA 2    #
            ####################
            elif opcion == "2":
                limpiar_pantalla()
                opcion_categoria = menu_opciones_categoria(dict_opciones_categoria)
                consulta2_mostrar_evolucion_popularidad_articulos(conexion=conexion_mysql,tipo=dict_opciones_categoria[opcion_categoria])

            ####################
            #    CONSULTA 3    #
            ####################
            elif opcion == "3":
                limpiar_pantalla()

                # Dentro de la opción 3, hay varias subopciones
                opcion_menu_consulta3 = menu_opciones_consulta3()

                if opcion_menu_consulta3 == "1":
                    limpiar_pantalla()
                    opcion_categoria = menu_opciones_categoria(dict_opciones_categoria)
                    consulta3_mostrar_histograma_por_nota(conexion=conexion_mysql,tipo=dict_opciones_categoria[opcion_categoria],producto=None)
                
                elif opcion_menu_consulta3 == "2":
                    limpiar_pantalla()
                    producto = menu_elegir_producto_consulta3(conexion_mysql)
                    consulta3_mostrar_histograma_por_nota(conexion=conexion_mysql,tipo=None,producto=producto)

            ####################
            #    CONSULTA 4    #
            ####################   
            elif opcion == "4":
                limpiar_pantalla() 
                opcion_categoria = menu_opciones_categoria(dict_opciones_categoria)
                consulta4_mostrar_evolucion_reviews_tiempo(conexion=conexion_mysql,tipo=dict_opciones_categoria[opcion_categoria])

            ####################
            #    CONSULTA 5    #
            ####################
            elif opcion == "5":
                consulta5_mostrar_histograma_reviews_por_usuario(conexion=conexion_mysql)
            
            ####################
            #    CONSULTA 6    #
            ####################
            elif opcion == "6":
                limpiar_pantalla()
                opcion_categoria = menu_opciones_categoria(dict_opciones_categoria,"No")
                consulta6_generar_nube_palabras_por_categoria(conexion=conexion_mysql,collection_name=collection_name,tipo=dict_opciones_categoria[opcion_categoria])
            
            ####################
            #    CONSULTA 7    #
            ####################
            elif opcion == "7":
                limpiar_pantalla()
                opcion_categoria = menu_opciones_categoria(dict_opciones_categoria,"No")
                consulta7_obtener_media_review_text_por_overall(conexion=conexion_mysql,collection_name=collection_name,tipo=dict_opciones_categoria[opcion_categoria])
            
            ####################
            #    CONSULTA 8    #
            ####################
            elif opcion == "8":
                print("\nSaliendo del programa. ¡Hasta pronto!")
                time.sleep(1)
                limpiar_pantalla()

                # Salimos del bucle principal porque el usuario ha querido terminar el programa
                encendido = False

            else:
                # Controlamos que solo se metan opciones válidas
                print("\nOpción no válida. Por favor, selecciona una opción del 1 al 8.")
                time.sleep(1.2)

    else:
        hilo_animación_carga.join()
        
############################################################################################################################################

if __name__=="__main__":

    try:
        # Llamamos a la función principal del archivo para que desarrolle el proceso completo
        main()

    except Exception as e:
        # Controlamos posibles excepciones
        print(f"\n>>>>>>>>>>>>>>>> ERROR: {e}")
    





