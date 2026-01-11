"""
Este script se empleará para plasmar la idea y ejecución del modelo de machine learning que hemos implementado a raíz de los datos 
que ya teníamos almacenados en nuestras distintas bases de datos. El modelo se emplea para poder hacer recomendaciones a cualquier 
usuario, sobre cuáles son los productos que seguramente más le puede interesar. Esto se hace analizando los usuarios que tienen gustos 
similares a los del usuario en cuestión, teniendo en cuenta también si las valoraciones de los productos que tienen en ambos en común son 
parecidas. De esta forma, podemos sugerir a los usuarios productos nuevos que aún quizá no conocen pero les pueden llegar a interesar.

En caso de cualquier error, se recogen las excepciones para que el programa no termine de forma abrupta en ningún caso, favoreciendo así
la experiencia del usuario.

Autores: Jorge Carnicero Príncipe y Andrés Gil Vicente
Grupo: 2º A IMAT
Proyecto: Proyecto Final 2024/2025 - Bases de Datos

"""

############################################################################################################################################

# Importamos las librerías necesarias
from configuracion import*
import pymysql
import numpy as np
from colorama import init, Fore, Style
from pymysql.connections import Connection
from colorama import Fore, Style, init
import time
import os

############################################################################################################################################

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

############################################################################################################################################

# CÁLCULOS Y EXTRACCIONES
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

    if denominador_1 == 0 or denominador_2 == 0:
        return 0

    PC = numerador / (denominador_1 * denominador_2)

    return PC

def calculo_interseccion_productos_reviewed(conexion:Connection, id_persona_1:int, id_persona_2:int)-> list:
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

def conseguir_usuarios_comun(conexion:Connection, usuario:int)-> tuple:
    """

    Obtiene los identificadores de usuarios que han interactuado con los mismos productos
    que un usuario dado. Se basa en encontrar coincidencias en las reseñas de productos.

    Args:
        conexion (pymysql.connections.Connection): Conexión activa a la base de datos MySQL.
        usuario: ID del usuario del que se quiere conocer con quién comparte productos reseñados.

    Returns:
        Tuple: Una tupla con los IDs de las personas que han reseñado al menos un producto en común
        con el usuario proporcionado. El usuario original también puede aparecer en el resultado.

    """
    
    sql = """
        SELECT DISTINCT r2.id_persona
        FROM review r1
        INNER JOIN review r2 ON r1.id_producto = r2.id_producto
        WHERE r1.id_persona = %s;
        """
    
    result_sql = ejecutar_consulta_sql(conexion, sql,args=[usuario])

    return result_sql

def conseguir_articulos_no_valorados_por_objetivo(conexion:Connection, id_usuario_similar:str, id_usuario_objetivo:str)->tuple:
    """

    Obtiene los artículos que han sido valorados por un usuario "similar" pero que
    aún no han sido valorados por el usuario objetivo.

    Esta función se puede usar como parte de un sistema de recomendación basado en
    filtrado colaborativo, sugiriendo artículos que podrían interesarle al usuario objetivo.

    Args:
        conexion (pymysql.connections.Connection): Conexión activa a la base de datos MySQL.
        id_usuario_similar (str): ID del usuario similar, es decir, alguien con gustos parecidos.
        id_usuario_objetivo (str): ID del usuario al que se le quieren recomendar artículos.

    Returns:
        Tuple: Tupla con los IDs de productos que podrían recomendarse, ya que el usuario objetivo no los ha valorado aún.

    """
    
    
    sql = """
        SELECT DISTINCT r.id_producto
        FROM review r
        WHERE r.id_persona = %s AND r.id_producto NOT IN (SELECT id_producto
                                                         FROM review
                                                         WHERE id_persona = %s
                                                         );
        """

    result_sql = ejecutar_consulta_sql(conexion, sql,args=[id_usuario_similar,id_usuario_objetivo])

    return result_sql

############################################################################################################################################

# MENÚS
def pedir_usuario_valido(id_max:int)-> int:
    """
    Pedir a que usuario queremos buscar la mejor recomendación.

    Args:
        id_max (int): máximo id disponible

    Returns:
        int or "": ID del usuario válido seleccionado o "" si se desea salir.
    """
    print()
    print(f"{Fore.CYAN}{Style.BRIGHT}╔════════════════════════════════════════════════════╗")
    print(f"{Fore.CYAN}{Style.BRIGHT}║      SISTEMA ML DE RECOMENDACIÓN DE PRODUCTOS      ║")
    print(f"{Fore.CYAN}{Style.BRIGHT}╚════════════════════════════════════════════════════╝")
    print(f"{Fore.MAGENTA}{Style.BRIGHT}➡️  Selecciona un ID de usuario para generar recomendaciones.")
    print(f"{Fore.RED}{Style.BRIGHT}⚠️  Rango válido → {Style.BRIGHT}0 a {id_max - 1}")
    print(f"{Fore.WHITE}{Style.BRIGHT}🛑 Presiona ENTER para salir.\n")
    print()

    while True:
        entrada = input(f"{Fore.YELLOW}{Style.BRIGHT}🧠 Introduce el ID de usuario: {Style.RESET_ALL}")

        if entrada.strip() == "":
            print(f"{Fore.WHITE}{Style.BRIGHT}👋 Has salido del sistema de recomendaciones, ¡hasta la próxima!.\n")            
            return "" 
        
        try:
            usuario = int(entrada)
            if 0 <= usuario < id_max:
                print(f"\n{Fore.GREEN}{Style.BRIGHT}✅ ID válido seleccionado: {usuario}\n")
                return usuario
            else:
                print(f"{Fore.RED}{Style.BRIGHT}❌ El ID debe estar entre 0 y {id_max - 1}\n")
        except ValueError:
            print(f"{Fore.RED}{Style.BRIGHT}❌ Entrada inválida. Por favor, introduce un número entero.\n")
        
        time.sleep(0.3)  # suaviza el bucle un poco, pequeño delay para que se vea más fluido

def mostrar_recomendaciones(usuario_a_recomendar:int, articulos_ordenados:dict)-> None:
    """
    Menu para mostrar los mejores artículos para recomendar, con cuantas personas con coeficientes similares lo han calificado 
    y la media de las calificaciones de esas personas.

    Args:
        usuario_a_recomendar (int): id del usuario que queremos recomendar
        articulos_ordenados (dict): diccionario con el id del producto y la cantidad de personas y la media que le han dado en conjunto

    Returns:
        None. Solo se muestra por pantalla información.

    """
    print()
    print(f"{Fore.CYAN}{Style.BRIGHT}🌟 Recomendaciones para el usuario [ID: {usuario_a_recomendar}] 🌟")
    print(f"{Style.DIM}{'─'*65}")

    for i, (id_producto, datos) in enumerate(articulos_ordenados.items()):
        cantidad, media = datos

        espacios = " "*(5-len(str(id_producto)))

        print(f"{Fore.YELLOW}{i+1}️⃣ {espacios} {Fore.WHITE}Producto ID: {Fore.LIGHTGREEN_EX}{id_producto}   "
              f"{Fore.WHITE}| ⭐ Media: {Fore.LIGHTBLUE_EX}{media:.2f}   "
              f"{Fore.WHITE}| 👥 Votos: {Fore.LIGHTMAGENTA_EX}{cantidad}")

        if i == 4: 
            break
    print(f"{Style.DIM}{'─'*65}\n")

############################################################################################################################################

# LIMPIAR PANTALLA
def limpiar_pantalla()-> None:
    """

    Limpia la terminal para aportar mejor estética.

    Args:
        None.

    Returns:
        None

    """
    os.system("cls" if os.name == "nt" else "clear")

############################################################################################################################################

# MAIN
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
    init(autoreset=True)

    conexion_mysql = conectar_mysql()
    sql_tam_max = """
        SELECT COUNT(id_persona)
        FROM personas;
        """
    id_max = ejecutar_consulta_sql(conexion=conexion_mysql,sql=sql_tam_max)[0][0]

    # Booleano que controla el bucle principal del programa
    jugando = True

    # Nada más empezar limpiamos la terminal
    limpiar_pantalla()

    while jugando:
    
        # Mostramos el menú de inicialización del programa
        usuario_a_recomendar = pedir_usuario_valido(id_max)

        # Comprobamos que el usuario no quiera salir
        if usuario_a_recomendar != "":
        
            usuarios_correlacion_parecida = []
            
            # Conseguir todos los usuarios
            result_sql_usuarios = conseguir_usuarios_comun(conexion=conexion_mysql,usuario=usuario_a_recomendar)

            # Conseguir la media del usuario1
            media_user_1 = conseguir_media_overall_por_id(conexion=conexion_mysql, id_usuario=usuario_a_recomendar)

            
            for usuario in result_sql_usuarios:
                # Cogemos el id del segundo usuario
                id_usuario2 = usuario[0] 
                # Extraemos la media de overall de las reviews del usuario 2
                media_user_2 = conseguir_media_overall_por_id(conexion=conexion_mysql, id_usuario=id_usuario2)
                # Buscamos los artículos sobre los que ambos usuarios han hecho reviews
                articulos_en_comun = calculo_interseccion_productos_reviewed(conexion=conexion_mysql, id_persona_1=usuario_a_recomendar, id_persona_2=id_usuario2)
                # Solo calculamos la similitud entre aquellos usuarios que tienen algún artículo en común entre sus reseñas
                if len(articulos_en_comun) > 0:
                    # Calculamos el Coeficiente de Pearson (similitud entre ambos usuarios)
                    PC = calculo_similitud_pearson(articulos_en_comun, media_user_1, media_user_2)
                    if PC > 0.85:
                        usuarios_correlacion_parecida.append(id_usuario2)
            
            # AQUI YA TENDRÍAMOS A TODOS LOS USUARIOS CON UNA CORRELACIÓN ALTA / PIENSAN IGUAL QUE NUESTRO OBJETIVO
            articulos_a_recomendar_media = {}
            for usuario_similar in usuarios_correlacion_parecida:
                articulos_valorados_result_sql = conseguir_articulos_no_valorados_por_objetivo(conexion=conexion_mysql,id_usuario_similar=usuario_similar,id_usuario_objetivo=usuario_a_recomendar)
                for articulo in articulos_valorados_result_sql:
                    if articulo[0] not in articulos_a_recomendar_media:
                        sql = """
                        SELECT avg(overall)
                        from review
                        where id_persona in %s and id_producto = %s
                        group by id_producto;
                        """
                        result_sql_media = ejecutar_consulta_sql(conexion=conexion_mysql, sql=sql,args=[tuple(usuarios_correlacion_parecida),articulo[0]])
                        articulos_a_recomendar_media[articulo[0]] = [1,result_sql_media[0][0]]
                    
                    # si vuelve a aparecer podemos hacerlo para ver cuanta gente a valorado a ese producto recomendado, usar como criterio tambien
                    else:
                        articulos_a_recomendar_media[articulo[0]][0] += 1

            # print(articulos_a_recomendar_media)
            articulos_a_recomendar_media_ordenado = dict(sorted(articulos_a_recomendar_media.items(),key=lambda x: (x[1][1], x[1][0]),reverse=True))

            if articulos_a_recomendar_media_ordenado:
                limpiar_pantalla()
                mostrar_recomendaciones(usuario_a_recomendar, articulos_a_recomendar_media_ordenado)
            else:
                print(f"{Fore.WHITE}{Style.BRIGHT}ℹ️  No se encontraron artículos para recomendar.")

        else:
            # Esto significa que el usuario ha decidido salir del programa
            jugando = False

############################################################################################################################################

if __name__=="__main__":

    try:
        # Llamamos a la función principal del archivo para que desarrolle el proceso completo
        main()
        
    except Exception as e:
        # Controlamos posibles excepciones
        print(f"\n>>>>>>>>>>>>>>>> ERROR: {e}")


