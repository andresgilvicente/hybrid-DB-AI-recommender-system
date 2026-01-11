# Proyecto Final - Bases de Datos
# Fecha de entrega - 22 de abril de 2024

Este proyecto consiste en la implementación de una base de datos híbrida con MySQL y MongoDB para almacenar y
analizar reviews de productos de Amazon. Se complementa con visualizaciones, análisis en grafo con Neo4J, 
y un sistema de recomendación basado en similitudes entre usuarios.

---

## Autores

- Jorge Carnicero Príncipe
- Andrés Gil Vicente

---

## Estructura del proyecto

- **configuracion.py**: Contiene rutas y credenciales necesarias para las bases de datos.
- **load_data.py**: Carga los datos desde los archivos JSON especificados en configuracion.py a MySQL y MongoDB.
- **menu_visualizacion.py**: Visualización de los datos mediante gráficos y menús.
- **neo4JProyecto.py**: Modelado de relaciones en grafo entre usuarios y productos.
- **inserta_dataset.py**: Añade nuevos conjuntos de datos adicional al sistema, especificados en configuracion.py. 
- **machine_learning.py**: Contiene una propuesta de sistema de recomendación basado en similitudes entre usuarios.
- **requirements.txt**: Lista de dependencias del proyecto.
- **data/**: Carpeta donde deben ubicarse los archivos JSON con los datos.

---

## Requisitos

Se recomienda crear un entorno virtual antes de instalar las dependencias.

### Crear entorno virtual (opcional pero recomendado):

python -m venv entorno_proyecto_final

.\entorno_proyecto_final\Scripts\Activate.ps1  # En Windows
source entorno_proyecto_final/bin/activate   # En Linux/macOS


### Instalar librerias necesarias:

pip install -r requirements.txt

---

## Ejecución del proyecto

Ejecutar los siguientes scripts en orden:

1. **load_data.py** 
   Carga los datos desde los archivos JSON especificados en FICHEROS_DATOS_LOAD_DATA en configuracion.py a MySQL y MongoDB.

2. **menu_visualizacion.py**  
   Permite visualizar los datos con gráficas interactivas mediante diferentes consultas.

3. **neo4JProyecto.py**  
   Construye grafos en Neo4J con nodos relacionando usuarios, productos o categorias.

4. *(Opcional)* **inserta_dataset.py**  
   Podremos añadir varios archivos JSON nuevos a nuestras bases de datos.

5. *(Opcional)* **machine_learning.py**  
   Implementa una propuesta de sistema de recomendación basado en similitudes entre usuarios, a un usuario concreto.
---