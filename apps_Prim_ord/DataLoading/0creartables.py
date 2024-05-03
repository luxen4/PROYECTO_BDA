import psycopg2
from pyspark.sql import SparkSession
# Crear una tablas para responder a las preguntas de ANALISIS

def createTable_WClientes():
    #5.2.1 Análisis de las preferencias de los clientes
    #¿Cuáles son las preferencias alimenticias más comunes entre los clientes?

    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_clientes (
                id_cliente SERIAL PRIMARY KEY,
                fecha_entrada DATE,
                fecha_salida DATE,
                preferencias_alimenticias VARCHAR (100),
                id_restaurante INTEGER,
                restaurante_name VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'W_CLIENTES' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)
    





def insertarTable_wcliente(id_cliente, nombre, tipo_habitacion, preferencias_comida):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO wcliente (id_cliente, nombre, tipo_habitacion, preferencias_comida) VALUES (%s, %s, %s, %s);", 
                       (id_cliente, nombre, tipo_habitacion, preferencias_comida))
                
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla Cliente.")
     
     
     
def dataframe_wcliente():
    
    spark = SparkSession.builder \
    .appName("Leer y procesar con Spark") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", 'test') \
    .config("spark.hadoop.fs.s3a.secret.key", 'test') \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.jars","./postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()

    try:
        
        bucket_name = 'my-local-bucket' 
        file_name = 'data_clientes'
        df_clientes = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_clientes.show()
        
        bucket_name = 'my-local-bucket' 
        file_name='data_reservas'
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_reservas.show()
        
        
        df = df_clientes.join(df_reservas.select("id_cliente","fecha_entrada","fecha_salida","tipo_habitacion","preferencias_comida"), "id_cliente", "left")
        #df.show()

        bucket_name = 'my-local-bucket'
        file_name = 'data_hoteles.csv' # este es directo al S3
        df_hoteles = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_hoteles.show()
        
        
        df = df.join(df_hoteles.select("id_hotel","id_restaurante","nombre"), "id_hotel", "left")
        #df.show()
       
        bucket_name = 'my-local-bucket'
        file_name = 'data_restaurantes.csv' # este es directo al S3
        df_restaurantes = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
      
        df = df.join(df_restaurantes.select("id_hotel","id_restaurante","nombre"), "id_hotel", "left")
        #df.show()
        
        '''
        # Agrupar"
        df = df[[col for col in df.columns if col != "preferencias_alimenticias"]]
        df = df[[col for col in df.columns if col != "direccion"]]'''
        
        df = df.dropDuplicates()    # Eliminar registros duplicados

        # Mostrar el DataFrame resultante
        #df.show()
        '''
        # No tocar que es OK
        for row in df.select("*").collect():   
            print(row)
            id_cliente, nombre, tipo_habitacion, preferencias_comida = row
            print(f"id_cliente: {id_cliente},  Nombre: {nombre}, Tipo de Habitación: {tipo_habitacion}, Preferencias Comida: {preferencias_comida} ")
            
            insertarTable_wcliente(id_cliente, nombre, tipo_habitacion, preferencias_comida)
        '''
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)
















      
    
# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_wRestaurantes():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_restaurantes (
                id_restaurante SERIAL PRIMARY KEY,
                precio_menu DECIMAL(10,2),
                nombre_plato VARCHAR (100),
                alergenos VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'W_RESTAURANTES' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  



# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_wReservas():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_reservas (
                id_reserva SERIAL PRIMARY KEY,
                fecha_entrada DATE,
                fecha_salida DATE,
                nombre_hotel VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_reservas' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  


# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_wHoteles():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_reservas (
                id_hotel SERIAL PRIMARY KEY,
                empleados VARCHAR (100),
                categoria_habitacion (100),
                precio_habitacion DECIMAL(10,2)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_reservas' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  


dataframe_wcliente()
###
#    createTable_WClientes()
#    createTable_wRestaurantes()
#    createTable_wReservas()
#    createTable_wHoteles()
###



'''
5.2.1 Análisis de las preferencias de los clientes
¿Cuáles son las preferencias alimenticias más comunes entre los clientes?
5.2.2 Análisis del rendimiento del restaurante:
¿Qué restaurante tiene el precio medio de menú más alto?
¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?
5.2.3 Patrones de reserva
¿Cuál es la duración media de la estancia de los clientes de un hotel?
¿Existen periodos de máxima ocupación en función de las fechas de reserva?
5.2.4 Gestión de empleados
¿Cuántos empleados tiene de media cada hotel?
5.2.5 Ocupación e ingresos del hotel
¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de
habitación?
¿Podemos estimar los ingresos generados por cada hotel basándonos en los
precios de las habitaciones y los índices de ocupación?
5.2.6 Análisis de menús
¿Qué platos son los más y los menos populares entre los restaurantes?
23/24 - IABD - Big Data Aplicado
¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los
platos?
5.2.7 Comportamiento de los clientes
¿Existen pautas en las preferencias de los clientes en función de la época del año?
¿Los clientes con preferencias dietéticas específicas tienden a reservar en
restaurantes concretos?
5.2.8 Garantía de calidad
¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas
reales realizadas?
5.2.9 Análisis de mercado
¿Cómo se comparan los precios de las habitaciones de los distintos hoteles y
existen valores atípicos?'''


