import psycopg2
from pyspark.sql import SparkSession
# Crear una tablas para responder a las preguntas de ANALISIS

    
# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_wRestaurantes():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        '''
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_restaurantes (
                id_registro SERIAL PRIMARY KEY,
                
                id_reserva VARCHAR (100),
                
                restaurante_id INTEGER,
                restaurante_name VARCHAR (100),
                
                id_menu INTEGER,
                menu_name VARCHAR (100),
                menu_price DECIMAL(10,2),
                
                plato_id INTEGER,
                plato_name VARCHAR (100)
            );
        """
        '''
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_restaurantes (
                id_registro SERIAL PRIMARY KEY,
                id_reserva VARCHAR (100),
                
                restaurante_id INTEGER,
                restaurante_name VARCHAR (100),
                
                id_menu INTEGER,
                menu_name VARCHAR (100),
                menu_price DECIMAL(10,2),   
                
                plato_id INTEGER,
                plato_name VARCHAR (100),
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

def insertarTable_wrestaurantes(id_registro, id_reserva, restaurante_id, restaurante_name, id_menu, menu_name, menu_price, plato_id, plato_name, alergenos):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_restaurantes (id_registro, id_reserva, restaurante_id, restaurante_name, id_menu, menu_name, menu_price, plato_id, plato_name, alergenos) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", 
                       (id_registro, id_reserva, restaurante_id, restaurante_name, id_menu, menu_name, menu_price, plato_id, plato_name, alergenos))
    
    connection.commit() 
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla w_restaurantes.")
     
     
     
def dataframe_wrestaurantes():
    
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
        file_name = 'data_reservas' 
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_reservas.show()
        
        bucket_name = 'my-local-bucket' 
        file_name = 'restaurantes_data.json'
        df_restaurantes= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        #df_restaurantes.show()
        
        df = df_reservas.join(df_restaurantes.select("id_restaurante","nombre"), "id_restaurante", "left")
        df = df.withColumnRenamed("nombre", "restaurante_name")   # Cambiar el nombre de la columna
        #df.show()
        
        bucket_name = 'my-local-bucket' 
        file_name='data_menus'      
        df_menus = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_menus.show()
        
        df = df.join(df_menus.select("id_restaurante","id_menu","precio"), "id_restaurante", "left") #### METER UN NOMBRE DE MENU
        #df.show()
        
       
        bucket_name = 'my-local-bucket'
        file_name = 'data_relaciones'
        df_relaciones= spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True) 
        #df_relaciones.show()
        df = df.join(df_relaciones.select("id_menu","id_plato"), "id_menu", "left")
        #df.show()
        
        
        
        bucket_name = 'my-local-bucket' 
        file_name='data_platos'      
        df_platos = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_platos.show()
        
        df_platos = df_platos.withColumnRenamed("platoID", "plato_id")   # Cambiar el nombre de la columna
        
        df = df.join(df_platos.select("plato_id","nombre","ingredientes","alergenos"), "plato_id", "left") #### METER UN NOMBRE DE MENU
        df.show()
     
# https://stackoverflow.com/questions/65114334/pyspark-join-with-different-column-names-and-cant-be-hard-coded-before-runti      
# left_key = 'leftColname'
# right_key = 'rightColname'
# final = ta.join(tb, ta[left_key] == tb[right_key], how='left')

        
      
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "fecha_llegada"]]
        df = df[[col for col in df.columns if col != "fecha_salida"]]
        df = df[[col for col in df.columns if col != "tipo_habitacion"]]
        df = df[[col for col in df.columns if col != "preferencias_comida"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
        # Mostrar el DataFrame resultante
        #df.show()
        
        # df = df.dropDuplicates()    # Eliminar registros duplicados

       
        
        # No tocar que es OK
        for row in df.select("*").collect():
            print(row)
            id_reserva=row["id_reserva"],
            restaurante_id = row["restaurante_id"]
            restaurante_name=row["restaurante_name"]
            id_menu=row["id_menu"]
            menu_name=row["menu_name"]
            menu_price=row["precio"]
            plato_id = row["menu_price"]
            plato_name = row["plato_name"]
            alergenos = row["alergenos"]
            
            print(f"""
                  id_reserva-Cliente: {id_reserva}, 
                  restaurante_name: {restaurante_name},
                  id_menu: {id_menu},
                  menu_price: {menu_price}
                  """)
            
            insertarTable_wrestaurantes( id_reserva, restaurante_id, restaurante_name, id_menu, menu_name, menu_price, plato_id, plato_name, alergenos)
           
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)





###
# createTable_wRestaurantes()
dataframe_wrestaurantes()
###





'''
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
                menu_price_habitacion DECIMAL(10,2)
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
'''



'''
5.2.1 Análisis de las preferencias de los clientes
¿Cuáles son las preferencias alimenticias más comunes entre los clientes?
5.2.2 Análisis del rendimiento del restaurante:
¿Qué restaurante tiene el menu_price medio de menú más alto?
¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?
'''

