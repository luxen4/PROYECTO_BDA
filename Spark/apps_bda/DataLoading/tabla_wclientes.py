import psycopg2
from pyspark.sql import SparkSession
# Crear una tablas para responder a las preguntas de ANALISIS

def createTable_WClientes():
    #5.2.1 Análisis de las preferencias de los clientes
    #¿Cuáles son las preferencias alimenticias más comunes entre los clientes?

    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_clientes (
                id_registro SERIAL PRIMARY KEY,
                nombre_cliente VARCHAR (100),
                fecha_llegada DATE,
                fecha_salida DATE,
                preferencias_comida VARCHAR (100),
                nombre_hotel VARCHAR (100)
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
    

def insertarTable_wcliente( nombre_cliente, fecha_llegada, fecha_salida, preferencias_comida, nombre_hotel):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_clientes (nombre_cliente, fecha_llegada, fecha_salida, preferencias_comida, nombre_hotel) VALUES ( %s, %s, %s, %s, %s);", 
                       ( nombre_cliente, fecha_llegada, fecha_salida, preferencias_comida, nombre_hotel))
    
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
        '''
        bucket_name = 'my-local-bucket' 
        file_name = 'data_clientes'
        df_clientes= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        df_clientes.show()
        '''
        
        bucket_name = 'my-local-bucket' 
        file_name='data_reservas'
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_reservas.show()
        
        
        #df = df_clientes.join(df_reservas.select("id_cliente","id_restaurante","fecha_llegada","fecha_salida","tipo_habitacion","preferencias_comida"), "id_cliente", "left")
        #df.show()
        
        
        bucket_name = 'my-local-bucket'
        file_name = 'restaurantes.json' 
        df_restaurantes= spark.read.json(f"s3a://{bucket_name}/{file_name}")
        #df_restaurantes.show()
        df = df.join(df_restaurantes.select("id_restaurante","id_hotel"), "id_restaurante", "left")
        
        
        
        bucket_name = 'my-local-bucket'
        file_name = 'data_hoteles.json' # este es directo al S3
        #df_hoteles = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_hoteles= spark.read.json(f"s3a://{bucket_name}/{file_name}")
        #df_hoteles.show()
        df = df.join(df_hoteles.select("id_hotel","nombre_hotel"), "id_hotel", "left")
        #df.show()
       
      
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "id_restaurante"]]
        df = df[[col for col in df.columns if col != "direccion"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
        df = df[[col for col in df.columns if col != "id_hotel"]]
        # Mostrar el DataFrame resultante
        df.show()
        
        # df = df.dropDuplicates()    # Eliminar registros duplicados

       
        
        # No tocar que es OK
        for row in df.select("*").collect():
            nombre_cliente=row["nombre"],
            fecha_llegada=row["fecha_llegada"]
            fecha_salida=row["fecha_salida"]
            tipo_habitacion=row["tipo_habitacion"]
            preferencias_comida = row["preferencias_comida"]
            nombre_hotel= row["nombre_hotel"]
            
            print(f"""
                  Nombre-Cliente: {nombre_cliente}, 
                  Fecha-LLegada: {fecha_llegada},
                  Fecha-Salida: {fecha_salida},
                  Tipo-Habitación: {tipo_habitacion},
                  Preferencias-Comida: {preferencias_comida},
                  Nombre_Hotel: {nombre_hotel}
                  """)
            
            insertarTable_wcliente( nombre_cliente, fecha_llegada, fecha_salida, preferencias_comida, nombre_hotel)
       
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)


###
createTable_WClientes()
dataframe_wcliente()
###




