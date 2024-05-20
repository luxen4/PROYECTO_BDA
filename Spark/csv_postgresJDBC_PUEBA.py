from pyspark.sql import SparkSession

# 1º Crear las tablas de cada uno
import psycopg2

# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_hoteles():
    try:
        connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
   
        create_table_query = """
            CREATE TABLE IF NOT EXISTS hoteles (
                id_hotel SERIAL PRIMARY KEY,
                nombre_hotel VARCHAR (100),
                direccion_hotel VARCHAR (100),
                empleados VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'HOTELES' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)



def prueba():

    # Configura la sesión de Spark
    spark = SparkSession.builder \
        .appName("InsertIntoPostgres") \
        .config("spark.driver.extraClassPath", "postgresql-42.7.3.jar") \
        .master("local[*]") \
        .getOrCreate()
    
    # leer el csv con spark
    # df = spark.read.csv("./../../spark-data/csv/hoteles.csv", header=True)
    df = spark.read.option("header", "true").option("multiLine", "true").csv("./../../spark-data/csv/hoteles.csv")
    
    # leer desde json con spark
    
    # leer desde txt con spark
    
    
    
    # columns = ['id_hotel', 'nombre_hotel', 'direccion_hotel', 'empleados']
    # df = spark.createDataFrame(data, columns)
    df.show()

    # Escribe los datos en la tabla PostgreSQL
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres","password": "casa1234","driver": "org.postgresql.Driver"
    }
    table_name = "hoteles"

    df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
    spark.stop()
    
    
createTable_hoteles()
prueba()








# from pyspark.sql.functions import to_date
# Convierte la columna 'date' a tipo de dato 'date'
# df = df.withColumn("date", to_date(df["date"]))