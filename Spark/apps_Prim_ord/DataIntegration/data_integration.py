import sys
sys.path.append("..")
import sessions


# Función -> Sube directamente los archivos al bucket
if __name__ == "__main__":
    try:
        spark = sessions.sesionSpark()
        
        # csv
        #df = spark.read.csv("./../../spark-data/csv/habitaciones.csv")                                     # Ruta relativa
        df = spark.read.csv("/opt/spark-data/csv/habitaciones.csv")                                         # Ruta absoluta
        ruta_salida = "s3a://my-local-bucket/habitaciones_csv"
        df=df.write.csv(ruta_salida, mode="overwrite")
        
        # json
        #df = spark.read.option("multiline", "true").json("./../../spark-data/json/restaurantes.json")      
        df = spark.read.option("multiline", "true").json("/opt/spark-data/json/restaurantes.json")
        ruta_salida = "s3a://my-local-bucket/restaurantes_json"
        df.write.option("multiline", "true").json(ruta_salida, mode="overwrite")
        
        
        # Leer un archivo de S3
        bucket_name = 'my-local-bucket' 
        file_name='habitaciones_csv'
        df_original = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_original.show()
        
        # Leer un archivo de S3
        bucket_name = 'my-local-bucket'
        file_name='restaurantes_json'
        df_original = spark.read.json(f"s3a://{bucket_name}/{file_name}")
        df_original.show()
        

        spark.stop()

    except Exception as e:
        print("error reading TXT")
        print(e)
     














'''
# Crear la sesión de Spark
def sesionSpark():
    spark = SparkSession.builder \
    .appName("Leer y procesar con Spark") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", 'test') \
    .config("spark.hadoop.fs.s3a.secret.key", 'test') \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
    
    return spark
'''