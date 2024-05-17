#import sesionspark
from pyspark.sql import SparkSession

def leerConSpark():
    
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
    
    try:
        #df = spark.read.text("s3a://my-local-bucket/stores_data_procesado.csv/")
        #d f = spark.read.text("s3a://my-local-bucket/data_reservas")
        
        # csv
        bucket_name = 'my-local-bucket' 
        folder_name='gastos_csv'
        df= spark.read.csv(f"s3a://{bucket_name}/{folder_name}", header=True, inferSchema=True)
        
        '''
# Leer un Dataframe
def leerResultados(resultado):
    store_id = None
    if not resultado.isEmpty():
        row = resultado.collect()[0]
        store_id = row["store_id"]
        store_name = row["store_name"]
        location = row["location"]
        demographics = row["demographics"]
        
        print("store_id:", store_id)
        print("store_name:", store_name)
        print("location:", location)
        print("demographics:", demographics)
        print()
'''
        
        
        #json
        bucket_name = 'my-local-bucket' 
        folder_name='hoteles_json'
        #df= spark.read.json(f"s3a://{bucket_name}/{folder_name}")
        

        df.show()
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)

leerConSpark()


# Que los guarde en local, hace 4 archivos 
# df.write.csv("./habitaciones", header=True, mode="overwrite")
# df.write.json("./restaurantes", mode="overwrite")

