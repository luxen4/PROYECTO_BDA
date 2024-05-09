# Leer los datos de postgres y crear los csv

from pyspark.sql import SparkSession

# Crear una sesión de Spark
def sesionSpark():
          
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
        .config("spark.jars","./../postgresql-42.7.3.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .master("local[*]") \
        .getOrCreate()

    return spark
        
        
def leerPostgres():
    spark = sesionSpark()
        
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
    connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    df = spark.read.jdbc(url=jdbc_url, table="empleados", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")

    resultado = spark.sql("SELECT * FROM tabla_spark;")
    resultado.show()
    
    
    resultado \
    .write \
    .option('header', 'true') \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://my-local-bucket/data_empleados.csv', sep=',')


    spark.stop()

leerPostgres()

# resultado.write.csv("s3a://my-local-bucket/hoteles.csv", header=True, mode="overwrite")
# #resultado = spark.sql("SELECT * FROM tabla_spark WHERE store_name ='" + store_name + "';")













'''
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Definir el esquema para los datos
schema = StructType([
    StructField("id_hotel", IntegerType(), True),
    StructField("nombre_hotel", StringType(), True),
    StructField("direccion_hotel", StringType(), True),
    StructField("empleados", StringType(), True)
])'''
    
   
    


