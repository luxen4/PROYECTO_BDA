import boto3
from pyspark.sql import SparkSession

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Eliminar archivo de S3") \
    .getOrCreate()

# Configurar el cliente de S3 con boto3
s3 = boto3.client('s3', aws_access_key_id='TU_ACCESS_KEY', aws_secret_access_key='TU_SECRET_KEY')

# Especificar el bucket y el archivo a eliminar
bucket_name = 'my-local-bucket'
file_key = 'ruta/del/archivo/a/eliminar.txt'

# Eliminar el archivo
s3.delete_object(Bucket=bucket_name, Key=file_key)

print(f"Archivo {file_key} eliminado del bucket {bucket_name}.")

# Detener la sesión de Spark
spark.stop()