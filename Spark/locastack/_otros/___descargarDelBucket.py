import boto3

def descargar_archivo_desde_s3(bucket_name, nombre_archivo, ruta_local):
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',)

    try:
        # Descargar el archivo desde S3 al sistema local
        s3.download_file(bucket_name, nombre_archivo, ruta_local)
        print(f"Archivo descargado exitosamente a: {ruta_local}")
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")

# Ejemplo de uso
bucket_name = 'my-local-bucket'
nombre_archivo = 'clientes_json'
ruta_local = './archivo.json'


descargar_archivo_desde_s3(bucket_name, nombre_archivo, ruta_local)