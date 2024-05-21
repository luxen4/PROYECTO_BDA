
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Configura el cliente S3
s3 = boto3.client('s3',
                  aws_access_key_id='test',
                  aws_secret_access_key='test',
                  endpoint_url='http://localhost:4566')  # Cambia esto si no usas LocalStack
# Nombre del bucket y el archivo que deseas eliminar
bucket_name = 'my-local-bucket'
file_key = 'plato_csv/'

try:
    # Elimina el archivo
    s3.delete_object(Bucket=bucket_name, Key=file_key)
    print(f'Archivo {file_key} eliminado exitosamente del bucket {bucket_name}.')

except NoCredentialsError:
    print("Credenciales no encontradas")
except PartialCredentialsError:
    print("Credenciales incompletas")
except Exception as e:
    print(f"Error al eliminar el archivo: {str(e)}")