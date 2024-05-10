import boto3

s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',)
bucket_name = 'my-local-bucket'         # Define the bucket name
s3.create_bucket(Bucket=bucket_name)    # Create the bucket

print(f"Bucket '{bucket_name}' created successfully.")


# Create an S3 client instance
# Crear un bucket por comando    ---> awslocal s3api create-bucket --bucket my-local-bucket
# Listar los archivos del bucket ---> awslocal s3 ls s3://my-local-bucket