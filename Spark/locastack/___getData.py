import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test',  )

bucket_name = 'my-local-bucket'
object_key = 'restaurantes.json'

response = s3.get_object(Bucket=bucket_name, Key=object_key)        
data = response['Body'].read()

print(f"File '{object_key}' downloaded from s3://{bucket_name}/ whose values is {data}")
