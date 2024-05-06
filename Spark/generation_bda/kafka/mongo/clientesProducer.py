# Mandar por kafka
from time import sleep                      
from json import dumps
from kafka import KafkaProducer

from pymongo import MongoClient     # Leer los registros de mongo

client = MongoClient()              # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)

db = client["proyecto"]
clients_collection = db["clients"]          # Accede a la colección "clients"
resultados = clients_collection.find()


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
# producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

# Imprime los resultados
print("Clientes Mandados:")

for client in resultados:
    print(client)
    
    print("ID del documento:", client["_id"])
    lista_clientes = client["clients"]
    
    for cliente in lista_clientes:
        id_cliente=cliente["id_cliente"] 
        nombre=cliente["nombre"]
        direccion=cliente["direccion"]
        preferencias_alimenticias = cliente["preferencias_alimenticias"]
    
        message = {
            "id_cliente": id_cliente,
            "nombre": nombre,
            "direccion": direccion,
            "preferencias_alimenticias": preferencias_alimenticias
        }
        print(message)
        producer.send('info', value=message)
    
        #sleep(1)