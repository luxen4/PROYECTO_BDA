from time import sleep                      
from json import dumps
from kafka import KafkaProducer

import pymongo
from pymongo import MongoClient     

#client = MongoClient()                                                     # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
# mongo_client = pymongo.MongoClient("mongodb://root:secret@localhost:27017/")  # clase
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["proyecto"]                                               # Accede a la base de datos
clients_collection = db["clients"]                                          # Accede a la colección 
resultados = clients_collection.find()


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
#producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

# Imprime los resultados
print("Clientes Mandados:")
for client in resultados:
    print(client)
    
    print("ID del documento:", client["_id"])
    lista_clientes = client["clients"]
    
    
    if lista_clientes is not None:
    
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
        
# Se leen los registros bajo consulta de Mongo
# Se prepara el mensaje del Producer
# Se manda el mensaje al Consumer