from pymongo import MongoClient
import json
import os.path


client = MongoClient("mongodb://root:secret@localhost:27017/")                  # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)
db = client["proyecto"]                 # accede a la base de datos
clients_collection = db["clients"]      # Accede a la colección


# Leer un archivo json, devuelve una lista de diccionarios
def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print('No se ha encontrado el fichero: '+filename)
        return 'Fichero no existe'
    

filename= 'Spark/data_Prim_ord/json/clientes.json'
clients = read_json_file(filename)
print(clients)
clients_collection.insert_one({"clients": clients}) # Inserta la lista de clients


print("Contenido de la colección 'clients':")       # Imprimir
for clients in clients_collection.find():
    print(clients)


# Archivo indicado para insertar registros en Mongo
# print(os.path.isfile('Spark\data_Prim_ord\json\clientes.json'))
