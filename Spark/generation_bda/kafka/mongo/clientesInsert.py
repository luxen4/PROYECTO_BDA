from pymongo import MongoClient
import json

client = MongoClient()                  # Conexión al servidor de MongoDB (por defecto, se conectará a localhost en el puerto 27017)

db = client["proyecto"]
clients_collection = db["clients"]      # Accede a la colección "clients"


# Leer un json
def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            print(data)
            return data
    except FileNotFoundError:
        return None

#filename='./../data_Prim_ord/json/cligentes.json'
filename='./../../../../data_bda/json/clientes.json'
clients = read_json_file(filename)

clients_collection.insert_one({"clients": clients}) # Inserta la lista de clients


print("Contenido de la colección 'clients':")       # Imprimir
for clients in clients_collection.find():
    print(clients)
    
### OK ###
    