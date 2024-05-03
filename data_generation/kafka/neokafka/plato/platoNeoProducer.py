from time import sleep                      
from json import dumps
from kafka import KafkaProducer             # Con el botón derecho
from neo4j import GraphDatabase
import json, csv

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
# producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))


class Neo4jClient:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def get_all_Platos(self, session):
        result = session.run("MATCH (p:Platos) RETURN p")
        return result


if __name__ == "__main__":
                                                                                        # Uri, usuario ypassword
    neo4j_client = Neo4jClient("bolt://localhost:7687", "neo4j", "your_password")       # Crear instancia de Neo4jClient
    neo4j_client.connect()
   
    with neo4j_client._driver.session() as session:         # Crear una sesión de Neo4j
        platos = neo4j_client.get_all_Platos(session)       # Consultar todos los platos

        # Imprimir los Platos
        print("Platos en la base de datos:")
        for record in platos:
            plato = record['p']
            
            print(f"platoID: {plato['platoID']}, nombre: {plato['nombre']}, ingredientes: {plato['ingredientes']}, alergenos: {plato['alergenos']}")

            message = {
            "platoID": plato['platoID'],
            "nombre": plato['nombre'],
            "ingredientes": plato['ingredientes'],
            "alergenos": plato['alergenos']
            }

            print(message)
            producer.send('info', value=message)

    # Cerrar la conexión con Neo4j
    neo4j_client.close()


  

'''
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
    
        #sleep(1)'''