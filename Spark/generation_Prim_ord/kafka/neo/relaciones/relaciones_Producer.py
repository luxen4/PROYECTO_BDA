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

    def get_all_Relaciones(self, session):
        result = session.run("MATCH (r:Relaciones) RETURN r")
        return result


if __name__ == "__main__":
                                                                                        # Uri, usuario ypassword
    neo4j_client = Neo4jClient("bolt://localhost:7687", "neo4j", "your_password")       # Crear instancia de Neo4jClient
    neo4j_client.connect()
   
    with neo4j_client._driver.session() as session:         # Crear una sesión de Neo4j
        relaciones = neo4j_client.get_all_Relaciones(session)       # Consultar todos 

        # Imprimir las relaciones
        print("Relaciones en la base de datos:")
        for record in relaciones:
            relacion = record['r']
            
            print(f"id_menu: {relacion['id_menu']}, id_plato: {relacion['id_plato']}")

            message = {
            "id_menu": relacion['id_menu'],
            "id_plato": relacion['id_plato'],
            }

            print(message)
            producer.send('relaciones', value=message)

    # Cerrar la conexión con Neo4j
    neo4j_client.close()

