    # Mandar por kafka
from time import sleep                      
from json import dumps
from kafka import KafkaProducer
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

    def get_all_Menus(self, session):
        result = session.run("MATCH (m:Menus) RETURN m")
        return result



if __name__ == "__main__":

    # Crear instancia de Neo4jClient    # URI, usuario y contraseña de la base de datos Neo4j
    neo4j_client = Neo4jClient("bolt://localhost:7687", "neo4j", "your_password")
    neo4j_client.connect()
    # Crear una sesión de Neo4j
    with neo4j_client._driver.session() as session:
        # Consultar todos los proveedores
        menus = neo4j_client.get_all_Menus(session)

        # Imprimir los Menús
        print("Menús en la base de datos:")
        for record in menus:
            print(record)
            menu = record['m']
            
            print(f"menu_id: {menu['id_menu']}, Precio: {menu['precio']}, Disponibilidad: {menu['disponibilidad']}, id_restaurante: {menu['id_restaurante']}")

            message = {
                "id_menu": menu['id_menu'],
                "precio": menu['precio'],
                "disponibilidad": menu['disponibilidad'],
                "id_restaurante": menu['id_restaurante']
            }


            print(message)
            producer.send('menus_stream', value=message)

    # Cerrar la conexión con Neo4j
    neo4j_client.close()