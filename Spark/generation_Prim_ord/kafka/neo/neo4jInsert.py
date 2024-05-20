import csv, json
from neo4j import GraphDatabase


# Crear los registros
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

    def create_menu(self, session, id_menu, precio, disponibilidad, id_restaurante):
        result = session.run("CREATE (m:Menus {id_menu: $id_menu, precio: $precio, disponibilidad: $disponibilidad, id_restaurante:$id_restaurante}) RETURN m", 
                             id_menu=id_menu, precio=precio, disponibilidad=disponibilidad, id_restaurante=id_restaurante)
    
        return result.single()[0]
  
    def create_plato(self, session, platoID, nombre, ingredientes, alergenos):
        result = session.run("CREATE (p:Platos {platoID: $platoID, nombre: $nombre, ingredientes: $ingredientes, alergenos:$alergenos}) RETURN p", 
                            platoID=platoID, nombre=nombre, ingredientes=ingredientes, alergenos=alergenos)

        return result.single()[0]
  
    
    def create_relacion(self, session, id_menu, id_plato):
        result = session.run("CREATE (r:Relaciones {id_menu: $id_menu, id_plato: $id_plato}) RETURN r", 
                            id_menu=id_menu, id_plato=id_plato)

        return result.single()[0]
    



menus=[]
# Leer un archivo csv, recorre las líneas y devuelve una lista con los atributos
def read_csv_fileMenus(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            menu ={"id_menu": row[0], "precio": row[1], "disponibilidad": row[2], "id_restaurante": row[3]}
            menus.append(menu)
        return menus
    

platos=[]
# Lee un archivo y devuelve una lista de items con sus atributos
def read_csv_filePlatos(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            plato ={"platoID": row[0], "nombre": row[1], "ingredientes": row[2], "alergenos": row[3]}
            platos.append(plato)
        return platos
    

# Function to read and return data from a JSON file
def read_json_fileRelaciones(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None   



if __name__ == "__main__":

    # Crear instancia de Neo4jClient # URI, usuario y contraseña de la base de datos Neo4j
    neo4j_client = Neo4jClient("bolt://localhost:7687", "neo4j", "your_password")
    neo4j_client.connect()


    # Inserta desde un csv a neo4j
    filename = "./Spark/data_Prim_ord/csv/menu.csv"
    menus = read_csv_fileMenus(filename)

    with neo4j_client._driver.session() as session:
        
        for menu in menus:
            id_menu = menu['id_menu']
            precio = menu['precio']
            disponibilidad = menu['disponibilidad']
            id_restaurante = menu['id_restaurante']
            
            men = neo4j_client.create_menu(session, id_menu, precio, disponibilidad, id_restaurante)
            print(f"Menú creado: {menu['id_menu']}, precio {menu['precio']},disponible: {menu['disponibilidad']}, id_restaurante: {menu['id_restaurante']}")
    neo4j_client.close()


    # Inserta desde un csv a neo4j
    filename = "./Spark/data_Prim_ord/csv/platos.csv"
    platos = read_csv_filePlatos(filename)

    with neo4j_client._driver.session() as session:
        for plato in platos:
            platoID = plato['platoID']
            nombre = plato['nombre']
            ingredientes = plato['ingredientes'] 
            alergenos = plato['alergenos']
            
            plat = neo4j_client.create_plato(session,  platoID, nombre, ingredientes, alergenos)
            print(f"Plato creado: {plato['platoID']}, nombre: {plato['nombre']}, ingredientes: {plato['ingredientes']}, alergenos: {plato['alergenos']}")
    neo4j_client.close()
        


    # Inserta desde un json en neo4j
    filename = "./Spark/data_Prim_ord/json/relaciones.json"
    relaciones = read_json_fileRelaciones(filename)

    with neo4j_client._driver.session() as session:
        for relacion in relaciones:
            id_menu = relacion['id_menu']
            id_restaurante = relacion['id_plato']
            
            relac = neo4j_client.create_relacion(session,  id_menu, id_restaurante)
            print(f"Relación creada -> id_menu: {relacion['id_menu']}, id_plato: {relacion['id_plato']}")
    neo4j_client.close()


# Se lee de archivos csv o json y se insertan los registros en Neo4j