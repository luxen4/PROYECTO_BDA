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
  
    
    def create_relacion(self, session, id_menu, id_restaurante):
        result = session.run("CREATE (r:Relaciones {id_menu: $id_menu, id_restaurante: $id_restaurante}) RETURN r", 
                            id_menu=id_menu, id_restaurante=id_restaurante)

        return result.single()[0]
    

if __name__ == "__main__":
    # URI, usuario y contraseña de la base de datos Neo4j
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "your_password"

    # Crear instancia de Neo4jClient
    neo4j_client = Neo4jClient(uri, user, password)
    neo4j_client.connect()



# Necesito leer un archivo menus.csv
# Function to read and display the content of a CSV file
import csv, json

menus=[]
def read_csv_fileMenus(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            menu ={"id_menu": row[0], "precio": row[1], "disponibilidad": row[2], "id_restaurante": row[3]}
            menus.append(menu)
        return menus

filename = "./menu/menu.csv"
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



platos=[]
def read_csv_filePlatos(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            plato ={"platoID": row[0], "nombre": row[1], "ingredientes": row[2], "alergenos": row[3]}
            platos.append(plato)
        return platos

filename = "./plato/platos.csv"
platos = read_csv_filePlatos(filename)


with neo4j_client._driver.session() as session:
    
    for plato in platos:
        platoID = plato['platoID']
        nombre = plato['nombre']
        ingredientes = plato['ingredientes'] 
        alergenos = plato['alergenos']
        
        
        plat = neo4j_client.create_plato(session,  platoID, nombre, ingredientes, alergenos)
        print(f"Plato creado: {plato['platoID']}, nombre: {plato['nombre']}, ingredientes: {plato['ingredientes']}, alergenos: {plato['alergenos']}")

    
# Function to read and return data from a JSON file
def read_json_fileRelaciones(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None    
    


filename = "./relaciones/relaciones.json"
relaciones = read_json_fileRelaciones(filename)


with neo4j_client._driver.session() as session:
    
    for relacion in relaciones:
        id_menu = relacion['id_menu']
        id_restaurante = relacion['id_restaurante']
        
        relac = neo4j_client.create_relacion(session,  id_menu, id_restaurante)
        
        print(f"Relación creada -> id_menu: {relacion['id_menu']}, id_restaurante: {relacion['id_restaurante']}")


neo4j_client.close()
 