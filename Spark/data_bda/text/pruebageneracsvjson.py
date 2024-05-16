from pymongo import MongoClient
import json, csv

# Function to create a JSON file
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file)
        file.write('\n')
    print(f"File '{filename}' created successfully!")
  
  
# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    products = data[0]['productos']
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file) 

        writer.writerow(["product_ID", "tipo", "talla", "color"])  # Escribir el encabezado
        for product in products:
            writer.writerow([product['id'], product['tipo'], product['talla'], product['color']])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")



client = MongoClient()                      # Accede a la colección "ropa"
db = client["products"]
ropa_collection = db["ropa"] 


resultados = ropa_collection.find()         # Obtener los resultados de la colección
productos_list = list(resultados)

# Convertir los ObjectId a cadenas en cada documento
data=[]
for producto in productos_list:
    #print(producto)
    producto['_id'] = str(producto['_id'])
    data = producto['productos']
    print(data)
    




# JSON
create_json_file('./../../../data_bda/json/data_products.json', data)

# CSV
file_name = "./../../../data_bda/csv/data_products.csv"
#create_csv_file(file_name, productos_list)




# Realiza una consulta para encontrar todos los productos de tipo "pantalones"
#consulta = { "tipo": "pantalones" }
# Ejecuta la consulta y obtén los resultados
#resultados = ropa_collection.find(consulta)



        # Eliminar la línea que convierte la fecha a una cadena ISO
        # item['Fecha'] = item['Fecha'].isoformat()