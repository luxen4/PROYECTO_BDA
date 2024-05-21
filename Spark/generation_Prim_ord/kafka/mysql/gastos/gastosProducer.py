# Mandar por kafka
from time import sleep                      
from json import dumps
from kafka import KafkaProducer
import json, csv

import mysql.connector

# Por si se quiere crear un archivo desde la consulta desde mysql
# Después de una consulta que guarde en un archivo


# Guardar los datos en el archivo JSON
def create_csv_file(file_name, data):
    with open(file_name, "w") as archivo_json:
        json.dump(data, archivo_json, indent=4)

    print(f"Los datos se han guardado en el archivo {file_name}")


# Guardar los datos en el archivo CSV
# Guardar los resultados en un archivo CSV  
def create_csv_file(filename, data):
    
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file) 

        writer.writerow(["id_hotel", "fecha", "concepto", "monto", "pagado"])  # Escribir el encabezado
        
        for gasto in data:
            writer.writerow([gasto['id_hotel'], gasto['fecha'], gasto['concepto'], gasto['monto'], gasto['pagado']])
    
    print(f"Archivo CSV '{filename}' generado exitosamente.")




# Función para consulta, devuelve una tupla
def selectGastos():
    conexion = mysql.connector.connect( host="localhost",user="user1",password="alberite",database="retail_db")

    cursor = conexion.cursor()      # Crear un cursor
    sql = "SELECT * FROM gastos"    # Consulta SQL para seleccionar todos los clientes
    cursor.execute(sql)             # Ejecutar la consulta
    resultados = cursor.fetchall()  # Obtener todos los resultados
    return resultados


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
#producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

resultados = selectGastos()

print("Mandados:")
lista_gastos = []
for gasto in resultados:
    print(gasto)
    registro=gasto[0]
    fecha= gasto[1]
    id_hotel=gasto[2]
    concepto=gasto[3]
    monto=gasto[4]
    pagado=gasto[5]
    
    message = {
            "id_hotel": gasto[0],
            "fecha": str(gasto[1]),
            "id_hotel": gasto[2],
            "concepto": gasto[3],
            "monto": str(gasto[4]),
            "pagado": gasto[5]
        }
    
    lista_gastos.append(message)
    
    #print(message)
    producer.send('gastos_stream', value=message)
        
    #sleep(1)
        
file_name = "./Spark/data_Prim_ord/json/gastos.json"
#create_json_file(file_name, lista_gastos)  


    
# Después de obtener los resultados
file_name = "./Spark/data_Prim_ord/csv/gastosADRIAN.csv"     
#create_csv_file(file_name, lista_gastos)
        

