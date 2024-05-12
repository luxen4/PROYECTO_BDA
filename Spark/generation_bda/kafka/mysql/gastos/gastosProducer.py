# Mandar por kafka
from time import sleep                      
from json import dumps
from kafka import KafkaProducer

import mysql.connector
import csv, json

conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="primord_db")

cursor = conexion.cursor()      # Crear un cursor
sql = "SELECT * FROM gastos"    # Consulta SQL para seleccionar todos los clientes

# Ejecutar la consulta
cursor.execute(sql)

# Obtener todos los resultados
resultados = cursor.fetchall()



producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
#producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

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
    
    print(message)
    producer.send('gastos_stream', value=message)
        
    #sleep(1)
        
        
        
        

''' Por si se quiere crear un archivo desde la consulta desde mysql
# Después de una consulta que guarde en un archivo
nombre_archivo = "./Spark/data_bda/text/gastosWWW.json"
# Guardar los datos en el archivo JSON
with open(nombre_archivo, "w") as archivo_json:
    json.dump(lista_gastos, archivo_json, indent=4)

print(f"Los datos se han guardado en el archivo {nombre_archivo}")


# Después de obtener los resultados
nombre_archivo = "./Spark/data_bda/text/gastosWWW.csv"

# Guardar los datos en el archivo CSV
with open(nombre_archivo, "w", newline="") as archivo_csv:
    escritor_csv = csv.writer(archivo_csv)
    
    # Escribir el encabezado del archivo CSV
    escritor_csv.writerow(["id", "fecha", "id_hotel", "concepto", "monto", "pagado"])
    
    # Escribir los datos de los gastos en el archivo CSV
    for gasto in resultados:
        escritor_csv.writerow(gasto)

print(f"Los datos se han guardado en el archivo {nombre_archivo}")
'''