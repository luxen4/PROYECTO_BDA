from time import sleep                      # Ejecutar con el botón derecho
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
import random  # Importa la librería random
                    
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
# producer = KafkaProducer(bootstrap_servers= ['kafka:9093'], value_serializer=lambda x: dumps(x).encode('utf-8'))

# Function to read and display the contents of a text file


reserva_id=0
cliente_id=0   
fecha_llegada=''
fecha_salida=''
tipo_habitacion=''
preferencias_comida=''
habitacion_id=0
id_restaurante=0
                    

def read_text_file(filename):
    try:
        
        
        with open(filename, 'r') as file:
            
            for line in file:
                line = line.strip()
                if line.startswith('*** Reserva'):          id_reserva =            line
                if line.startswith('ID Cliente'):           cliente_id =            line.split(':')[1]
                if line.startswith('Fecha Llegada'):        fecha_llegada =         line.split(':')[1]
                if line.startswith('Fecha Salida'):         fecha_salida =          line.split(':')[1]
                if line.startswith('Tipo Habitacion'):      tipo_habitacion =       line.split(':')[1]
                if line.startswith('Preferencias Comida'):  preferencias_comida =   line.split(':')[1]
                if line.startswith('Id Habitacion'):        habitacion_id =         line.split(':')[1]
                
                if line.startswith('ID Restaurante'):       
                    id_restaurante = line.split(':')[1]
            
                    message = {
                    "id_reserva": id_reserva,
                    "timestamp": int(datetime.now().timestamp() * 1000),
                    "id_cliente": cliente_id.strip(),
                    "fecha_llegada": fecha_llegada.strip(),
                    "fecha_salida": fecha_salida.strip(),
                    "tipo_habitacion": tipo_habitacion.strip(),
                    "preferencias_comida": preferencias_comida.strip(),
                    "habitacion_id": habitacion_id.strip(),
                    "id_restaurante": id_restaurante.strip(),
                    }
                    
                                            
                    print(message)
                    producer.send('info', value=message)
                    #sleep(1)

    except FileNotFoundError:
        print(f"File '{filename}' not found.")

#filename='./../../../../data_Prim_ord/text/reservas.txt'
filename = "./Spark/data_Prim_ord/text/reservas.txt"
read_text_file(filename)