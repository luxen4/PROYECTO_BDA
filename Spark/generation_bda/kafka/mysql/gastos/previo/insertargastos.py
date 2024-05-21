import mysql.connector
import random, string

conexion = mysql.connector.connect( host="localhost", user="user1", password="alberite", database="retail_db")
cursor = conexion.cursor()


def read_text_file(filename):
    try:
        with open(filename, 'r') as file:
            
            for line in file:
                line = line.strip()
                
                fecha = line.split('*')[0]
                id_hotel = line.split('*')[1] 
                concepto = line.split('*')[2]
                monto = line.split('*')[3]
                pagado= line.split('*')[4]
                
                # Crear un cursor para ejecutar consultas
                cursor = conexion.cursor()
                
                cursor.execute(""" INSERT INTO gastos (fecha, id_hotel, concepto, monto, pagado) VALUES (%s, %s, %s, %s, %s) """, 
                               (fecha, id_hotel, concepto, monto, pagado))
                
                # Hacer commit para confirmar la transacción
                conexion.commit()
                
                print("Registro insertado")

            # Cerrar el cursor y la conexión
            cursor.close()
            conexion.close()

    except FileNotFoundError:
        print(f"File '{filename}' not found.")


filename = "./Spark/data_Prim_ord/text/gastos.txt"
read_text_file(filename)
