# 1º Crear las tablas de cada uno
import psycopg2

# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_hoteles():
    try:
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
   
        create_table_query = """
            CREATE TABLE IF NOT EXISTS hoteles (
                id_hotel SERIAL PRIMARY KEY,
                nombre_hotel VARCHAR (100),
                direccion_hotel VARCHAR (100),
                empleados VARCHAR (100)
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'HOTELES' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)
   

     
def insertar_Hoteles(id_hotel, nombre_hotel, direccion_hotel, empleados):
    
    #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO hoteles (id_hotel, nombre_hotel, direccion_hotel, empleados) VALUES (%s, %s, %s, %s);", 
                   (id_hotel, nombre_hotel, direccion_hotel, empleados))

    
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla HOTELES.")





     
        
        
def createTable_empleados():
    try:
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        #connection = psycopg2.connect( host="localhost", port="9999", database="primOrd_db", user="primOrd", password="bdaPrimOrd")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS empleados (
                id_empleado SERIAL PRIMARY KEY,
                nombre VARCHAR (100),
                posicion VARCHAR (100),
                fecha_contratacion DATE
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'EMPLEADO' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)

def insertar_Empleados(id_empleado,nombre,posicion,fecha_contratacion):
    
    #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO empleados (id_empleado,nombre,posicion,fecha_contratacion) VALUES (%s, %s, %s, %s);", 
                   (id_empleado,nombre,posicion,fecha_contratacion))

    
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla EMPLEADOS.")
     
     





# leer el csv
import csv

def readCSV_Hoteles(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            id_hotel = row[0]
            nombre_hotel = row[1]
            direccion_hotel = row[2]
            empleados = row[3]
            insertar_Hoteles(id_hotel, nombre_hotel, direccion_hotel, empleados)
            
    



### QUE LEA OTRO ARCHIVO DE JSON ###
def readCSV_Empleados(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            id_empleado = row[0]
            nombre = row[1]
            posicion = row[2]
            fecha_contratacion = row[3]
            insertar_Empleados(id_empleado,nombre,posicion,fecha_contratacion)
 
 
 

createTable_hoteles()
filename="./hoteles.csv"
readCSV_Hoteles(filename)
 
            
createTable_empleados()
filename="./empleados.csv"
readCSV_Empleados(filename)