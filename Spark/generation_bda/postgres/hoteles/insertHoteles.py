import psycopg2

def createTable_hoteles():
    try:
        connection = psycopg2.connect( host="localhost", port="9999", database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
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
    
    connection = psycopg2.connect(host="localhost", port="9999", database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO hoteles (id_hotel, nombre_hotel, direccion_hotel, empleados) VALUES (%s, %s, %s, %s);", 
                   (id_hotel, nombre_hotel, direccion_hotel, empleados))

    
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla HOTELES.")


# leer el csv
import csv

def readCSV_Hoteles(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            print(row)
            id_hotel = row[0]
            nombre_hotel = row[1]
            direccion_hotel = row[2]
            empleados = row[3]
            insertar_Hoteles(id_hotel, nombre_hotel, direccion_hotel, empleados)


createTable_hoteles()
filename="./../../../data_bda/csv/hoteles.csv"
readCSV_Hoteles(filename)
