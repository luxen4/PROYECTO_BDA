# 1º Crear las tablas de cada uno
import psycopg2
import csv
  
def createTable_empleados():
    try:
        connection = psycopg2.connect(host="localhost", port="9999", database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
        # Probar mayusculas 
         
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
    
    connection = psycopg2.connect( host="localhost", port="9999", database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
    
    cursor = connection.cursor()
    cursor.execute("INSERT INTO empleados (id_empleado,nombre,posicion,fecha_contratacion) VALUES (%s, %s, %s, %s);", 
                   (id_empleado,nombre,posicion,fecha_contratacion))

    
    connection.commit()     # Confirmar los cambios y cerrar la conexión con la base de datos
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla EMPLEADOS.")
     

### QUE LEA OTRO ARCHIVO DE JSON ###
def readCSV_Empleados(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            print(row)
            id_empleado = row[0]
            nombre = row[1]
            posicion = row[2]
            fecha_contratacion = row[3]
            insertar_Empleados(id_empleado,nombre,posicion,fecha_contratacion)
            
createTable_empleados()
filename="./../../../data_bda/csv/empleados.csv"
readCSV_Empleados(filename)



# Probar que lo meta con jdbc, se va a hacer en el warehouse.