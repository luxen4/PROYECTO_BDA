import psycopg2
import sessions
from pyspark.sql.functions import size, regexp_replace, split

spark = sessions.sesionSpark()

# Función que elimina una tabla.
def dropTable_wHoteles():
    try:
        
        connection = psycopg2.connect(host="spark-database-1", port="5432", 
                                      database="primord", user="primord", password="bdaprimord")   
        cursor = connection.cursor()
        cursor.execute(""" DROP TABLE IF EXISTS w_hoteles;""")
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_hoteles' DELETED successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  



#Función que crea una tabla.
def createTable_wHoteles():
    try:
        
        connection = psycopg2.connect(host="spark-database-1", port="5432", 
                                      database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_hoteles (
                id_registro SERIAL PRIMARY KEY,
                
                hotel_id INTEGER,
                hotel_name VARCHAR (100),
                
                reserva_id VARCHAR (100),
                
                fecha_llegada Date,
                fecha_salida Date,
                
                empleados INTEGER,
                categoria_habitacion VARCHAR (100),
                price_habitacion DECIMAL(10,2)
            );
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_hoteles' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)    


# Función que escribe el DataFrame en la tabla de PostgreSQL.
def insertJDBC(df):
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"    # Desde dentro es en nombre del contenedor y su puerto
    connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}
    table_name = "w_hoteles" 
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties) # mode="append"
    

'''
def insertarTable_whoteles(hotel_id, hotel_name, reserva_id, fecha_llegada, fecha_salida, empleados, categoria_habitacion, price_habitacion):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_hoteles (hotel_id, hotel_name, reserva_id, fecha_llegada, fecha_salida, empleados, categoria_habitacion, price_habitacion) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s);", 
                       (hotel_id, hotel_name, reserva_id, fecha_llegada, fecha_salida, empleados, categoria_habitacion, price_habitacion))
    
    connection.commit() 
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla w_restaurantes.")
'''
     
# Escribe el DataFrame en la tabla de PostgreSQL
def dataframe_wrestaurantes():
    
    bucket_name = 'my-local-bucket' 

    try:
        
        file_name = 'clientes_json'
        df_clientes= spark.read.json(f"s3a://{bucket_name}/{file_name}")
        #df_clientes.show()
        
        '''
        file_name = 'clientes_csv'
        df_clientes= spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_clientes.show()'''
        
        file_name='reservas_csv'
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df = df_clientes.join(df_reservas.select("id_cliente","id_restaurante","fecha_llegada","fecha_salida","tipo_habitacion","preferencias_comida"), "id_cliente", "left")
        #df.show()
        
        
        file_name = 'restaurantes_json' 
        df_restaurantes= spark.read.json(f"s3a://{bucket_name}/{file_name}")
        #df_restaurantes.show()
        df = df_reservas.join(df_restaurantes.select("id_restaurante","id_hotel"), "id_restaurante", "left")
        
        
        file_name = 'hoteles_json' 
        df_hoteles= spark.read.json(f"s3a://{bucket_name}/{file_name}")
        #df_hoteles.show()
        df = df.join(df_hoteles.select("id_hotel","nombre_hotel","empleados"), "id_hotel", "left")
        
        
        # Habitación
        file_name = 'habitaciones_csv' 
        df_habitaciones = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        
        df_habitaciones = df_habitaciones.withColumnRenamed("numero_habitacion", "habitacion_id")
        # df_habitaciones.show()
        df = df.join(df_habitaciones.select("habitacion_id","categoria","tarifa_por_noche"), "habitacion_id", "left")
        
        
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "tipo_habitacion"]]
        df = df[[col for col in df.columns if col != "preferencias_comida"]]
        df = df[[col for col in df.columns if col != "id_restaurante"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
   
        #df.show()
        
        df = df.dropDuplicates()    # Eliminar registros duplicados
        
        # Modo de inserción sin JDBC
        '''
        for row in df.select("*").collect():
            print(row) hotel_id=row["id_hotel"]
           hotel_name=row["nombre_hotel"]
            reserva_id=row["id_reserva"]
            fecha_llegada=row["fecha_llegada"]
            fecha_salida=row["fecha_salida"]
            empleados=row["empleados"]
            categoria_habitacion=row["categoria"]
            price_habitacion=row["tarifa_por_noche"]
            
            print(f"""id_reserva-Cliente: {id_reserva}, restaurante_name: {restaurante_name}""")
            insertarTable_whoteles( hotel_id, hotel_name, reserva_id, fecha_llegada, fecha_salida, empleados, categoria_habitacion, price_habitacion)
        '''
           
        
        # Reemplazar los valores vacíos en la columna "empleados" con vacio, por si acaso ;)
        df = df.fillna({'empleados': ''})
       
        df = df.withColumn("empleados", regexp_replace("empleados", "\[", ""))   # Eliminar el caracter.
        df = df.withColumn("empleados", regexp_replace("empleados", "\]", ""))
        df = df.withColumn("empleados", size(split(df["empleados"], ",")))       # nº de empleados
        df.show(10)
 
            
        insertJDBC(df)
           
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)


###
dropTable_wHoteles()
createTable_wHoteles()
dataframe_wrestaurantes()
###