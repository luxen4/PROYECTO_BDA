import psycopg2
import sessions

spark = sessions.sesionSpark()
bucket_name = 'my-local-bucket' 

def dropTable_wReservas():
    try:
        connection = psycopg2.connect(host="spark-database-1", port="5432", 
                                      database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
        
        cursor = connection.cursor()
        create_table_query = """ DROP TABLE IF EXISTS w_reservas;"""
        
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_reservas' DELETED successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  




def createTable_wReservas():
    try: 
        connection = psycopg2.connect(host="spark-database-1", port="5432", 
                                      database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
        
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_reservas (
                registro_id SERIAL PRIMARY KEY,
                
                reserva_id VARCHAR (100),
                fecha_entrada DATE,
                fecha_salida DATE,
                
                cliente_name VARCHAR (100),
                
                hotel_id INTEGER,
                nombre_hotel VARCHAR (100),
                
                
                categoria_habitacion VARCHAR (100),
                tarifa_nocturna Decimal(10,2),
                preferencia_comida VARCHAR (100),
                
                restaurante_id INTEGER,
                restaurante_name VARCHAR (100)
            );
        """
# empleados VARCHAR (500),       
        
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_reservas' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  

# Escribe el DataFrame en la tabla de PostgreSQL
def insertJDBC(df):
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"    # Desde dentro es en nombre del contenedor y su puerto
    connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}
    table_name = "w_reservas" 
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties) # mode="append"
    

'''
def insertarTable_wreservas(reserva_id, fecha_entrada, fecha_salida, cliente_name ,hotel_id, nombre_hotel,
                    categoria_habitacion, tarifa_nocturna, preferencia_comida, restaurante_id,
                    restaurante_name):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_reservas (reserva_id, fecha_entrada, fecha_salida, cliente_name ,hotel_id, nombre_hotel,categoria_habitacion, tarifa_nocturna, preferencia_comida, restaurante_id,restaurante_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", 
                       (reserva_id, fecha_entrada, fecha_salida, cliente_name ,hotel_id, nombre_hotel,
                    categoria_habitacion, tarifa_nocturna, preferencia_comida, restaurante_id,
                    restaurante_name))
    
    connection.commit() 
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla w_reservas.")
'''    
     
     
def dataframe_wreservas():

    try:
    
        file_name = 'reservas_csv' 
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_reservas.show()
        
        
        file_name='restaurantes_json'      
        df_restaurantes= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        #df_restaurantes.show()
        
        
        df = df_reservas.join(df_restaurantes.select("id_restaurante","nombre","id_hotel"), "id_restaurante", "left")  #### METER UN NOMBRE DE MENU
        df = df.withColumnRenamed("nombre", "restaurante_name")                                             # Cambiar el nombre de la columna
        #df.show()
        

        file_name = 'hoteles_json'
        df_hoteles= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar                          #df_hoteles.show()
        df = df.join(df_hoteles.select("id_hotel","nombre_hotel","empleados"), "id_hotel", "left")          #df.show()
        
    
        file_name = 'habitaciones_csv' 
        df_habitaciones = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_habitaciones = df_habitaciones.withColumnRenamed("numero_habitacion", "habitacion_id")           #df_habitaciones.show()  
                                                                                                            # Cambiar el nombre de la columna
        
        
        # Me vuelves loco Rafa con no llamar a columnas iguales, con nombres iguales (o la práctica se está gestando todavía o vamos a pillar ;) )
        df = df.join(df_habitaciones.select("habitacion_id","categoria","tarifa_por_noche"), "habitacion_id", "left")   #df.show()
        
        
        file_name = 'clientes_json' 
        df_clientes = spark.read.json(f"s3a://{bucket_name}/{file_name}")                                               #df_clientes.show()
        df = df.join(df_clientes.select("id_cliente", "nombre", "preferencias_alimenticias"), "id_cliente", "left")
        df = df.withColumnRenamed("nombre", "cliente_name")
      
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "preferencias_comida"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
       
        #df.show()
        df = df.dropDuplicates()    # Eliminar registros duplicados
       
        '''
        # No tocar que es OK
        for row in df.select("*").collect():
            print(row)
            reserva_id=row["id_reserva"]
            fecha_entrada=row["fecha_llegada"]
            fecha_salida=row["fecha_salida"]
            cliente_name=row["cliente_name"]
            hotel_id=row["id_hotel"]
            nombre_hotel=row["nombre_hotel"]
            categoria_habitacion=row["categoria"]
            tarifa_nocturna=row["tarifa_por_noche"]
            preferencia_comida=row["preferencias_alimenticias"]
            restaurante_id=row["id_restaurante"]
            restaurante_name=row["restaurante_name"]
            
            print(f""" reserva_id: {reserva_id}, fecha_entrada: {fecha_entrada}""")
            
            
            # Hoteles, reservas, habitaciones, empleados
            insertarTable_wreservas(reserva_id, fecha_entrada, fecha_salida, cliente_name ,hotel_id, nombre_hotel,
                    categoria_habitacion, tarifa_nocturna, preferencia_comida, restaurante_id,
                    restaurante_name)'''
            
        insertJDBC(df)
            
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)


###
dropTable_wReservas()
createTable_wReservas()
dataframe_wreservas()
###



