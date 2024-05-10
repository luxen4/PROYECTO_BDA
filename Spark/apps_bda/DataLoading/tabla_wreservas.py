import psycopg2
import sessions


# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_wReservas():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_reservas (
                id_registro SERIAL PRIMARY KEY,
                id_reserva VARCHAR (100),
                fecha_entrada DATE,
                fecha_salida DATE,
                nombre_hotel VARCHAR (100),
                empleados VARCHAR (500),
                categoria_habitacion VARCHAR (100),
                tarifa_nocturna Decimal(10,2),
                preferencia_comida VARCHAR (100)
            );
        """
        
        """ Meter esta
        id_registro SERIAL PRIMARY KEY,
        id_reserva VARCHAR (100),
        cliente_name VARCHAR (100),
        fecha_entrada DATE,
        fecha_salida DATE,
        nombre_hotel VARCHAR (100),
        empleados VARCHAR (500),
        categoria_habitacion VARCHAR (100),
        tarifa_nocturna Decimal(10,2),
        preferencia_comida VARCHAR (100),
        restaurante_name VARCHAR (100)
        """
        
        
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_reservas' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  



def insertarTable_wreservas(id_reserva, fecha_entrada, fecha_salida, nombre_hotel, empleados):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_reservas (id_reserva, fecha_entrada, fecha_salida, nombre_hotel, empleados) VALUES (%s, %s, %s, %s, %s);", 
                       (id_reserva, fecha_entrada, fecha_salida, nombre_hotel, empleados))
    
    connection.commit() 
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla w_reservas.")
     
     
     
def dataframe_wreservas():
    spark = sessions.sesionSpark()
    bucket_name = 'my-local-bucket' 

    try:
    
        file_name = 'data_reservas' 
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_reservas.show()
        
        
        file_name='restaurantes.json'      
        df_restaurantes= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        #df_restaurantes.show()
        
        
        df = df_reservas.join(df_restaurantes.select("id_restaurante","nombre","id_hotel"), "id_restaurante", "left")  #### METER UN NOMBRE DE MENU
        df = df.withColumnRenamed("nombre", "restaurante_name")                                             # Cambiar el nombre de la columna
        #df.show()
        

        file_name = 'data_hoteles.json'
        df_hoteles= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        #df_hoteles.show()
        
        
        df = df.join(df_hoteles.select("id_hotel","nombre_hotel","empleados"), "id_hotel", "left")
        df.show()
        
    
        file_name = 'data_habitaciones.csv' 
        df_habitaciones = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_habitaciones.show()
        
        df = df.join(df_habitaciones.select("id_hotel","categoria"), "id_hotel", "left")
        df.show()
        
        
        #CLIENTES
        
        
        
        
        
        
      
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "preferencias_comida"]]
        df = df[[col for col in df.columns if col != "id_restaurante"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
        # Mostrar el DataFrame resultante
        df.show()
        
        # df = df.dropDuplicates()    # Eliminar registros duplicados

       
        
        # No tocar que es OK
        for row in df.select("*").collect():
            print(row)
            id_reserva=row["id_reserva"]
            fecha_llegada=row["fecha_llegada"]
            fecha_salida=row["fecha_salida"]
            nombre_hotel=row["nombre_hotel"]
            empleados=row["empleados"]
            
            print(f"""
                  id_reserva-Cliente: {id_reserva}, 
                  fecha_llegada: {fecha_llegada},
                  fecha_salida: {fecha_salida},
                  nombre_hotel: {nombre_hotel}
                  empleados= {"empleados"}
                  """)
            
            
            # Hoteles, reservas, habitaciones, empleados
            insertarTable_wreservas(id_reserva, fecha_llegada, fecha_salida, nombre_hotel, empleados)
           
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)


###
createTable_wReservas()
dataframe_wreservas()
###



