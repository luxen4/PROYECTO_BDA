import psycopg2
import sessions


def createTable_wHoteles():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="localhost", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_hoteles (
                id_registro SERIAL PRIMARY KEY,
                
                id_hotel INTEGER,
                hotel_name VARCHAR (100),
                
                id_reserva INTEGER,
                
                fecha_llegada Date,
                fecha_salida Date,
                
                empleados VARCHAR (100),
                categoria_habitacion (100),
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


def insertarTable_whoteles(id_hotel, empleados, categoria_habitacion, price_habitacion):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_hoteles (id_hotel, empleados, categoria_habitacion, price_habitacion) VALUES ( %s, %s, %s, %s);", 
                       (id_hotel, empleados, categoria_habitacion, price_habitacion))
    
    connection.commit() 
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla w_restaurantes.")
     
     
     
def dataframe_wrestaurantes():
    
    spark = sessions.sesionSpark()
    bucket_name = 'my-local-bucket' 

    try:
        
        file_name = 'hoteles_json'
        df_hoteles= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        df_hoteles.show()
        
        
        
        file_name = 'reservas_data.csv' 
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_reservas.show()
        
        
        
        file_name = 'habitaciones_data.csv' 
        df_habitaciones = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_habitaciones.show()
        
        
        df = df_hoteles.join(df_habitaciones.select("id_restaurante","nombre"), "id_restaurante", "left")
        
        df = df.withColumnRenamed("nombre", "restaurante_name")   # Cambiar el nombre de la columna
        df.show()
        

        file_name='menus_csv'      
        df_menus = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        df_menus.show()
        
        
        df = df.join(df_menus.select("id_restaurante","id_menu","precio"), "id_restaurante", "left") #### METER UN NOMBRE DE MENU
        df.show()
        
        '''
        file_name = 'platos'
        df_platos= spark.read.json(f"s3a://{bucket_name}/{file_name}") 
        #df_restaurantes.show()
        df = df.join(df_platos.select("id_restaurante","id_hotel"), "id_restaurante", "left")
        '''
        
      
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "fecha_llegada"]]
        df = df[[col for col in df.columns if col != "fecha_salida"]]
        df = df[[col for col in df.columns if col != "tipo_habitacion"]]
        df = df[[col for col in df.columns if col != "preferencias_comida"]]
        df = df[[col for col in df.columns if col != "id_restaurante"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
        # Mostrar el DataFrame resultante
        df.show()
        
        # df = df.dropDuplicates()    # Eliminar registros duplicados

        for row in df.select("*").collect():
            print(row)
            id_reserva=row["id_reserva"]
            restaurante_name=row["restaurante_name"],
            id_menu=row["id_menu"]
            menu_price=row["precio"]
            
            print(f"""
                  id_reserva-Cliente: {id_reserva}, 
                  restaurante_name: {restaurante_name},
                  id_menu: {id_menu},
                  menu_price: {menu_price}
                  """)
            
            insertarTable_whoteles( id_hotel, empleados, categoria_habitacion, price_habitacion)

           
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)


###
createTable_wHoteles()
dataframe_wrestaurantes()
###



'''
5.2.1 Análisis de las preferencias de los clientes
¿Cuáles son las preferencias alimenticias más comunes entre los clientes?
5.2.2 Análisis del rendimiento del restaurante:
¿Qué restaurante tiene el menu_price medio de menú más alto?
¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?
5.2.3 Patrones de reserva
¿Cuál es la duración media de la estancia de los clientes de un hotel?
¿Existen periodos de máxima ocupación en función de las fechas de reserva?
5.2.4 Gestión de empleados
¿Cuántos empleados tiene de media cada hotel?
5.2.5 Ocupación e ingresos del hotel
¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de
habitación?
¿Podemos estimar los ingresos generados por cada hotel basándonos en los
menu_prices de las habitaciones y los índices de ocupación?
5.2.6 Análisis de menús
¿Qué platos son los más y los menos populares entre los restaurantes?
23/24 - IABD - Big Data Aplicado
¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los
platos?
5.2.7 Comportamiento de los clientes
¿Existen pautas en las preferencias de los clientes en función de la época del año?
¿Los clientes con preferencias dietéticas específicas tienden a reservar en
restaurantes concretos?
5.2.8 Garantía de calidad
¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas
reales realizadas?
5.2.9 Análisis de mercado
¿Cómo se comparan los menu_prices de las habitaciones de los distintos hoteles y
existen valores atípicos?'''



