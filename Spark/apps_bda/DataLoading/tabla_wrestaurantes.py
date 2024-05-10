import psycopg2
import sessions

    
# Crear una tabla para responder a las preguntas de ANALISIS-VENTAS en WAREHOSE
def createTable_wRestaurantes():
    try:
        #connection = psycopg2.connect( host="my_postgres_service", port="5432", database="warehouse_retail_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
        connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    
        cursor = connection.cursor()
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS w_restaurantes (
                id_registro SERIAL PRIMARY KEY,
                id_reserva VARCHAR (100),
                
                restaurante_id INTEGER,
                restaurante_name VARCHAR (100),
                
                id_menu INTEGER,
               
                menu_price DECIMAL(10,2),   
                
                plato_id INTEGER,
                plato_name VARCHAR (100),
                alergenos VARCHAR (100)
            );
        """
        #  menu_name VARCHAR (100),
                                                                                   
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'W_RESTAURANTES' created successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  

def insertarTable_wrestaurantes(id_reserva, restaurante_id, restaurante_name, id_menu, menu_price, plato_id, plato_name, alergenos):
    
    connection = psycopg2.connect( host="my_postgres_service", port="5432", database="primord_db", user="postgres", password="casa1234")   # Conexión a la base de datos PostgreSQL
    # connection = psycopg2.connect( host="my_postgres_service", port="9999", database="primord_db", user="PrimOrd", password="bdaPrimOrd")   
        
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_restaurantes (id_reserva, restaurante_id, restaurante_name, id_menu, menu_price, plato_id, plato_name, alergenos) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s);", 
                       (id_reserva, restaurante_id, restaurante_name, id_menu, menu_price, plato_id, plato_name, alergenos))
    
    connection.commit() 
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla w_restaurantes.")
     
     
     
def dataframe_wrestaurantes():
    
    spark = sessions.sesionSpark()
    bucket_name = 'my-local-bucket' 

    try:
        
        file_name = 'data_reservas' 
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_reservas.show()
        
        file_name = 'restaurantes_data.json'
        df_restaurantes= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        #df_restaurantes.show()
        
        df = df_reservas.join(df_restaurantes.select("id_restaurante","nombre"), "id_restaurante", "left")
        df = df.withColumnRenamed("nombre", "restaurante_name")   # Cambiar el nombre de la columna
        #df.show()
        

        file_name='data_menus'      
        df_menus = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_menus.show()
        
        df = df.join(df_menus.select("id_restaurante","id_menu","precio"), "id_restaurante", "left") #### METER UN NOMBRE DE MENU
        #df.show()
        
       
        file_name = 'data_relaciones'
        df_relaciones= spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True) 
        #df_relaciones.show()
        df = df.join(df_relaciones.select("id_menu","id_plato"), "id_menu", "left")
        #df.show()
        
         
        file_name='data_platos'      
        df_platos = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        
        df_platos = df_platos.withColumnRenamed("platoID", "id_plato")   # Cambiar el nombre de la columna
        df_platos.show()
        
        df.show()
        
        df = df.join(df_platos.select("id_plato","nombre","ingredientes","alergenos"), "id_plato", "left") #### METER UN NOMBRE DE MENU
        
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "fecha_llegada"]]
        df = df[[col for col in df.columns if col != "fecha_salida"]]
        df = df[[col for col in df.columns if col != "tipo_habitacion"]]
        df = df[[col for col in df.columns if col != "preferencias_comida"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
        # Mostrar el DataFrame resultante
      
        
        # df = df.dropDuplicates()    # Eliminar registros duplicados

        
        for row in df.select("*").collect():
            #print(row)
            id_reserva=row["id_reserva"],
            restaurante_id = row["id_restaurante"]
            restaurante_name=row["restaurante_name"]
            id_menu=row["id_menu"]
            # menu_name=row["menu_name"]   Podríamos agrear un menu_name para mysql (HACER)
            menu_price=row["precio"]
            plato_id = row["id_plato"]
            plato_name = row["nombre"]
            alergenos = row["alergenos"]
            
            '''
            print(f"""
                  id_reserva-Cliente: {id_reserva}, 
                  restaurante_id: = {restaurante_id},
                  restaurante_name: {restaurante_name},
                  id_menu: {id_menu},
                  menu_price: {menu_price},
                  plato_id:{plato_id},
                  plato_name:{plato_name},
                  alergenos:{alergenos}
                  """)'''
            
            #insertarTable_wrestaurantes( id_reserva, restaurante_id, restaurante_name, id_menu, menu_price, plato_id, plato_name, alergenos)
           
        spark.stop()
    
    except Exception as e:
        print("error reading TXT")
        print(e)

###
createTable_wRestaurantes()
dataframe_wrestaurantes()
###




# https://stackoverflow.com/questions/65114334/pyspark-join-with-different-column-names-and-cant-be-hard-coded-before-runti      
# left_key = 'leftColname'
# right_key = 'rightColname'
# final = ta.join(tb, ta[left_key] == tb[right_key], how='left')