import psycopg2
import sessions

spark = sessions.sesionSpark()
bucket_name = 'my-local-bucket' 

def dropTable_wRestaurantes():
    try:
        connection = psycopg2.connect(host="spark-database-1", port="5432", 
                                      database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
        
        cursor = connection.cursor()
        create_table_query = """ DROP TABLE IF EXISTS w_restaurantes;"""
        
        cursor.execute(create_table_query)
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print("Table 'w_restaurantes' DELETED successfully.")
    except Exception as e:
        print("An error occurred while creating the table:")
        print(e)  


def createTable_wRestaurantes():
    try:
        connection = psycopg2.connect(host="spark-database-1", port="5432", 
                                      database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
        
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
                ingredientes VARCHAR (100),
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


# Escribe el DataFrame en la tabla de PostgreSQL
def insertJDBC(df):
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"    # Desde dentro es en nombre del contenedor y su puerto
    connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}
    table_name = "w_restaurantes" 
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties) # mode="append"
    

'''
def insertarTable_wrestaurantes(id_reserva, restaurante_id, restaurante_name, id_menu, menu_price, plato_id, plato_name, ingredientes, alergenos):
    
    connection = psycopg2.connect(host="spark-database-1", port="5432", 
                                      database="primord", user="primord", password="bdaprimord")   # Conexión a la base de datos PostgreSQL
         
    cursor = connection.cursor()
    cursor.execute("INSERT INTO w_restaurantes (id_reserva, restaurante_id, restaurante_name, id_menu, menu_price, plato_id, plato_name, ingredientes, alergenos) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s);", 
                       (id_reserva, restaurante_id, restaurante_name, id_menu, menu_price, plato_id, plato_name, ingredientes, alergenos))
    
    connection.commit() 
    cursor.close()
    connection.close()

    print("Datos cargados correctamente en tabla w_restaurantes.")
'''     
     
     
def dataframe_wrestaurantes():

    try:
        
        file_name = 'reservas_csv' 
        df_reservas = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_reservas.show()
        
        file_name = 'restaurantes_json'
        df_restaurantes= spark.read.json(f"s3a://{bucket_name}/{file_name}") # No tocar
        #df_restaurantes.show()
        
        df = df_reservas.join(df_restaurantes.select("id_restaurante","nombre"), "id_restaurante", "left")
        df = df.withColumnRenamed("nombre", "restaurante_name")   # Cambiar el nombre de la columna
        #df.show()
        

        file_name='menus_csv'      
        df_menus = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        #df_menus.show()
        
        df = df.join(df_menus.select("id_restaurante","id_menu","precio"), "id_restaurante", "left") #### METER UN NOMBRE DE MENU
        #df.show()
        
       
        file_name = 'relaciones_csv'
        df_relaciones= spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True) 
        #df_relaciones.show()
        df = df.join(df_relaciones.select("id_menu","id_plato"), "id_menu", "left")
        
         
        file_name='plato_csv'      
        df_platos = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
        
        
        
        file_name='plato_json'      
        #df_platos = spark.read.json(f"s3a://{bucket_name}/{file_name}")
        
        df_platos = df_platos.withColumnRenamed("platoID", "id_plato")   # Cambiar el nombre de la columna
        df_platos.show()
        df = df.join(df_platos.select("id_plato","nombre","ingredientes","alergenos"), "id_plato", "left") #### METER UN NOMBRE DE MENU
        
        # Eliminar columnas"
        df = df[[col for col in df.columns if col != "timestamp"]]
        df = df[[col for col in df.columns if col != "fecha_llegada"]]
        df = df[[col for col in df.columns if col != "fecha_salida"]]
        df = df[[col for col in df.columns if col != "tipo_habitacion"]]
        df = df[[col for col in df.columns if col != "preferencias_comida"]]
        df = df[[col for col in df.columns if col != "id_cliente"]]
      
        df = df.dropDuplicates()    # Eliminar registros duplicados
        df.show()
        '''
        for row in df.select("*").collect():
            print(row)
            id_reserva=row["id_reserva"],
            restaurante_id = row["id_restaurante"]
            restaurante_name=row["restaurante_name"]
            id_menu=row["id_menu"]
            # menu_name=row["menu_name"]   Podríamos agrear un menu_name para mysql (HACER)
            menu_price=row["precio"]
            plato_id = row["id_plato"]
            plato_name = row["nombre"]
            ingredientes = row["ingredientes"]
            alergenos = row["alergenos"]
            
            
            print(f""" 
                  id_reserva-Cliente: {id_reserva}, 
                  restaurante_id: = {restaurante_id}, restaurante_name: {restaurante_name},
                  id_menu: {id_menu},  menu_price: {menu_price},
                  plato_id:{plato_id}, plato_name:{plato_name}, ingredientes:{ingredientes}alergenos:{alergenos}""")
            
            insertarTable_wrestaurantes( id_reserva, restaurante_id, restaurante_name, id_menu, menu_price, plato_id, plato_name, ingredientes, alergenos)
            '''
            
        insertJDBC(df)
        
    
    except Exception as e:
        print("error reading TXT")
        print(e)

###
dropTable_wRestaurantes()
createTable_wRestaurantes()
dataframe_wrestaurantes()
###

spark.stop()
