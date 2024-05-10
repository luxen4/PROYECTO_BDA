import sessions

jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord_db"
connection_properties = {"user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

spark = sessions.sesionSpark()

def select():
    
    df = spark.read.jdbc(url=jdbc_url, table="w_reservas", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    # Crear una tabla temporal para descomponer la lista de empleados en filas separadas
    spark.sql("""
        CREATE TEMP VIEW temp_empleados AS
        SELECT nombre_hotel, EXPLODE(SPLIT(empleados, ',')) AS num_empleados
        FROM tabla_spark
    """)

    # Calcular la media de los empleados por hotel
    media_empleados = spark.sql("""
        SELECT nombre_hotel, count(DISTINCT num_empleados) AS media_empleados
        FROM temp_empleados
        GROUP BY nombre_hotel
    """)
    
    media_empleados.show()
    spark.stop()
    
print("¿Cuántos empleados tiene de media cada hotel?")    
select()



