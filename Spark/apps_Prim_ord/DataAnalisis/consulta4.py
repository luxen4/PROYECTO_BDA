
jdbc_url = "jdbc:postgresql://spark-database-1:5432/primord"
connection_properties = {"user": "primord", "password": "bdaprimord", "driver": "org.postgresql.Driver"}

def select(spark):
    print("¿Cuántos empleados tiene de media cada hotel?") 
    df = spark.read.jdbc(url=jdbc_url, table="w_hoteles", properties=connection_properties)
    df.createOrReplaceTempView("tabla_spark")
    
    media_empleados = spark.sql("""
        SELECT DISTINCT nombre_hotel, empleados
        FROM tabla_spark
        ORDER BY nombre_hotel ASC
    """)
    
    media_empleados.show()
    


