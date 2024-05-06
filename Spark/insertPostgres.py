from pyspark.sql import SparkSession


def insertarPostgres():
    spark = SparkSession.builder \
    .appName("ReadFromPostgres") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar") \
    .master("spark://spark-master:7077") \
    .config("spark.jars","postgresql-42.7.3.jar") \
    .getOrCreate()

    df_original = spark.read.csv("./../../../spark-data/csv/data_stores.csv", header=True)
    df_original.show()
    
    
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/retail_db"
    connection_properties = { "user": "postgres", "password": "casa1234", "driver": "org.postgresql.Driver"}

    table_name = "stores" 
    # Escribe el DataFrame en la tabla de PostgreSQL
    df_original.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)
    
insertarPostgres()












# Agregar una tupla a la lista
            #data.append((store_id, store_name, location, date, product_id, quantity_sold, revenue))
'''
columns = ["store_id", "store_name", "location", "date", "product_ID", "quantity_sold", "revenue"]
df = spark.createDataFrame(data, columns)


# Convierte la columna 'date' a tipo de dato 'date'
df = df.withColumn("date", to_date(df["date"]))
df.show()
    

# Escribe los datos en la tabla PostgreSQL
jdbc_url = "jdbc:postgresql://spark-database-1:5432/warehouse_retail_db"
connection_properties = {
    "user": "postgres",
    "password": "casa1234",
    "driver": "org.postgresql.Driver"
}
table_name = "ventas"

df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
print(data)
'''