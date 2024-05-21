import mysql.connector

conexion = mysql.connector.connect( host="localhost",user="user1",password="alberite",database="retail_db")
cursor = conexion.cursor()


def dropTablaGastos():
    cursor = conexion.cursor()
    sql = "DROP TABLE IF EXISTS gastos;"
    cursor.execute(sql)  # Ejecutar la consulta SQL
    conexion.commit()    # Confirmar los cambios
    print("Table 'GASTOS' ELIMINADA exitosamente.")



def crearTablaGastos():
    sql = """
        CREATE TABLE IF NOT EXISTS gastos(
            id INT AUTO_INCREMENT PRIMARY KEY,
            fecha Date,
            id_hotel INT,
            concepto VARCHAR(100),
            monto DECIMAL (10.2),
            pagado VARCHAR(3)
        );
        """

    # fecha,id_hotel,concepto,monto,pagado

    # email VARCHAR(255) NOT NULL,
    # bird_date DATE
    # created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    cursor.execute(sql)
    conexion.commit()
    
    cursor.close()      # Cerrar el cursor y la conexión
    conexion.close()

    print("Table 'GASTOS' creada exitosamente.")
    
dropTablaGastos()
crearTablaGastos()