import mysql.connector

conexion = mysql.connector.connect( host="localhost",user="root",password="alberite",database="primord_db")
cursor = conexion.cursor()

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

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

print("Table 'GASTOS' creada exitosamente.")