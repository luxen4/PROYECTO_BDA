from faker import Faker
import pandas as pd
import random

# Inicializar Faker
faker = Faker()

# Leer el archivo CSV
df = pd.read_csv("habitaciones.csv")  # Reemplaza "ruta_del_archivo.csv" con la ruta real del archivo

# Generar nombres de restaurantes para la columna restaurante_name
df['restaurante_name'] = [faker.company() for _ in range(len(df))]

# Mostrar el DataFrame resultante
print(df)

# Guardar el DataFrame resultante en un nuevo archivo CSV
df.to_csv("habitaciones3.csv", index=False)  # Reemplaza "nuevo_archivo.csv" con el nombre deseado para el nuevo archivo