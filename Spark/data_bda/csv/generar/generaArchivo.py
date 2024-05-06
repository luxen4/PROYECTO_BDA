import pandas as pd
import random

# Leer el archivo CSV
df = pd.read_csv("habitaciones.csv")  # Reemplaza "ruta_del_archivo.csv" con la ruta real del archivo

# Generar valores aleatorios para la columna id_hotel
df['id_hotel'] = [random.randint(1, 2000) for _ in range(len(df))]

# Mostrar el DataFrame resultante
print(df)

# Guardar el DataFrame resultante en un nuevo archivo CSV
df.to_csv("habitaciones2.csv", index=False)  # Reemplaza "nuevo_archivo.csv" con el nombre deseado para el nuevo archivo