import pandas as pd
import random

df = pd.read_csv("./../habitaciones.csv")  # Leer el archivo CSV
df['id_hotel'] = [random.randint(1, 2000) for _ in range(len(df))]
print(df)
 
df.to_csv("habitaciones2.csv", index=False) # Guardar el DataFrame en un csv