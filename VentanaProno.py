import sqlite3
import pandas as pd

# Conectar a la base de datos
conn = sqlite3.connect("prono_shn.db")

# Leer los datos de la tabla
df = pd.read_sql_query("SELECT * FROM pronosticos_mareas", conn)

# Cerrar la conexión (opcional si no vas a hacer más consultas)
conn.close()

# Convertir a datetime
df["Fecha"] = pd.to_datetime(df["Fecha"])
df["Fecha_Prono"] = pd.to_datetime(df["Fecha_Prono"])

# Calcular la diferencia en horas
df["Diferencia_horas"] = (df["Fecha"] - df["Fecha_Prono"]).dt.total_seconds() / 3600

# Mostrar el resultado
df.to_csv('pronosticos.csv',index=False)
