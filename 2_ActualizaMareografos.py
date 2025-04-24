import pandas as pd
import sqlite3
import glob

# 1. Ruta a los archivos CSV
archivos = glob.glob("completa_mareografos/*.csv")

# 2. Leer y unir todos los CSV
df = pd.concat([pd.read_csv(archivo) for archivo in archivos], ignore_index=True)
df = df.rename(columns={"Nivel": "Altura"})
df['Fecha'] = pd.to_datetime(df['Fecha'])
df['Fecha'] = df['Fecha'].dt.tz_localize(None)

# 3. Conectar a la base de datos
conn = sqlite3.connect("prono_shn.db")

claves_existentes = pd.read_sql_query(
    "SELECT Mareografo, Fecha FROM alturas_horarias", conn)
claves_existentes['Fecha'] = pd.to_datetime(claves_existentes['Fecha'])

df_nuevos = df.merge(
    claves_existentes, on=["Mareografo", "Fecha"], how="left", indicator=True)
df_nuevos = df_nuevos[df_nuevos["_merge"] == "left_only"].drop(columns=["_merge"])

# 4. Guardar los datos en la tabla
df_nuevos.to_sql("alturas_horarias", conn, if_exists="append", index=False)

# 5. Cerrar conexión
conn.close()
