import requests, sqlite3, glob, os
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def obtener_html_alturas(url="https://www.hidro.gov.ar/Oceanografia/AlturasHorarias.asp"):
    """Descarga y parsea la página de alturas horarias."""
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser")
    return soup

def parsear_tabla_alturas(soup):
    """Parsea la tabla de alturas horarias y devuelve un DataFrame."""
    tabla = soup.find("table", class_="table-striped")
    columnas = tabla.find("thead").find_all("th")[2:]
    fechas_horas = [th.get_text(strip=False).replace("\n", "").replace("\r", "") for th in columnas]

    filas = tabla.find("tbody").find_all("tr")
    data = []

    for fila in filas:
        celdas = fila.find_all("td")
        if len(celdas) < 2:
            continue
        estacion = celdas[1].get_text(strip=True).replace("(*)", "").replace("(**)", "").replace("(***)", "")
        alturas = [celda.get_text(strip=True).replace(",", ".") for celda in celdas[2:]]
        for fecha_hora, altura in zip(fechas_horas, alturas):
            data.append([estacion, fecha_hora, altura])

    df = pd.DataFrame(data, columns=["Mareografo", "Fecha", "Altura"])
    df["Altura"] = pd.to_numeric(df["Altura"], errors="coerce")
    df["Fecha"] = pd.to_datetime(df['Fecha'], format=" %d/%m/%Y %H:%M")#
    return df

def guardar_csv_t1(df, carpeta_destino="descargas"):
    """Guarda el DataFrame a un CSV con fecha y hora actual en el nombre."""
    ahora = datetime.now().strftime("%Y-%m-%d_%H")
    nombre_archivo = f"{carpeta_destino}/alturas_horarias_{ahora}.csv"
    df.to_csv(nombre_archivo, index=False, encoding="ISO-8859-1")
    print(f"CSV guardado: {nombre_archivo}")

def obtener_html_pronostico(url="https://www.hidro.gov.ar/oceanografia/pronostico.asp"):
    """Descarga y parsea la página de pronóstico de mareas."""
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser")
    return soup

def extraer_tabla(tabla_html):
    """Procesa una tabla de pronóstico y devuelve una lista de filas."""
    filas = tabla_html.find("tbody").find_all("tr")
    datos = []
    lugar_actual = ""
    for fila in filas:
        celdas = fila.find_all("td")
        if not celdas:
            continue
        lugar = celdas[0].get_text(strip=True)
        if lugar:
            lugar_actual = lugar
        estado = celdas[1].get_text(strip=True)
        hora = celdas[2].get_text(strip=True)
        altura = celdas[3].get_text(strip=True).replace(",", ".")
        fecha = celdas[4].get_text(strip=True)
        try:
            altura_float = float(altura)
        except ValueError:
            altura_float = None
        datos.append([lugar_actual, estado, hora, altura_float, fecha])
    return datos

def procesar_tablas(soup):
    """Extrae las dos tablas (INTERIOR y EXTERIOR) y devuelve un DataFrame combinado."""
    tablas = soup.find_all("table", class_="table-striped")
    datos_interior = extraer_tabla(tablas[0])
    datos_exterior = extraer_tabla(tablas[1])

    nom_Col = ["Lugar", "Estado", "Hora", "Altura", "Dia"]
    df_interior = pd.DataFrame(datos_interior, columns=nom_Col)
    df_exterior = pd.DataFrame(datos_exterior, columns=nom_Col)

    df_unido = pd.concat([df_interior, df_exterior], ignore_index=True)
    df_unido['Fecha'] = pd.to_datetime(df_unido['Dia']+' '+df_unido['Hora'],format="%d/%m/%Y %H:%M",errors='coerce')
    df_unido = df_unido.drop(columns=["Dia", "Hora"])
    return df_unido

def guardar_csv_t2(df, carpeta_destino="descargas"):
    """Guarda el DataFrame como CSV con la hora actual en el nombre."""
    ahora = datetime.now().strftime("%Y-%m-%d_%H")
    nombre_archivo = f"{carpeta_destino}/pronostico_mareas_{ahora}.csv"
    ahora = datetime.now().strftime("%Y-%m-%d %H")
    df["Fecha_Prono"] = ahora
    df.to_csv(nombre_archivo, index=False, encoding="ISO-8859-1")
    print(f"CSV guardado: {nombre_archivo}")

def actualizar_base_mareas(carpeta_csvs="descargas", db_name="prono_shn.db"):
    # Buscar CSVs de alturas horarias
    archivos = glob.glob(os.path.join(carpeta_csvs, "alturas_horarias_*.csv"))
    # Leer y concatenar
    dfs = [pd.read_csv(archivo,encoding="ISO-8859-1") for archivo in archivos]
    df_total = pd.concat(dfs, ignore_index=True)

    # Eliminar duplicados por mareógrafo + fecha-hora
    df_total.drop_duplicates(subset=["Mareografo", "Fecha"], inplace=True)

    # Conectar a SQLite
    conn = sqlite3.connect(db_name)
    df_total.to_sql("alturas_horarias", conn, if_exists="append", index=False)

    # Opcional: eliminar duplicados a nivel SQL también (solo si se repiten por error)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alturas_horarias_unica AS
        SELECT DISTINCT * FROM alturas_horarias
    """)
    conn.execute("DROP TABLE alturas_horarias")
    conn.execute("ALTER TABLE alturas_horarias_unica RENAME TO alturas_horarias")
    conn.commit()
    conn.close()

    print(f"Base SQLite '{db_name}' actualizada con {len(df_total)} registros únicos.")

def actualizar_base_pronosticos(carpeta_csv="descargas", db_path="prono_shn.db"):
    archivos_csv = glob.glob(os.path.join(carpeta_csv, "pronostico_mareas_*.csv"))

    dfs = [pd.read_csv(archivo,encoding="ISO-8859-1") for archivo in archivos_csv]
    df_total = pd.concat(dfs, ignore_index=True)

    df_total = df_total.drop_duplicates(subset=["Lugar", "Fecha","Fecha_Prono"])

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pronosticos_mareas (
        Lugar TEXT,
        Estado TEXT,
        Altura REAL,
        Fecha TEXT,
        Fecha_Prono TEXT,
        PRIMARY KEY (Lugar, Fecha, Fecha_Prono)
    )
    """)
    # Leer claves existentes en la base
    claves_existentes = pd.read_sql_query(
        "SELECT Lugar, Fecha, Fecha_Prono FROM pronosticos_mareas", conn
    )

    # Unir con el DataFrame a insertar para filtrar duplicados
    df_merge = df_total.merge(claves_existentes, on=["Lugar", "Fecha", "Fecha_Prono"], how="left", indicator=True)
    df_filtrado = df_merge[df_merge["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_filtrado.empty:
        try:
            df_filtrado.to_sql("pronosticos_mareas", conn, if_exists="append", index=False)
            print("Base de datos actualizada correctamente.")
        except Exception as e:
            print(f"Error al insertar en la base de datos: {e}")
    else:
        print("No hay nuevos registros para insertar.")

    conn.commit()
    conn.close()

def main():
    soup1 = obtener_html_alturas()
    df1 = parsear_tabla_alturas(soup1)
    guardar_csv_t1(df1)

    soup2 = obtener_html_pronostico()
    df_2 = procesar_tablas(soup2)
    guardar_csv_t2(df_2)

    actualizar_base_mareas()
    actualizar_base_pronosticos()

# if __name__ == "__main__":
#     main()


import schedule
import time

for hora in [1,7,13,19]:
    hora_formateada = f"{hora:02d}:15"
    print(hora_formateada)
    schedule.every().day.at(hora_formateada).do(main)

while True:
    schedule.run_pending()
    time.sleep(60)  # Revisar cada minuto