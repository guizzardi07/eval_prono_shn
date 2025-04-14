import os
import glob
import logging
import sqlite3
from datetime import datetime

import requests
import pandas as pd
from bs4 import BeautifulSoup

# Configuración general
URL_ALTURAS = "https://www.hidro.gov.ar/Oceanografia/AlturasHorarias.asp"
URL_PRONOSTICO = "https://www.hidro.gov.ar/oceanografia/pronostico.asp"
CARPETA_DESCARGAS = "descargas"
DB_NAME = "prono_shn.db"
ENCODING = "ISO-8859-1"

# Setup de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

os.makedirs(CARPETA_DESCARGAS, exist_ok=True)

def obtener_html(url):
    try:
        res = requests.get(url)
        res.encoding = "utf-8"
        return BeautifulSoup(res.text, "html.parser")
    except Exception as e:
        logging.error(f"Error obteniendo HTML desde {url}: {e}")
        return None

def parsear_tabla_alturas(soup):
    tabla = soup.find("table", class_="table-striped")
    if not tabla:
        raise ValueError("No se encontró la tabla de alturas horarias.")

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
    df["Fecha"] = pd.to_datetime(df['Fecha'], format=" %d/%m/%Y %H:%M", errors="coerce")
    return df

def guardar_csv(df, nombre_base):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H")
    nombre_archivo = f"{CARPETA_DESCARGAS}/{nombre_base}_{timestamp}.csv"
    df.to_csv(nombre_archivo, index=False, encoding=ENCODING)
    logging.info(f"CSV guardado: {nombre_archivo}")

def extraer_tabla(tabla_html):
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

def procesar_tablas_pronostico(soup):
    tablas = soup.find_all("table", class_="table-striped")
    if len(tablas) < 2:
        raise ValueError("No se encontraron ambas tablas de pronóstico.")

    datos_interior = extraer_tabla(tablas[0])
    datos_exterior = extraer_tabla(tablas[1])

    columnas = ["Lugar", "Estado", "Hora", "Altura", "Dia"]
    df = pd.concat([
        pd.DataFrame(datos_interior, columns=columnas),
        pd.DataFrame(datos_exterior, columns=columnas)
    ], ignore_index=True)

    df['Fecha'] = pd.to_datetime(df['Dia'] + ' ' + df['Hora'], format="%d/%m/%Y %H:%M", errors='coerce')
    df.drop(columns=["Dia", "Hora"], inplace=True)
    df["Fecha_Prono"] = datetime.now().strftime("%Y-%m-%d %H")
    return df

def actualizar_base_alturas():
    archivos = glob.glob(os.path.join(CARPETA_DESCARGAS, "alturas_horarias_*.csv"))
    df_total = pd.concat([pd.read_csv(f, encoding=ENCODING) for f in archivos], ignore_index=True)
    df_total.drop_duplicates(subset=["Mareografo", "Fecha"], inplace=True)

    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alturas_horarias (
            Mareografo TEXT,
            Fecha TEXT,
            Altura REAL,
            PRIMARY KEY (Mareografo, Fecha))""")

    claves_existentes = pd.read_sql_query(
        "SELECT Mareografo, Fecha FROM alturas_horarias", conn
    )

    # Filtrar registros nuevos
    df_nuevos = df_total.merge(
        claves_existentes, on=["Mareografo", "Fecha"], how="left", indicator=True)
    df_nuevos = df_nuevos[df_nuevos["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_nuevos.empty:
        try:
            df_nuevos.to_sql("alturas_horarias", conn, if_exists="append", index=False)
            print(f"Se insertaron {len(df_nuevos)} registros nuevos en la base '{DB_NAME}'.")
        except Exception as e:
            print(f"Error al insertar registros nuevos: {e}")
    else:
        print("No hay registros nuevos para insertar.")

    conn.commit()
    conn.close()

def actualizar_base_pronosticos():
    archivos = glob.glob(os.path.join(CARPETA_DESCARGAS, "pronostico_mareas_*.csv"))
    if not archivos:
        logging.info("No se encontraron archivos CSV de pronóstico para procesar.")
        return
    
    df_total = pd.concat([pd.read_csv(f, encoding=ENCODING) for f in archivos], ignore_index=True)
    df_total.drop_duplicates(subset=["Lugar", "Fecha", "Fecha_Prono"], inplace=True)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pronosticos_mareas (
            Lugar TEXT,
            Estado TEXT,
            Altura REAL,
            Fecha TEXT,
            Fecha_Prono TEXT,
            PRIMARY KEY (Lugar, Fecha, Fecha_Prono))""")
    
    claves_existentes = pd.read_sql_query(
        "SELECT Lugar, Fecha, Fecha_Prono FROM pronosticos_mareas",conn)
    
    df_nuevos = df_total.merge(
        claves_existentes,
        on=["Lugar", "Fecha", "Fecha_Prono"],
        how="left",indicator=True)
    
    df_nuevos = df_nuevos[df_nuevos["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_nuevos.empty:
        try:
            df_nuevos.to_sql("pronosticos_mareas", conn, if_exists="append", index=False)
            logging.info(f"Se insertaron {len(df_nuevos)} nuevos registros en la tabla de pronósticos.")
        except Exception as e:
            logging.error(f"Error insertando nuevos registros: {e}")
    else:
        logging.info("No hay registros nuevos para insertar en la tabla de pronósticos.")

    conn.commit()
    conn.close()
    
def main():
    logging.info("Descargando y procesando alturas horarias...")
    soup_alturas = obtener_html(URL_ALTURAS)
    if soup_alturas:
        df_alturas = parsear_tabla_alturas(soup_alturas)
        guardar_csv(df_alturas, "alturas_horarias")
        actualizar_base_alturas()

    logging.info("Descargando y procesando pronóstico de mareas...")
    soup_pronostico = obtener_html(URL_PRONOSTICO)
    if soup_pronostico:
        df_prono = procesar_tablas_pronostico(soup_pronostico)
        guardar_csv(df_prono, "pronostico_mareas")
        actualizar_base_pronosticos()

# if __name__ == "__main__":
# main()

import schedule
import time

for hora in [0,6,12,18]:
    hora_formateada = f"{hora:02d}:30"
    print(hora_formateada)
    schedule.every().day.at(hora_formateada).do(main)

while True:
    schedule.run_pending()
    time.sleep(60)  # Revisar cada minuto
