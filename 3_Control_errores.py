import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import timedelta
import numpy as np


guarda_csv = False
figura = False


# Leer tablas de la base de datos SQLite
conn = sqlite3.connect('prono_shn.db')

observados_completo = pd.read_sql_query("SELECT * FROM alturas_horarias", conn, parse_dates=['Fecha'])
pronosticos_completo = pd.read_sql_query("SELECT * FROM pronosticos_mareas", conn, parse_dates=['Fecha'])
conn.close()

# Estacion_m = 'Buenos  Aires'
# Lugar = 'PUERTO DE BUENOS AIRES (Dársena F)'

Dic_Estaciones = {'Buenos  Aires':'PUERTO DE BUENOS AIRES (Dársena F)',
                  'La Plata':'PUERTO LA PLATA',
                  'Oyarvide':'CANAL PUNTA INDIO (Oyarvide - Km 133)',
                  'San Fernando':'SAN FERNANDO'}
for est_met in Dic_Estaciones.keys():
    est_prono = Dic_Estaciones[est_met]

    observados = observados_completo[observados_completo['Mareografo'] == est_met]
    observados = observados.rename(columns={"Altura": "Altura_observada"})
    del observados['Mareografo']
    observados["Fecha"] = pd.to_datetime(observados["Fecha"])

    xmin = observados['Fecha'].min() - timedelta(hours=6)
    xmax = observados['Fecha'].max() + timedelta(hours=24)

    pronosticos_completo = pronosticos_completo.sort_values(by='Fecha')
    pronosticos = pronosticos_completo[pronosticos_completo['Lugar'] == est_prono]
    pronosticos = pronosticos.drop_duplicates(subset=['Altura', 'Fecha']).dropna(subset=['Altura', 'Fecha'])
    pronosticos = pronosticos[['Fecha','Altura','Fecha_Prono']]
    pronosticos = pronosticos.rename(columns={"Altura": "Altura_pronosticada"})
    pronosticos["Fecha"] = pd.to_datetime(pronosticos["Fecha"])
    pronosticos["Fecha_Prono"] = pd.to_datetime(pronosticos["Fecha_Prono"])
    pronosticos = pronosticos[pronosticos["Fecha"]<=observados['Fecha'].max()]


    # Union
    df_total = pd.merge(pronosticos, observados, on="Fecha", how="outer")
    df_total = df_total.set_index("Fecha")
    df_total = df_total.sort_index()  
    df_total["Altura_observada"] = df_total["Altura_observada"].interpolate(method="time", limit_direction="both")
    df_total = df_total.dropna()
    df_total = df_total.reset_index()

    df_total["Anticipacion_hs"] = (df_total["Fecha"] - df_total["Fecha_Prono"]).dt.total_seconds() / 3600

    # Calcular errores
    df_total["Error_absoluto"] = (df_total["Altura_observada"] - df_total["Altura_pronosticada"]).abs()
    df_total["Error_relativo_%"] = 100 * (df_total["Altura_observada"] - df_total["Altura_pronosticada"]) / df_total["Altura_observada"]
    df_total = df_total.dropna(subset=["Altura_observada"])

    df_total = df_total[df_total["Anticipacion_hs"] >= 0].copy()
    df_total = df_total.sort_values("Fecha_Prono").drop_duplicates(subset="Fecha", keep="first")
    df_total = df_total.sort_values("Fecha")

    # print(df_total)

    rmse = np.sqrt(((df_total["Altura_pronosticada"] - df_total["Altura_observada"]) ** 2).mean())

    # 2. % que supera 20 cm de error absoluto
    errores_abs = (df_total["Altura_pronosticada"] - df_total["Altura_observada"]).abs()
    porcentaje_mayor_20cm = (errores_abs > 0.20).mean() * 100

    # 3. Valor superado el 3% del tiempo (percentil 97)
    percentil_97 = np.percentile(errores_abs, 97)

    # Mostrar resultados
    print(est_met)
    print(f"RMSE: {rmse:.3f} m")
    print(f"% de errores mayores a 20 cm: {porcentaje_mayor_20cm:.2f}%")
    print(f"Error absoluto superado el 3% del tiempo: {percentil_97:.3f} m")
    print('-----------------------------------------------------------------------------------')

    if guarda_csv:
        df_total.to_csv('PronosError_'+est_met+'.csv')

    if figura:    
        plt.figure(figsize=(12, 6))
        observados = observados.sort_values(by='Fecha')
        plt.plot(observados['Fecha'], observados['Altura_observada'], label=f'Alturas SHN - {est_met}', linestyle='--')
        # plt.scatter(df_total['Fecha'], df_total['Altura'], label=f'Correccion - {Lugar}',color='g')
        plt.scatter(df_total['Fecha'], df_total['Altura_observada'], label='Altura_observada',color='b')
        plt.scatter(df_total['Fecha']   , df_total['Altura_pronosticada'], label='Altura_pronosticada',color='r')

        plt.xlabel('Fecha')
        plt.ylabel('Nivel [m]')
        plt.xlim(xmin, xmax)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()
