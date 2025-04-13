import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import timedelta

Estacion = 'BuenosAires'
Estacion_m = 'Buenos  Aires'
# Leer CSV
df_marea = pd.read_csv('Tmareas/'+Estacion+'.csv')
df_marea['dia'] = df_marea['dia'].ffill().astype('int').astype('string')
df_marea['mes'] = df_marea['mes'].astype('string')
df_marea['fecha'] = pd.to_datetime(df_marea['dia']+'/'+df_marea['mes']+'/2025'+' '+df_marea['hora'],format="%d/%m/%Y %H:%M")


# Leer tablas de la base de datos SQLite
conn = sqlite3.connect('prono_shn.db')

df_alturas = pd.read_sql_query("SELECT * FROM alturas_horarias", conn, parse_dates=['Fecha'])
df_pronos = pd.read_sql_query("SELECT * FROM pronosticos_mareas", conn, parse_dates=['Fecha'])
conn.close()

Estacion_m = 'Buenos  Aires'
Lugar = 'PUERTO DE BUENOS AIRES (Dársena F)'

df_alturas = df_alturas[df_alturas['Mareografo'] == Estacion_m]
df_alturas = df_alturas.sort_values(by='Fecha')
df_pronos = df_pronos[df_pronos['Lugar'] == Lugar]


xmin = df_alturas['Fecha'].min() - timedelta(hours=6)
xmax = df_alturas['Fecha'].max() + timedelta(hours=24)

df_marea = df_marea[['fecha','altura']]
df_marea = df_marea.rename(columns={'fecha':'Fecha','altura':'Marea'})

df_pronos = df_pronos.drop_duplicates(subset=['Altura', 'Fecha']).dropna(subset=['Altura', 'Fecha'])
df_pronos = df_pronos[['Fecha','Altura','Fecha_Prono']]

print(df_marea.head())
print(df_pronos.head())


df_prono_corr = pd.merge(df_marea, df_pronos, on='Fecha', how='outer')
df_prono_corr = df_prono_corr.sort_values('Fecha')
df_prono_corr.set_index('Fecha', inplace=True)
df_prono_corr['Marea'] = df_prono_corr['Marea'].interpolate(method='time')

df_prono_corr['Prono'] = df_prono_corr[['Altura', 'Marea']].sum(axis=1, skipna=False)
df_prono_corr = df_prono_corr.dropna()


plt.figure(figsize=(12, 6))
plt.plot(df_marea['Fecha'], df_marea['Marea'], label='Marea', linestyle='-')
plt.plot(df_alturas['Fecha'], df_alturas['Altura'], label=f'Alturas SHN - {Estacion_m}', linestyle='--')
plt.scatter(df_pronos['Fecha'], df_pronos['Altura'], label=f'Correccion - {Lugar}',color='g')
plt.scatter(df_prono_corr.index, df_prono_corr['Prono'], label=f'Marea + Correccion - {Lugar}',color='r')


plt.xlabel('Fecha')
plt.ylabel('Nivel [m]')
plt.xlim(xmin, xmax)
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()










