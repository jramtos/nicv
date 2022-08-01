# Mexico NICV

# Author: Jesica Ramirez

import os
customer_lib_dir = '/home/customer/lib'
if os.path.isdir(customer_lib_dir):
    import sys
    sys.path.insert(0, '/home/customer/lib')

import requests
import pandas as pd
import datetime as dt
import io

OUTPUT_DIRECTORY = os.path.join(os.pardir, "archivos")

MEX_URL = 'https://datos.covid-19.conacyt.mx/Downloads/Files/Casos_Diarios_Municipio_Confirmados_{}.csv'
CLAVES_ENTIDADES = {1: 'Aguascalientes', 2: 'Baja California', 3: 'Baja California Sur',
                    4: 'Campeche', 5: 'Coahuila', 6: 'Colima', 7: 'Chiapas', 8: 'Chihuahua',
                    9: 'Ciudad de México', 10: 'Durango', 11: 'Guanajuato', 12: 'Guerrero',
                    13: "Hidalgo", 14: 'Jalisco', 15: 'Estado de México', 16: 'Michoacán',
                    17: 'Morelos', 18: 'Nayarit', 19: 'Nuevo León', 20: 'Oaxaca',
                    21: 'Puebla', 22: 'Querétaro', 23: 'Quintana Roo', 24: 'San Luis Potosí',
                    25: 'Sinaloa', 26: 'Sonora', 27: 'Tabasco', 28: 'Tamaulipas',
                    29: 'Tlaxcala', 30: 'Veracruz', 31: 'Yucatán', 32: 'Zacatecas'}


def get_mex_data():

    # Bajar los datos
    if dt.datetime.now().hour >= 21:
        days = 0
    else:
        days = 1
    today_minus_1 = dt.datetime.today() - dt.timedelta(days=days)
    r = requests.get(MEX_URL.format(today_minus_1.strftime('%Y%m%d')))
    mex = pd.read_csv(io.StringIO(r.content.decode('utf-8')))

    # Ponerlos en una tabla y obtener el nombre del Estado
    mex = pd.melt(mex, id_vars=mex.columns[:3], value_vars=mex.columns[3:])
    mex.rename(columns={'variable': 'date', 'value': 'daily'}, inplace=True)
    mex['state'] = mex.apply(
        lambda x: CLAVES_ENTIDADES[x['cve_ent']//1000], axis=1)
    mex['location'] = mex['nombre'] + ", " + mex['state']+', Mexico'
    mex['date'] = pd.to_datetime(mex['date'], format='%d-%m-%Y')
    mex = mex.sort_values(by=['cve_ent', 'date']).reset_index(drop=True)

    # Calcular casos diarios acumluados
    mex['confirmed'] = mex.groupby(['cve_ent']).daily.cumsum()

    # Calcular NICV para municipios
    rolling14 = mex.groupby(['cve_ent'])\
        .apply(lambda x:
               x.set_index('date')).rolling(14)['confirmed'].apply(lambda x: x[-1] - x[0],
                                                                   raw=True).reset_index()
    rolling14.index = mex.index
    mex['last14'] = rolling14['confirmed']
    mex['nicv'] = mex['last14']*1000/(mex['poblacion'].astype(float))
    municipios = mex[['location', 'date', 'nicv']].pivot(index='date',
                                                         columns='location',
                                                         values='nicv').reset_index()
    municipios = municipios[municipios['date'] >
                            dt.datetime.today() - dt.timedelta(days=367)]
    output_file_path = os.path.join(OUTPUT_DIRECTORY, '3-20-1-NICV-Municipios-México.csv')
    municipios.to_csv(output_file_path, index=False)
    # print('Municipios')
    # print(municipios)

    # Calcular NICV para Estados
    states = mex.groupby(['state', 'date']).agg({'confirmed': 'sum',
                                                 'poblacion': 'sum'}).reset_index()
    rolling14 = states.groupby(['state'])\
        .apply(lambda x:
               x.set_index('date')).rolling(14)['confirmed'].apply(lambda x: x[-1] - x[0],
                                                                   raw=True).reset_index()
    rolling14.index = states.index
    states['last14'] = rolling14['confirmed']
    states['nicv'] = states['last14']*1000/(states['poblacion'].astype(float))
    states = states[['state', 'date', 'nicv']].pivot(index='date',
                                                     columns='state',
                                                     values='nicv').reset_index()
    states = states[states['date'] >
                    dt.datetime.today() - dt.timedelta(days=367)]
    output_file_path = os.path.join(OUTPUT_DIRECTORY, '3-1-NICV-Estados-México.csv')
    states.to_csv(output_file_path, index=False)
    # print('Estados de Mexico')
    # print(states)


if __name__ == "__main__":
    get_mex_data()
