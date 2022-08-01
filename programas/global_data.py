# Global NICV

# Author: Jesica Ramirez

import datetime as dt
import io
import pandas as pd
import requests
import os
customer_lib_dir = '/home/customer/lib'
if os.path.isdir(customer_lib_dir):
    import sys
    sys.path.insert(0, '/home/customer/lib')

customer_lib_dir = '/home/customer/lib'
if os.path.isdir(customer_lib_dir):
    import sys
    sys.path.insert(0, '/home/customer/lib')


OUTPUT_DIRECTORY = os.path.join(os.pardir, "archivos")

OUTPUT_DIRECTORY = os.path.join(os.pardir, "archivos")

GLOBAL_BIWEEKLY = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/biweekly_cases_per_million.csv'


def get_data():
    '''
    Obtain all biweekly cases and calculate NICV for all Countries. 

    Returns: DataFrame
    '''
    # Obtain information
    r = requests.get(GLOBAL_BIWEEKLY)
    rawCases = pd.read_csv(io.StringIO(r.content.decode('utf-8')))

    # Convertir a un formato manipulable
    raw_long = pd.melt(rawCases, id_vars=[
                       'date'], value_vars=rawCases.columns[1:])
    raw_long.rename(columns={'variable': 'location', 'value': 'biweekly_million'},
                    inplace=True)
    # Computation of NICV
    raw_long['nicv'] = raw_long['biweekly_million']*1000/1000000

    # Filtrar columnas relevantes y cambiar formato
    global_csv = raw_long[['date', 'location', 'nicv']]
    global_csv = global_csv.rename(columns={'location': 'Country'})

    # Quitar Summer Olympics
    global_csv = global_csv[(global_csv.Country != '2020 Summer Olympics athletes & staff') &
                            (global_csv.Country != 'International')]

    # Poner World al principio
    global_csv = global_csv.loc[global_csv.Country == 'World'].append(
        global_csv).drop_duplicates().reset_index(drop=True)

    # Filtrar solo fechas deseadas
    global_csv['date'] = pd.to_datetime(global_csv['date'])
    end = dt.datetime.today()
    start = end - dt.timedelta(days=368)

    global_csv_final = global_csv[global_csv['date'] >= start].pivot(
        index='date', columns='Country', values=['nicv'])

    global_csv_final.columns = global_csv_final.columns.droplevel()
    global_csv_final.reset_index(inplace=True)
    global_csv_final.columns.name = ''
    global_csv_final = global_csv_final.round(4)

    # Guardar archivo como csv
    global_csv_final.to_csv(os.path.join(OUTPUT_DIRECTORY, '1-1-NICV-World.csv'),
                            index=False)

    # Print result
    # print('Data has: {} rows and {} columns'.format(
    #     global_csv_final.shape[0], global_csv_final.shape[1]))
    # print(global_csv_final)


if __name__ == "__main__":
    get_data()
