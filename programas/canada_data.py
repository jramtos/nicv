# Canada NICV

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

CANADA_URL = 'https://health-infobase.canada.ca/src/data/covidLive/covid19-download.csv'


def get_can_data():

    # Bajar los datos
    r = requests.get(CANADA_URL)
    can = pd.read_csv(io.StringIO(r.content.decode('utf-8')))

    # Obtener solo regiones(estados) de Canada
    can_regions = can[(can.prname != 'Canada') & (can.prname != 'Repatriated travellers')][['prname', 'date',
                                                                                            'ratetotal_last14']]

    # Recalcular NICV
    can_regions['nicv'] = can_regions['ratetotal_last14']/100000*1000

    can_regions['date'] = pd.to_datetime(can_regions['date'])
    can_regions['location'] = can_regions['prname'] + ', Canada'
    can_regions = can_regions.pivot(
        index='date', columns='location', values='nicv').reset_index()

    # Filtrar solo fechas de interes
    can_regions = can_regions[can_regions.date >
                              dt.datetime.today() - dt.timedelta(days=368)]

    output_file_path = os.path.join(OUTPUT_DIRECTORY, '4-1-NICV-Estados-Canadá.csv')
    can_regions.to_csv(output_file_path, index=False)
    #print(can_regions)


if __name__ == "__main__":
    get_can_data()
