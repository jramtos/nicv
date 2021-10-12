# United States NICV

# Author: Jesica Ramirez

import requests
import pandas as pd
import datetime as dt
import io

url_county_us = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{}.csv"


def get_us_data():
    # Establecer Fechas (Hoy hasta menos un año)
    end = dt.datetime.today()
    step = dt.timedelta(days=1)
    start = end - dt.timedelta(days=369+15)
    dates = []
    while start < end:
        dates.append(start.strftime('%m-%d-%Y'))
        print(start.strftime('%m-%d-%Y'))
        start += step

    # Bajar cada hoja de información
    bigraw = None
    for i, date in enumerate(dates[:-1]):
        r = requests.get(url_county_us.format(date))
        rawData = pd.read_csv(io.StringIO(r.content.decode('utf-8')))
        rawData = rawData[rawData.Country_Region == 'US'][['FIPS', 'Admin2',
                                                           'Province_State', 'Country_Region', 'Combined_Key',
                                                           'Lat', 'Long_', 'Confirmed']]
        rawData['date'] = date
        if i == 0:
            bigraw = rawData
        else:
            bigraw = bigraw.append(rawData)

    # Bajar información de Población
    r = requests.get(
        "https://api.census.gov/data/2019/pep/charagegroups?get=NAME,POP&for=county:*")
    populations = pd.DataFrame(r.json()[1:], columns=r.json()[0])
    populations['FIPS'] = (populations['state'] +
                           populations['county']).astype(float).astype(str)
    populations['POP'] = populations['POP'].astype(float)

    bigraw['FIPS'] = bigraw.groupby(['Combined_Key'])['FIPS'].transform(lambda x:
                                                                        x.fillna(x.astype(float).min())).astype(str)

    # Juntar todo y calcular 14 dias
    big = bigraw.merge(populations)
    big['date'] = pd.to_datetime(big['date'])
    big = big.sort_values(by=['Combined_Key', 'date']).reset_index(drop=True)
    big.rename(columns={'Admin2': 'County',
                        'Province_State': 'State'}, inplace=True)

    # States
    states = big.groupby(['State', 'date']).agg(
        {'Confirmed': 'sum', 'POP': 'sum'}).reset_index()
    vals = states.groupby(['State']).apply(lambda x:
                                           x.set_index('date')).rolling(14)['Confirmed'].apply(lambda x: x[-1] - x[0],
                                                                                               raw=True).reset_index()['Confirmed']
    states['last14'] = vals
    states['nicv'] = states['last14']/states['POP']*1000
    states = states.pivot(index='date', columns='State',
                          values='nicv').reset_index()
    states = states[states['date'] > end - dt.timedelta(days=369)]
    states.to_csv('2-1-NICV-Estados-USA.csv', index=False)
    print('Estados de USA')
    print(states)

    # Counties
    big = big[big.County != 'Unassigned']
    counties = big.groupby(['Combined_Key', 'date']).agg(
        {'Confirmed': 'sum', 'POP': 'sum'}).reset_index()
    vals = counties.groupby(['Combined_Key']).apply(lambda x:
                                                    x.set_index('date'))\
        .rolling(14)['Confirmed'].apply(lambda x: x[-1] - x[0],
                                        raw=True).reset_index()['Confirmed']
    counties['last14'] = vals
    counties['nicv'] = counties['last14']/counties['POP']*1000
    counties = counties.pivot(
        index='date', columns='Combined_Key', values='nicv').reset_index()
    counties = counties[counties['date'] > end - dt.timedelta(days=369)]
    counties.to_csv('2-1-NICV-Counties-USA.csv', index=False)
    print('Counties de USA')
    print(counties)


if __name__ == "__main__":
    get_us_data()
