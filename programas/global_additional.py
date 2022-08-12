# Global Additional NICV

# Author: Jesica Ramirez

import pandas as pd
import os
customer_lib_dir = '/home/customer/lib'
if os.path.isdir(customer_lib_dir):
    import sys
    sys.path.insert(0, '/home/customer/lib')


OUTPUT_DIRECTORY = os.path.join(os.pardir, "archivos")


def pull_and_format_data():
    '''
    Pulls world data and pushes new data files to the web storage
    '''
    global_csv = pd.read_csv(os.path.join(
        OUTPUT_DIRECTORY, '1-1-NICV-World.csv'))

    # Clean
    global_csv.drop(columns=['id'], inplace=True)
    global_csv['date'] = pd.to_datetime(global_csv['date'])

    # First File
    one_record_world_file = pd.DataFrame(
        global_csv[['date', 'World']].iloc[-1, :]).T
    one_record_world_file.to_csv(os.path.join(OUTPUT_DIRECTORY, 'N-World-001.csv'),
                                 index=False)

    # Second File
    all_records_world_file = global_csv[['date', 'World']]
    all_records_world_file.to_csv(os.path.join(OUTPUT_DIRECTORY, '1-World/N-World-A.csv'),
                                  index=False)

    # Third File
    all_countries_transposed = global_csv.set_index('date').T

    cols_needed = [all_countries_transposed.columns[-1] - pd.DateOffset(years=1),
                   all_countries_transposed.columns[-1] -
                   pd.DateOffset(months=9),
                   all_countries_transposed.columns[-1] -
                   pd.DateOffset(months=6),
                   all_countries_transposed.columns[-1] -
                   pd.DateOffset(months=3),
                   all_countries_transposed.columns[-1] -
                   pd.DateOffset(months=2),
                   all_countries_transposed.columns[-1] -
                   pd.DateOffset(months=1),
                   all_countries_transposed.columns[-15],
                   all_countries_transposed.columns[-8],
                   all_countries_transposed.columns[-1]]

    try:
        all_countries_file = all_countries_transposed[cols_needed]
    except:
        cols_needed = [all_countries_transposed.columns[0],
                       all_countries_transposed.columns[-271],
                       all_countries_transposed.columns[-181],
                       all_countries_transposed.columns[-91],
                       all_countries_transposed.columns[-61],
                       all_countries_transposed.columns[-31],
                       all_countries_transposed.columns[-15],
                       all_countries_transposed.columns[-8],
                       all_countries_transposed.columns[-1]
                       ]
        all_countries_file = all_countries_transposed[cols_needed]

    all_countries_file.index.name = 'date'
    all_countries_file.reset_index().to_csv(os.path.join(OUTPUT_DIRECTORY, 'World-V-Tabla.csv'),
                                            index=False)


if __name__ == "__main__":
    pull_and_format_data()
