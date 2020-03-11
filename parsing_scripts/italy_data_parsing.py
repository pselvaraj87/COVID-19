import os
import pandas as pd
from datetime import datetime, timedelta

dropbox_path = os.path.join(os.path.expanduser('~'), 'Dropbox (IDM)', 'COVID-19', 'timeseries-modeling')


def create_dataframe_for_prior_data(df):

    days = [21, 22, 23]
    dates = [datetime(2020, 1, 23, 18, 0, 0)] + [datetime(2020, 2, d, 18, 0, 0) for d in days]

    regions_to_append = ['Lombardia', 'Veneto', 'Emilia Romagna']
    num_dates_to_append = [4, 3, 2] # number of dates to append
    cases = [[2, 15, 40, 57], [2, 16, 7], [2, 7]]
    df_append = pd.DataFrame()
    for i, region in enumerate(regions_to_append):
        data_dict = {}
        region_dates = dates[4-num_dates_to_append[i]:4]
        data_dict['date'] = region_dates
        data_dict['country'] = ['ITA']*len(region_dates)
        data_dict['region_code'] = [df[df['region_name']==region]['region_code'].unique()[0]]*len(region_dates)
        data_dict['region_name'] = [region] * len(region_dates)
        data_dict['hospitalized_with_symptoms'] = [0] * len(region_dates)
        data_dict['intensive_care'] = [0] * len(region_dates)
        data_dict['total_hospitalized'] = [0] * len(region_dates)
        data_dict['home_confinement'] = [0] * len(region_dates)
        data_dict['total_current_positive_cases'] = [0] * len(region_dates)
        data_dict['new_positive_cases'] = cases[i]
        data_dict['recovered'] = [0] * len(region_dates)
        data_dict['death'] = [0] * len(region_dates)
        data_dict['total_cases'] = [0] * len(region_dates)
        data_dict['tests_performed'] = [0] * len(region_dates)
        df_temp = pd.DataFrame(data_dict)
        df_append = pd.concat([df_append, df_temp])

    df = pd.concat([df_append, df])

    return df


def get_data_from_provinces():

    data_file = os.path.join('/Users/pselvaraj/Github', 'COVID-19', 'dati-province', 'dpc-covid19-ita-province.csv')
    df = pd.read_csv(data_file)
    df.rename(columns={'data':'date',
                       'stato':'country',
                       'codice_regione':'region_code',
                       'denominazione_regione':'region_name',
                       'codice_provincia':'province_code',
                       'denominazione_provincia':'province_name',
                       'sigla_provincia':'province_abbreviation',
                       'totale_casi':'total_cases'}, inplace=True)

    today_date = datetime.today().strftime('%m_%d_%Y')
    df.to_csv(os.path.join(dropbox_path, 'Italy_provinces_as_of_%s.csv' % today_date))

    return None


def get_data_from_regions():

    data_file = os.path.join('/Users/pselvaraj/Github', 'COVID-19', 'dati-regioni', 'dpc-covid19-ita-regioni.csv')
    df = pd.read_csv(data_file)
    df.rename(columns={'data': 'date',
                       'stato': 'country',
                       'codice_regione': 'region_code',
                       'denominazione_regione': 'region_name',
                       'ricoverati_con_sintomi': 'hospitalized_with_symptoms',
                       'terapia_intensiva': 'intensive_care',
                       'totale_ospedalizzati': 'total_hospitalized',
                       'isolamento_domiciliare': 'home_confinement',
                       'totale_attualmente_positivi': 'total_current_positive_cases',
                       'nuovi_attualmente_positivi': 'new_positive_cases',
                       'dimessi_guariti': 'recovered',
                       'deceduti': 'death',
                       'totale_casi': 'total_cases',
                       'tamponi': 'tests_performed'}, inplace=True)

    df = create_dataframe_for_prior_data(df)
    df = df[['date', 'region_code',  'region_name', 'new_positive_cases']]
    df.sort_values(['date', 'region_code'], inplace=True)

    today_date = datetime.today().strftime('%m_%d_%Y')
    df.to_csv(os.path.join(dropbox_path, 'Italy_regions_as_of_%s.csv' % today_date))

    return None


if __name__ == '__main__':

    get_data_from_provinces()
    get_data_from_regions()