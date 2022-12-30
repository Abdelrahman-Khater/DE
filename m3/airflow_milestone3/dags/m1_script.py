import pandas as pd
import numpy as np


def task1():
    filename = '/opt/airflow/data/UK_Accidents_2016.csv'
    df = pd.read_csv(filename)
    df_clean = clean(df)
    df_trans, lookup = transform(df_clean)
    df_trans.to_csv('/opt/airflow/data/df_m1.csv',index=False)
    lookup.to_csv('/opt/airflow/data/lookup_m1.csv',index=False)


# TODO: By Jobail
# don't transform longitude and latitude as i will be using them in task 2
# lookup table columns are ['feature_value', 'code']
def transform(df):
    df_trans = df.copy()
    lookup = pd.DataFrame(columns=['feature_value', 'code'])

    return df_trans, lookup


def clean(df):
    df_clean = df.replace(['Data missing or out of range', 'unknown (self reported)'], np.nan)
    df_clean = remove_rows_with_nan_less_than(df_clean, threshold=1)
    for col_name in ['road_type', 'weather_conditions', 'trunk_road_flag']:
        df_clean[col_name] = replace_nan_with_mode(df_clean, col_name)
    for col_name in ['junction_control', 'second_road_number']:
        df_clean[col_name] = df_clean[col_name].fillna('None')
    df_clean.second_road_class = df_clean.second_road_class.replace('-1', 'None')
    col_name = 'did_police_officer_attend_scene_of_accident'
    df_clean[col_name] = df_clean[col_name].replace('No - accident was reported using a self completion  form (self rep only)', 'No')
    df_clean = df_clean.drop(['accident_index', 'accident_year', 'location_easting_osgr', 'location_northing_osgr'], axis=1)
    all_cols_except_ref = df_clean.columns.to_list().remove('accident_reference')
    df_clean = df_clean.drop_duplicates(all_cols_except_ref)
    return df_clean


def remove_rows_with_nan_less_than(dataset: pd.DataFrame, threshold:float = 0):
    cnt = dataset.isna().sum()
    cnt = cnt[cnt > 0] 
    cnt = cnt * 100 / len(dataset)
    cnt = cnt[cnt < threshold]
    cols = cnt.index.to_list()
    return dataset.dropna(axis='index', subset=cols)

def replace_nan_with_mode(dataset:pd.DataFrame, col_name):
    return dataset[col_name].fillna(dataset[col_name].mode()[0])