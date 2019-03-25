import os

import pandas as pd
import numpy as np

import common.aws as common_aws
from common.aggregate_dfs import add_ratios
from common.transform_output import generate_output_list
import json

os.environ['METADATA_API_URL'] = ''

s3_bucket = 'ok-origo-dataplatform-dev'
historic_id = 'husholdning_totalt_historisk-UNIQUE-ZYX'

def handle(event, context):
    common_aws.handle(event, context, start())


def start():
    household_raw = pd.read_csv(f'test_data/Husholdninger_med_barn.csv', sep=';', converters={
        'delbydelid': lambda x: str(x)
    })

    data_points = ['single_adult', 'no_children', 'with_children']
    with_district_and_subdistrict = standarize_columns(household_raw.copy())
    with_data_points = with_household_data_points(with_district_and_subdistrict)
    households_by_sub_district = with_data_points.groupby(
        ['delbydelid', 'date', 'district']).sum().reset_index()

    input_df = households_by_sub_district.append(aggregate_district(households_by_sub_district), sort=True)
    input_df = add_ratios(input_df, data_points, ratio_of=data_points)

    input_df_latest = input_df[input_df['date'] == input_df['date'].max()]

    household_total_historic = generate_output_list(input_df, template='c', data_points=data_points)
    household_total_status = generate_output_list(input_df_latest, template='a', data_points=data_points)
    household_total_matrix = generate_output_list(input_df_latest, template='i', data_points=data_points)

    #write_files('bucket')
    print(json.dumps(household_total_historic))


def standarize_columns(household_raw):
    household_raw['district'] = household_raw['delbydelid'].str.slice(4,6)
    household_raw['date'] = household_raw['År']
    household_raw = household_raw[household_raw['district'].str.len() > 0]

    return household_raw

def with_household_data_points(household_raw):
    household_raw['household_data_point'] = household_raw['Husholdningstype'].apply(household_data_point)

    with_data_points = pd.concat((household_raw[['date', 'district', 'delbydelid']],
                                 household_raw.pivot(columns='household_data_point',values='Antall husholdninger')),
                                axis=1)

    return with_data_points

def aggregate_district(df):
    districts_aggregated = df.groupby(['district', 'date']).sum().reset_index()
    districts_aggregated['delbydelid'] = np.nan
    oslo_aggregated = df.groupby('date').sum().reset_index()
    oslo_aggregated['district'] = '00'
    oslo_aggregated['delbydelid'] = np.nan
    return districts_aggregated.append(oslo_aggregated, sort=True)


def household_data_point(household_type):
    with_children = [
        'Mor/far med små barn',
        'Mor/far med store barn',
        'Par med små barn',
        'Par med store barn',
        'Enfamiliehusholdninger med voksne barn',
        'Flerfamiliehusholdninger med små barn',
        'Flerfamiliehusholdninger med store barn'
    ]
    no_children = [
        'Flerfamiliehusholdninger uten barn 0 - 17 år',
        'Par uten hjemmeboende barn',
        'Flerfamiliehusholdninger uten barn 0-17 år'
    ]
    single_adult = ['Aleneboende']

    if household_type in with_children:
        return 'with_children'
    elif household_type in no_children:
        return 'no_children'
    elif household_type in single_adult:
        return 'single_adult'
    else:
        raise Exception(f'No data_point for Hushodningstype={household_type}')


def write_files(bucket, dataset_id, output):
    series = [
        {"heading": "Antall aleneboende", "subheading": ""},
        {"heading": "Antall øvrige husholdninger uten barn", "subheading": ""},
        {"heading": "Antall husholdninger med barn", "subheading": ""},
    ]
    heading = "Innvandring befolkning"
    common.write_files(bucket=bucket, dataset_id=dataset_id, output_list=output, heading=heading, series=series)

start()
