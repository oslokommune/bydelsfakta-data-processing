import os

import pandas as pd
import numpy as np

import common.aws as common_aws
import common.aggregate_dfs as aggregate
import common.transform as transform
from common.transform_output import generate_output_list

import json

os.environ['METADATA_API_URL'] = ''

s3_bucket = 'ok-origo-dataplatform-dev'
historic_id = 'husholdning_totalt_historisk-UNIQUE-ZYX'

def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event['keys']['boligpriser-urFqK']
    bucket = event['bucket']
    start(bucket, s3_key)
    return "OK"


def start(bucket, key):
    household_raw = common_aws.read_from_s3(
        s3_key=key,
        date_column='År'
    )

    #     pd.read_csv(f'test_data/Husholdninger_med_barn.csv', sep=';', converters={
    #     'delbydelid': lambda x: str(x)
    # })

    data_points = ['single_adult', 'no_children', 'with_children']

    with_district = transform.add_district_id(household_raw.copy())

    with_data_points = with_household_data_points(with_district)
    with_data_points = with_data_points.groupby(['delbydelid', 'date', 'district']).sum().reset_index()

    input_df = aggregate.aggregate_from_subdistricts(with_data_points, _aggregations(data_points))
    input_df = aggregate.add_ratios(input_df, data_points, ratio_of=data_points)

    household_total_historic = generate_output_list(*transform.historic(input_df),
                                                    template='c',
                                                data_points=data_points)
    household_total_status = generate_output_list(*transform.status(input_df),
                                                  template='a',
                                                  data_points=data_points)
    household_total_matrix = generate_output_list(*transform.status(input_df),
                                                  template='i',
                                                  data_points=data_points)

    _write_to_intermediate('husholdning-totalt-historisk-xyz', household_total_historic)
    _write_to_intermediate('husholdning-totalt-status-xyz', household_total_status)
    _write_to_intermediate('husholdning-totalt-matrise-xyz', household_total_matrix)


def _aggregations(data_points):
    return \
        [{'data_points': data_point, 'agg_func': 'sum'} for data_point in data_points]


def with_household_data_points(household_raw):
    household_raw['household_data_point'] = household_raw['Husholdningstype'].apply(household_data_point)

    with_data_points = pd.concat((household_raw[['date', 'district', 'delbydelid']],
                                 household_raw.pivot(columns='household_data_point',values='Antall husholdninger')),
                                axis=1)

    return with_data_points


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


def _output_key(dataset_id):
    return f'intermediate/green/{dataset_id}/version=1/edition=??'


def _write_to_intermediate(dataset_id, output_list):
    series = [
        {"heading": "Antall aleneboende", "subheading": ""},
        {"heading": "Antall øvrige husholdninger uten barn", "subheading": ""},
        {"heading": "Antall husholdninger med barn", "subheading": ""},
    ]
    heading = "Husholdninger"
    output_key = _output_key(dataset_id)
    common_aws.write_to_intermediate(output_key, output_list,heading, series)
