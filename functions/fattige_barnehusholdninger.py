import os

import pandas as pd

import common.aws as common_aws
import common.transform as transform
from common.transform_output import generate_output_list

os.environ['METADATA_API_URL'] = ''

s3_bucket = 'ok-origo-dataplatform-dev'

historic_dataset_id = ''
historic_version_id = ''
historic_edition_id = ''
status_dataset_id = ''
status_version_id = ''
status_edition_id = ''

pd.set_option('display.max_rows', 1000)

def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event['keys']['Fattige-barnehusholdninger-iFHCQ']
    bucket = event['bucket']
    start(bucket, s3_key)
    return "OK"


def start(bucket, key):
    low_income_household_raw = common_aws.read_from_s3(
        s3_key=key,
        date_column='År'
    )

    data_points = ['antall_fattige_barnehusholdninger']

    input_df = generate_input_df(low_income_household_raw.copy(), data_points)

    low_income_household_historic = generate_output_list(*transform.historic(input_df),
                                                    template='b',
                                                data_points=data_points)
    low_income_household_status = generate_output_list(*transform.status(input_df),
                                                  template='a',
                                                  data_points=data_points)

    _write_to_intermediate(historic_dataset_id, historic_version_id, historic_edition_id, low_income_household_historic)
    _write_to_intermediate(status_dataset_id, status_version_id, status_edition_id, low_income_household_status)

def generate_input_df(df, data_points):
    df['Geografi'] = df['Geografi'].apply(str.strip)
    with_district = transform.add_district_id(filter_invalid_geographies(df),
                                              district_column='Geografi')
    input_df = add_data_points(with_district, data_points[0])
    return input_df

def filter_invalid_geographies(df):
    return df[df['Geografi'] != 'Uoppgitt']

def add_data_points(df, datapoint):
    df[datapoint] = df['Husholdninger med barn <18 år']*df['Husholdninger med barn <18 år EU-skala']/100
    df[f'{datapoint}_ratio'] = df['Husholdninger med barn <18 år EU-skala']/100
    df = df[df[datapoint].notnull()]
    return df[['delbydelid', 'district', 'date', datapoint, f'{datapoint}_ratio']]

def _aggregations(data_points):
    return \
        [{'data_points': data_point, 'agg_func': 'sum'} for data_point in data_points]


def _output_key(dataset_id, version_id, edition_id):
    return f'processed/green/{dataset_id}/version={version_id}/edition={edition_id}/'


def _write_to_intermediate(dataset_id, version_id, edition_id, output_list):
    series = []
    heading = "Lavinntekts husholdninger med barn"
    output_key = _output_key(dataset_id, version_id, edition_id)
    common_aws.write_to_intermediate(output_key, output_list, heading, series)

start(None, None)
if __name__ == '__main__':
    handle(
            {"bucket": "ok-origo-dataplatform-dev",
             "keys": {
                 "Husholdninger_med_barn-XdfNB": "raw/green/Husholdninger_med_barn-XdfNB/version=1-oTr62ZHJ/edition=EDITION-ivaYi/Husholdninger_med_barn(1.1.2008-1.1.2018-v01).csv"}
             }
            , {})
