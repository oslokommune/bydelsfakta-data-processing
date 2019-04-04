import os

import pandas as pd

import common.aws as common_aws
import common.transform as transform
import common.aggregate_dfs as aggregator
from common.transform_output import generate_output_list
import json

os.environ['METADATA_API_URL'] = ''

s3_bucket = 'ok-origo-dataplatform-dev'

historic_dataset_id = 'Lav_utdanning_historisk-qBXzf'
historic_version_id = '1-tt4pE7NE'
historic_edition_id = 'EDITION-smQmD'
status_dataset_id = 'Lav_utdanning_status-fYdyK'
status_version_id = '1-FswVHo7C'
status_edition_id = 'EDITION-H7XFn'

pd.set_option('display.max_rows', 1000)

def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event['keys']['Lav_utdanning-Xn55b']
    bucket = event['bucket']
    start(bucket, s3_key)
    return "OK"


def start(bucket, key):
    low_education_raw = common_aws.read_from_s3(
        s3_key=key,
        date_column='År'
    )

    data_points = ['low_education']

    input_df = generate_input_df(low_education_raw.copy())

    low_education_historic = generate_output_list(*transform.historic(input_df),
                                                    template='b',
                                                data_points=data_points)
    low_education_status = generate_output_list(*transform.status(input_df),
                                                  template='a',
                                                  data_points=data_points)

    _write_to_intermediate(historic_dataset_id, historic_version_id, historic_edition_id, low_education_historic)
    _write_to_intermediate(status_dataset_id, status_version_id, status_edition_id, low_education_status)

def generate_input_df(df):
    df = pivot_table(df, 'Høyeste fullførte utdanning', 'Antall personer')
    with_district = transform.add_district_id(df)

    value_cols = ['Videregående skolenivå', 'Universitets- og høgskolenivå, kort','Universitets- og høgskolenivå, lang',
                  'Ingen utdanning/Uoppgitt utdanning', 'Grunnskole']
    df_aggregated = aggregator.aggregate_from_subdistricts(with_district, _aggregations(value_cols))

    data_point = 'low_education'
    df_aggregated[data_point] = df_aggregated['Ingen utdanning/Uoppgitt utdanning'] + df_aggregated['Grunnskole']

    input_df = aggregator.add_ratios(df_aggregated, [data_point], value_cols)
    return input_df



def pivot_table(df, pivot_column, value_column):
    key_columns = list(
        filter(lambda x: x not in [pivot_column, value_column], list(df))
    )
    df_pivot = pd.concat((df[key_columns],
                          df.pivot(columns=pivot_column, values=value_column)),
                         axis=1)
    return df_pivot.groupby(key_columns).sum().reset_index()


def _aggregations(data_points):
    return \
        [{'data_points': data_point, 'agg_func': 'sum'} for data_point in data_points]


def _output_key(dataset_id, version_id, edition_id):
    return f'processed/green/{dataset_id}/version={version_id}/edition={edition_id}/'


def _write_to_intermediate(dataset_id, version_id, edition_id, output_list):
    series = []
    heading = "Personer mellom 30-59 år med lav utdanning"
    output_key = _output_key(dataset_id, version_id, edition_id)
    common_aws.write_to_intermediate(output_key, output_list, heading, series)

#start(None, None)
if __name__ == '__main__':
    handle(
            {"bucket": "ok-origo-dataplatform-dev",
             "keys": {
                 "Lav_utdanning-Xn55b": "raw/green/Lav_utdanning-Xn55b/version=1-pYXX8pg4/edition=EDITION-7ofpj/Lav_utdanning(1.1.2008-1.1.2018-v01).csv"}
             }
            , {})