import os

import pandas as pd

import common.aws as common_aws
import common.transform as transform
import common.aggregate_dfs as aggregator
from common.transform_output import generate_output_list
import json

os.environ['METADATA_API_URL'] = ''

s3_bucket = 'ok-origo-dataplatform-dev'

historic_dataset_id = 'Sysselsatte_historisk-r8KeQ'
historic_version_id = '1-wbekCGXQ'
historic_edition_id = 'EDITION-yVKK8'
status_dataset_id = 'Sysselsatte_status-3nUAC'
status_version_id = '1-aeEECFzo'
status_edition_id = 'EDITION-QzKgm'

pd.set_option('display.max_rows', 1000)


def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    sysselsatte_key = event['keys']['Sysselsatte-SYiFy']
    befolkning_key = event['keys']['Befolkningen_etter_bydel_delby-J7khG']
    start(sysselsatte_key, befolkning_key)
    return "OK"


def start(sysselsatte_key, befolkning_key):
    sysselsatte_raw = common_aws.read_from_s3(
        s3_key=sysselsatte_key,
        date_column='År'
    )
    befolkning_raw = common_aws.read_from_s3(
        s3_key=befolkning_key,
        date_column='År'
    )

    data_point = 'Antall sysselsatte'
    total = 'total'

    befolkning_df = befolkning_30_to_59(befolkning_raw)
    '''Value for date in "sysselsatte" was measured in 4th. quarter of 2017, while date for "befolkning" was measured 1.1.2018.'''
    befolkning_df['date'] = befolkning_df['date'] - 1

    befolkning_df = transform.add_district_id(befolkning_df)

    sub_districts = befolkning_df['delbydelid'].unique()

    sysselsatte_df = sysselsatte_raw
    sysselsatte_df = sysselsatte_df[sysselsatte_df['delbydelid'].isin(sub_districts)]
    sysselsatte_df = sysselsatte_df[['delbydelid', 'date', data_point]]
    sysselsatte_df = transform.add_district_id(sysselsatte_df)
    sysselsatte_df[data_point] = sysselsatte_df[data_point].apply(string_with_whitepace_to_int)
    sysselsatte_befolkning_df = aggregator.merge_dfs(sysselsatte_df, befolkning_df, how='outer')


    aggregated_df = aggregator.aggregate_from_subdistricts(sysselsatte_befolkning_df, aggregations=_aggregations([data_point, total]))

    input_df = aggregator.add_ratios(aggregated_df, [data_point], ratio_of=[total])

    sysselsatte_historic = generate_output_list(*transform.historic(input_df),
                                                    template='b',
                                                data_points=[data_point])
    sysselsatte_status = generate_output_list(*transform.status(input_df),
                                                  template='a',
                                                  data_points=[data_point])

    _write_to_intermediate(historic_dataset_id, historic_version_id, historic_edition_id, sysselsatte_historic)
    _write_to_intermediate(status_dataset_id, status_version_id, status_edition_id, sysselsatte_status)


def string_with_whitepace_to_int(s):
    return int(s.replace(' ', ''))

def befolkning_30_to_59(df):
    df = df[['delbydelid', 'Alder', 'Antall personer', 'Kjønn', 'date']]
    df = df[df['delbydelid'].notnull()]
    df = df[df['Alder'].between(30, 59)]
    df = pivot_table(df, 'Kjønn', 'Antall personer')
    df['total'] = df[1] + df[2]
    df = df[['delbydelid', 'date', 'total']]
    df = df.groupby(['delbydelid', 'date']).sum().reset_index()
    return df

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


if __name__ == '__main__':
    handle(
        {
            "bucket": "ok-origo-dataplatform-dev",
            "keys": {
                "Sysselsatte-SYiFy": "raw/green/Sysselsatte-SYiFy/version=1-7jQgDyo6/edition=EDITION-PKiEG/Sysselsatte(2007-2017-v01).csv",
                "Befolkningen_etter_bydel_delby-J7khG": "raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv"
            }
        }
        , {})
