import os

import pandas as pd

import common.aws as common_aws
import common.transform as transform
import common.aggregate_dfs as aggregator
from common.transform_output import generate_output_list
import json

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
    landbakgrunn_key = event['keys']['Landbakgrunn_storste_innvandri-VqRsp']
    befolkning_key = event['keys']['Befolkningen_etter_bydel_delby-J7khG']
    start(landbakgrunn_key, befolkning_key)
    return "OK"


def start(landbakgrunn_key, befolkning_key):
    landbakgrunn_df = common_aws.read_from_s3(
        s3_key=landbakgrunn_key,
        date_column='År'
    )

    befolkning_raw = common_aws.read_from_s3(
        s3_key=befolkning_key,
        date_column='År'
    )

    ''' Antall personer | date | Bydel | Innvandringskategori | Landbakgrunn'''
    landbakgrunn_df = pivot_table(landbakgrunn_df, 'Innvandringskategori', 'Antall personer')#landbakgrunn_df[landbakgrunn_df['Innvandringskategori'] == 'Innvandrer']
    data_points = ['Innvandrer', 'Norskfødt med innvandrerforeldre']
    landbakgrunn_df['total'] = landbakgrunn_df[data_points].sum(axis=1)
    data_points.append('total')

    output_list = generate_output_list(landbakgrunn_df, data_points, n_district=10, n_total=15)

    #print(json.dumps(output_list))

    # _write_to_intermediate(historic_dataset_id, historic_version_id, historic_edition_id, low_education_historic)
    # _write_to_intermediate(status_dataset_id, status_version_id, status_edition_id, low_education_status)

def generate_output_list(df, data_points, n_district, n_total):
    landbakgrunn_df = df
    oslo_total_df = landbakgrunn_df.groupby(['date', 'Landbakgrunn']).sum().reset_index()
    top_n_district = get_top_n_district(landbakgrunn_df, n_district)
    top_n_total = get_top_n_total(oslo_total_df, n_total)

    output_list = []
    for district in landbakgrunn_df['Bydel'].unique():
        district_obj = {
            'district': district,
            'data': []
        }
        district_df = landbakgrunn_df[landbakgrunn_df['Bydel'] == district]
        for geography in top_n_district[district]:
            geo_df = district_df[district_df['Landbakgrunn'] == geography]
            geo_obj = generate_geo_obj(geo_df, geography, data_points)
            district_obj['data'].append(geo_obj)
        output_list.append(district_obj)

    oslo_total_obj = {
        'district': 'Oslo i alt',
        'data': []
    }
    for geography in top_n_total:
        geo_df = oslo_total_df[oslo_total_df['Landbakgrunn'] == geography]
        geo_obj = generate_geo_obj(geo_df, geography, data_points)
        oslo_total_obj['data'].append(geo_obj)
    output_list.append(oslo_total_obj)
    return output_list

def generate_geo_obj(df, geography, data_points):
    series = list_to_time_series(data_points)
    for value in df.to_dict('r'):
        for data_point in data_points:
            series[data_point].append({'date': value['date'], 'value': value[data_point]})
    return {'geography': geography, 'values': series}

def list_to_time_series(data_points):
    d = {}
    for data_point in data_points:
        d[data_point] = []
    return d

def get_top_n_total(df, n):
    total_df = df[df['date'] == 2018]
    print(total_df['total'].sum())
    total_df = total_df.nlargest(n, 'total')
    return total_df['Landbakgrunn'].tolist()

def population_df(df):
    df = df[['delbydelid', 'Alder', 'Antall personer', 'Kjønn', 'date']]
    df = df[df['delbydelid'].notnull()]
    df = pivot_table(df, 'Kjønn', 'Antall personer')
    df['total'] = df[1] + df[2]
    df = df[['delbydelid', 'date', 'total']]
    df = df.groupby(['delbydelid', 'date']).sum().reset_index()
    return df


def get_top_n_district(df, n):
    top_n = {}
    for district in df['Bydel'].unique():
        district_df = df[df['Bydel'] == district]
        district_df = district_df[district_df['date'] == 2018]
        district_df = district_df.nlargest(n, 'total')
        top_n[district] = district_df['Landbakgrunn'].tolist()
    return top_n


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
                 "Landbakgrunn_storste_innvandri-VqRsp": "raw/green/Landbakgrunn_storste_innvandri-VqRsp/version=1-vSao7mKy/edition=EDITION-MGGzQ/Landbakgrunn_storste_innvandringsgrupper(1.1.2008-1.1.2018-v01).csv",
                 "Befolkningen_etter_bydel_delby-J7khG": "raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv"}
             }
            , {})