import os

import pandas as pd

import common.aws as common_aws
import common.transform as transform
import common.aggregate_dfs as aggregator
import common.util as util
from common.transform_output import generate_output_list

os.environ['METADATA_API_URL'] = ''

s3_bucket = 'ok-origo-dataplatform-dev'

historic_dataset_id = 'Landbakgrunn-innvandringsgrupp-87zEq'
historic_version_id = '1-JNRhrxaH'
historic_edition_id = 'EDITION-Zkwpy'
status_dataset_id = 'Landbakgrunn-innvandringsgrupp-cLcKm'
status_version_id = '1-jwbJpksw'
status_edition_id = 'EDITION-FbQet'

pd.set_option('display.max_rows', 1000)

def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    landbakgrunn_key = event['keys']['Landbakgrunn_storste_innvandri-VqRsp']
    befolkning_key = event['keys']['Befolkningen_etter_bydel_delby-J7khG']
    start(landbakgrunn_key, befolkning_key)
    return "OK"


def start(landbakgrunn_key, befolkning_key):
    landbakgrunn_raw = common_aws.read_from_s3(
        s3_key=landbakgrunn_key,
        date_column='År'
    )
    landbakgrunn_raw = pivot_table(landbakgrunn_raw, 'Innvandringskategori', 'Antall personer')

    befolkning_raw = common_aws.read_from_s3(
        s3_key=befolkning_key,
        date_column='År'
    )

    befolkning_df = generate_population_df(befolkning_raw)
    befolkning_district_df = generate_district_population_df(befolkning_df, ignore_districts=['16', '17'])

    landbakgrunn_df = process_country_df(landbakgrunn_raw)

    data_points = ['Innvandrer', 'Norskfødt med innvandrerforeldre', 'total']
    input_df = pd.merge(landbakgrunn_df, befolkning_district_df, how='inner', on=['district', 'date'])
    input_df = aggregator.add_ratios(input_df, data_points, ratio_of=['population'])

    output_list_historic = generate_output_list(input_df, data_points, top_n=1, template_fun=generate_geo_obj_historic)

    input_df_status = input_df[input_df['date'] == 2018]
    output_list_status = generate_output_list(input_df_status, data_points, top_n=10, template_fun=generate_geo_obj_status)

    _write_to_intermediate(historic_dataset_id, historic_version_id, historic_edition_id, output_list_historic)
    _write_to_intermediate(status_dataset_id, status_version_id, status_edition_id, output_list_status)


def generate_output_list(input_df, data_points, top_n, template_fun):
    top_n_district = get_top_n_district(input_df, top_n)

    output_list = []
    for district in input_df['district'].unique():
        district_obj = {
            'district': district,
            'data': []
        }
        district_df = input_df[input_df['district'] == district]
        for geography in top_n_district[district]:
            geo_df = district_df[district_df['Landbakgrunn'] == geography]
            geo_obj = template_fun(geo_df, geography, data_points)
            district_obj['data'].append(geo_obj)
        output_list.append(district_obj)

    return output_list


def generate_geo_obj_status(df, geography, data_points):
    series = {}
    for value in df.to_dict('r'):
        for data_point in data_points:
            series[data_point] = {
                'date': value['date'],
                'value': value[data_point],
                'ratio': value[f'{data_point}_ratio']
            }
    values = []
    [values.append(series[data_point]) for data_point in data_points if series]
    return {'geography': geography, 'values': values}


def generate_geo_obj_historic(df, geography, data_points):
    series = list_to_time_series(data_points)
    for value in df.to_dict('r'):
        for data_point in data_points:
            series[data_point].append({
                'date': value['date'],
                'value': value[data_point],
                'ratio': value[f'{data_point}_ratio']})
    values = []
    [values.append(series[data_point]) for data_point in data_points]
    return {'geography': geography, 'values': values}


def list_to_time_series(data_points):
    d = {}
    for data_point in data_points:
        d[data_point] = []
    return d


def process_country_df(df):
    data_points = ['Innvandrer', 'Norskfødt med innvandrerforeldre']
    df['total'] = df[data_points].sum(axis=1)
    data_points.append('total')
    oslo_total_df = df.groupby(['date', 'Landbakgrunn']).sum().reset_index()
    df['district'] = df['Bydel'].apply(util.get_district_id)
    oslo_total_df['district'] = '00'
    country_df = pd.concat((df, oslo_total_df), sort=False, ignore_index=True)
    return country_df[['district', 'date', 'Landbakgrunn', 'Innvandrer', 'Norskfødt med innvandrerforeldre', 'total']]


def generate_district_population_df(population_df, ignore_districts = []):
    population_df = transform.add_district_id(population_df)
    population_df = population_df[~population_df['district'].isin(ignore_districts)]
    population_district_df = population_df.groupby(['district','date']).sum().reset_index()
    oslo_total_df = population_district_df.groupby('date').sum().reset_index()
    oslo_total_df['district'] = '00'
    population_district_df = pd.concat((population_district_df, oslo_total_df), sort=False, ignore_index=True)
    return population_district_df


def generate_population_df(df):
    df = df[['delbydelid', 'Alder', 'Antall personer', 'Kjønn', 'date']]
    df = df[df['delbydelid'].notnull()]
    df = pivot_table(df, 'Kjønn', 'Antall personer')
    df['population'] = df[1] + df[2]
    df = df[['delbydelid', 'date', 'population']]
    df = df.groupby(['delbydelid', 'date']).sum().reset_index()
    return df


def get_top_n_district(df, n):
    top_n = {}
    for district in df['district'].unique():
        district_df = df[df['district'] == district]
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
    series = [
        {"heading": "Innvandrer", "subheading": ""},
        {"heading": "Norskfødt med innvandrerforeldre", "subheading": ""},
        {"heading": "Totalt", "subheading": ""}
    ]
    heading = "10 største innvandringsgrupper"
    output_key = _output_key(dataset_id, version_id, edition_id)
    common_aws.write_to_intermediate(output_key, output_list, heading, series)


if __name__ == '__main__':
    handle(
            {"bucket": "ok-origo-dataplatform-dev",
             "keys": {
                 "Landbakgrunn_storste_innvandri-VqRsp": "raw/green/Landbakgrunn_storste_innvandri-VqRsp/version=1-vSao7mKy/edition=EDITION-MGGzQ/Landbakgrunn_storste_innvandringsgrupper(1.1.2008-1.1.2018-v01).csv",
                 "Befolkningen_etter_bydel_delby-J7khG": "raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv"}
             }
            , {})