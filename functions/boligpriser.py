import json

import pandas as pd
import numpy as np
import re
import pandas_layer as pl
import bydelsfakta_layer as bl
import logging

pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 30)

logging.basicConfig(level=logging.INFO)


def handler(event, context):
    logging.info(event)
    bucket, key, groups = pl.match_s3_key(event)
    df = read_csv(bucket, key)
    historic = create_dataset(df)
    # TODO: Since we are missing aggregated date for districts from 2018, we cannot use max() of this row yet
    status = create_dataset(df[df.År == 2017])

    unfinished_output_key = 'processed/green/boligpriser_alle/{version}/{edition}/'.format(
        version=groups['version'],
        edition=groups['edition']
    )

    write_dataset(historic, 'historic', bucket, unfinished_output_key)
    write_dataset(status, 'status', bucket, unfinished_output_key)

def ensure_alphanumeric(filename):
    o_regex = re.compile('[Øø]')
    a_regex = re.compile('[Åå]')
    ae_regex = re.compile('[Ææ]')
    u_regex = re.compile('[Üü]')
    default_regex = re.compile('[^a-zA-Z0-9]')

    filename = o_regex.sub("o", filename)
    filename = a_regex.sub("a", filename)
    filename = ae_regex.sub("ae", filename)
    filename = u_regex.sub("u", filename)
    filename = default_regex.sub('_', filename)
    return filename

def write_dataset(df, suffix, bucket, unfinished_output_key):
    logging.info('Writing ' + suffix)
    for current_district, subdistrict in df:

        s3_key = unfinished_output_key + "{current_district}-{suffix}.json".format(current_district=ensure_alphanumeric(current_district),
                                                                                   suffix=suffix)

        district = bl.generate_metadata(current_district,
                                        heading="Gjennomsnittpris (kr) pr kvm for blokkleilighet",
                                        series=None,
                                        data=subdistrict
                                        )
        pl.write_json_to_s3(bucket=bucket, key=s3_key, data=json.dumps(district, ensure_ascii=False))
    logging.info('Complete ' + suffix)


def read_csv(bucket, key):
    df_source = pl.read_csv_from_s3(bucket, key, separator=';')
    df_source = df_source.drop(columns=['antall omsatte blokkleieligheter', 'Delbydelnummer']) \
        .rename(columns={'kvmpris': 'value', 'Oslo-Bydelsnavn': 'bydelsnavn', 'Delbydelsnavn': 'geography'})
    return df_source


def fix_district_prefix(line):
    if line.startswith("Bydel "):
        return fix_district_prefix(line[6:])
    elif line.startswith("St "):
        return re.sub('^%s' % "St ", "St.", line)
    else:
        return line


def create_district(district, group, oslo_in_all):
    data_list = [] + oslo_in_all
    for subdistrict, values in group.groupby(['geography']):
        data = {'geography': subdistrict, 'values': values.drop(columns=['geography', 'bydelsnavn']).to_dict('records')}
        if subdistrict == 'AGGREGATED':
            data['geography'] = district
            if district == 'Oslo i alt':
                data['totalRow'] = True
            else:
                data['aggRow'] = True
        data_list.append(data)
    return district, data_list


def create_oslo(df, level):
    data_list = []
    for name, value in df.groupby(['geography']):
        data = {'geography': name, 'values': value.drop(columns=['geography']).to_dict('records')}
        if name == 'Oslo i alt':
            data[level] = True
        data_list.append(data)
    return data_list


def create_dataset(df_source):
    df_source['bydelsnavn'] = df_source['bydelsnavn'].apply(lambda x: fix_district_prefix(x))
    df_source['geography'] = df_source['geography'].fillna('AGGREGATED')

    oslo_in_all = df_source[(df_source['bydelsnavn'] == 'Oslo i alt')]
    oslo_in_all = create_oslo(oslo_in_all.drop(columns=['geography']).rename(columns={'bydelsnavn': 'geography'}),
                              'totalRow')

    total_oslo = df_source[(df_source['geography'] == 'AGGREGATED')]
    total_oslo = total_oslo.drop(columns=['geography']).rename(columns={'bydelsnavn': 'geography'})
    total_oslo = "Oslo i alt", create_oslo(total_oslo, "avgRow")

    df_source = df_source.groupby(['bydelsnavn'])
    ds_list = [create_district(district, group, oslo_in_all) for district, group in df_source]
    return ds_list + [total_oslo]


if __name__ == '__main__':
    event = {'Records': [
        {'s3': {
            'bucket': {'name': 'test-bucket'},
            'object': {'key': 'raw/green/datasetid/version/1030/distribution.csv'}
        }
        }
    ]}
    handler(event, {})
