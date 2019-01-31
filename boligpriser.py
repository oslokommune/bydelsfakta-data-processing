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
    historic = process_source(df)
    # TODO: Since we are missing aggregated date for districts from 2018, we cannot use max() of this row yet
    status = process_source(df[df.År == 2017])

    unfinished_output_key = 'processed/green/{dataset}/{version}/{edition}/'.format(
        dataset=groups['dataset'],
        version=groups['version'],
        edition=groups['edition']
    )

    write_dataset(historic, 'historic', bucket, unfinished_output_key)
    write_dataset(status, 'status', bucket, unfinished_output_key)


def write_dataset(df, suffix, bucket, unfinished_output_key):
    logging.info('Writing ' + suffix)
    for current_district, subdistrict in df:
        s3_key = unfinished_output_key + "{current_district}-{suffix}.json".format(current_district=current_district,
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


def subdistrict_as_dict(key, values):
    return {'geography': key, 'values': values}


def area_as_dict(key, values, scope):
    if scope == 'bydel':
        return {'geography': key, 'values': values, 'avgRow': True}
    if scope == 'oslo':
        return {'geography': key, 'values': values, 'totalRow': True}


def process_source(df_source):
    source_district = district_source(df_source)
    districts_list = pd.unique(source_district.geography)
    logging.debug("This is slow")
    subdistrict_list = [create_dataset_for_district(dis, df_source) for dis in districts_list]
    logging.debug("Complete")
    return subdistrict_list


def create_dataset_for_district(dis, df_source):
    source_subdistrict = df_source.dropna()
    source_oslo = oslo_source(df_source)
    source_district = district_source(df_source)

    subdistrict = source_subdistrict[source_subdistrict['bydelsnavn'] == dis]
    subdistrict = subdistrict.groupby(['geography'])['År', 'value'].apply(
        lambda x: x.astype(object).to_dict(orient='records'))
    subdistrict = [subdistrict_as_dict(key, values) for key, values in subdistrict.items()]
    "======="
    district = source_district[source_district['geography'] == dis]
    district = district[['År', 'value']]
    district = area_as_dict(dis, district.astype(object).to_dict(orient='records'), 'bydel')
    "======="
    oslo = source_oslo[['År', 'value']]
    oslo = area_as_dict('Oslo i alt ', oslo.astype(object).to_dict(orient='records'), 'oslo')
    subdistrict.append(district)
    subdistrict.append(oslo)
    return dis, subdistrict


def district_source(df_source: pd.DataFrame):
    df_source = df_source[pd.isnull(df_source['geography'])]
    drop_numbers = df_source.drop(columns=['geography']).rename(columns={'bydelsnavn': 'geography'})

    district = drop_numbers[drop_numbers['geography'] != "Oslo i alt"]
    district['geography'] = district['geography'].map(lambda x: fix_district_prefix(x))
    return district


def oslo_source(df_source: pd.DataFrame):
    df_source = df_source[pd.isnull(df_source['geography'])]
    drop_numbers = df_source.drop(columns=['geography']).rename(columns={'bydelsnavn': 'geography'})

    oslo = drop_numbers[drop_numbers['geography'] == "Oslo i alt"]
    return oslo


if __name__ == '__main__':
    event = {'Records': [
        {'s3': {
            'bucket': {'name': 'test-bucket'},
            'object': {'key': 'raw/green/datasetid/version/1030/distribution.csv'}
        }
        }
    ]}
    handler(event, {})
