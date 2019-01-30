import json

import pandas as pd
import numpy as np
import re

pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 30)


def generate_metadata(district, data):
    return {
        "meta": {
            "scope": "bydel",
            "heading": "Gjennomsnittpris (kr) pr kvm for blokkleilighet i {district}".format(district=district),
            "help": "Dette er en beskrivelse for hvordan dataene leses",
            "series": [
                {"heading": "gj.snittpris", "subheading": " pr kvm for blokkleilighet"}
            ],
            "xAxis": {
                "format": "%",
                "title": False
            },
            "publishedDate": "2019-06-01",
            "dataSource": {
                "url": "http://ssb.no/data/1234124/",
                "label": "Statistisk sentralbyrå"
            },
            "files": [
                {
                    "url": "http://data.oslo.kommune.no/data/1234123/data.xls",
                    "type": "Excel"
                },
                {
                    "url": "http://data.oslo.kommune.no/data/1234123/data.csv",
                    "type": "csv"
                }
            ]
        },

        "data": data
    }


def read_csv():
    df_source = pd.read_csv('test_data/Boligpriser.csv', sep=";", verbose=True)
    df_source = df_source.drop(columns=['antall omsatte blokkleieligheter', 'Delbydelnummer']) \
        .rename(columns={'kvmpris': 'value', 'Oslo-Bydelsnavn': 'bydelsnavn', 'Delbydelsnavn': 'geography'})
    process_source(df_source, 'historisk')
    process_source(df_source[df_source.År == 2017], 'status')


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


def process_source(df_source, suffix):

    source_district = district_source(df_source)
    districts_list = pd.unique(source_district.geography)

    subdistrict_list = [create_dataset_for_district(dis, df_source) for dis in districts_list]
    for dis, subdistrict in subdistrict_list:
        district = generate_metadata(dis, subdistrict)
        with open("out/{dis}-{suffix}.json".format(dis=dis, suffix=suffix), 'w+') as file:
            file.write(json.dumps(district, ensure_ascii=False))


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
    district['geography'] = district['geography'].apply(lambda x: fix_district_prefix(x))
    return district


def oslo_source(df_source: pd.DataFrame):
    df_source = df_source[pd.isnull(df_source['geography'])]
    drop_numbers = df_source.drop(columns=['geography']).rename(columns={'bydelsnavn': 'geography'})

    oslo = drop_numbers[drop_numbers['geography'] == "Oslo i alt"]
    return oslo


if __name__ == '__main__':
    read_csv()
