import json

import pandas as pd
import numpy as np
import re

pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 30)


def generate_metadata(bydel, data):
    return {
        "meta": {
            "scope": "bydel",
            "heading": "Gjennomsnittpris (kr) pr kvm for blokkleilighet i {bydel}".format(bydel=bydel),
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


def fix_area_prefix(line):
    if line.startswith("Bydel "):
        return fix_area_prefix(line[6:])
    elif line.startswith("St "):
        return re.sub('^%s' % "St ", "St.", line)
    else:
        return line


def district_as_dict(key, values):
    return {'geography': key, 'values': values}

def area_as_dict(key, values, scope):
    if scope == 'bydel':
        return {'geography': key, 'values': values, 'avgRow': True}
    if scope == 'oslo':
        return {'geography': key, 'values': values, 'totalRow': True}


def process_source(df_source, suffix):

    source_area = area_source(df_source)
    districts = pd.unique(source_area.geography)
    delbydel_list = [create_dataset_for_area(dis, df_source) for dis in districts]
    for dis, delbydel in delbydel_list:
        bydel = generate_metadata(dis, delbydel)
        with open("out/{dis}-{suffix}.json".format(dis=dis, suffix=suffix), 'w+') as file:
            file.write(json.dumps(bydel, ensure_ascii=False))


def create_dataset_for_area(dis, df_source):
    source_district = df_source.dropna()
    source_oslo = oslo_source(df_source)
    source_area = area_source(df_source)

    delbydel = source_district[source_district['bydelsnavn'] == dis]
    delbydel = delbydel.groupby(['geography'])['År', 'value'].apply(
        lambda x: x.astype(object).to_dict(orient='records'))
    delbydel = [district_as_dict(key, values) for key, values in delbydel.items()]
    "======="
    area = source_area[source_area['geography'] == dis]
    area = area[['År', 'value']]
    area = area_as_dict(dis, area.astype(object).to_dict(orient='records'), 'bydel')
    "======="
    oslo = source_oslo[['År', 'value']]
    oslo = area_as_dict('Oslo i alt ', oslo.astype(object).to_dict(orient='records'), 'oslo')
    delbydel.append(area)
    delbydel.append(oslo)
    return dis, delbydel


def area_source(df_source: pd.DataFrame):
    df_source = df_source[pd.isnull(df_source['geography'])]
    drop_numbers = df_source.drop(columns=['geography']).rename(columns={'bydelsnavn': 'geography'})

    area = drop_numbers[drop_numbers['geography'] != "Oslo i alt"]
    area['geography'] = area['geography'].apply(lambda x: fix_area_prefix(x))
    return area


def oslo_source(df_source: pd.DataFrame):
    df_source = df_source[pd.isnull(df_source['geography'])]
    drop_numbers = df_source.drop(columns=['geography']).rename(columns={'bydelsnavn': 'geography'})

    oslo = drop_numbers[drop_numbers['geography'] == "Oslo i alt"]
    return oslo


if __name__ == '__main__':
    read_csv()
