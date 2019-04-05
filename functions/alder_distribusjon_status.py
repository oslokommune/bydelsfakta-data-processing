import json

import numpy as np
import pandas as pd

import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output
import common.util

import pyperclip


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event['keys']['Befolkningen_etter_bydel_delby-J7khG']
    bucket = event['bucket']
    df = start(bucket, s3_key)

    create_ds(df, "ds_id")
    return "OK"


def start(bucket, key):
    original = common.aws.read_from_s3(
            s3_key=key
    )
    original = original.rename(columns={'Antall personer': 'value'})
    original = common.transform.status(original)[0]
    # TODO: Run csv transform to get correct ids for 'no address'
    original.loc[original['Delbydel'] == 9999, 'delbydelid'] = "0301999901"
    original = original[original['delbydelid'].notna()]
    original = common.transform.add_district_id(original)
    base = original.groupby(['district', 'delbydelid', 'date', 'Alder', 'Kjønn'])['value'].sum().reset_index()
    base = base.pivot_table(values='value', index=['district', 'delbydelid', 'date', 'Alder'], columns='Kjønn',
                            fill_value=0).rename(columns={1: 'mann', 2: 'kvinne'}).reset_index()
    
    base = base.groupby(['district', 'delbydelid', 'date']).apply(lambda x: _ensure_sequential_age(x.name, x)).reset_index(drop=True)
    base['value'] = base['mann'] + base['kvinne']

    subdistrict = base.groupby(['delbydelid',  'date']).apply(_ratio)

    district = base.groupby(['district',  'date',  'Alder'])['mann','kvinne','value'].sum().reset_index()
    district['delbydelid'] = np.nan
    district = district.groupby(['district',  'date']).apply(_ratio).reset_index()

    oslo = base.groupby(['date',  'Alder'])['mann', 'kvinne', 'value'].sum().reset_index()
    oslo['delbydelid'] = np.nan
    oslo['district'] = "00"
    oslo = oslo.groupby(['date']).apply(_ratio)

    merge = subdistrict.append(district, ignore_index=True)
    merge = merge.append(oslo, ignore_index=True)
    return merge

def _ensure_sequential_age(name, df):
    new_index = pd.Index(np.arange(0, 120, 1), name="Alder")
    df = df.set_index('Alder').reindex(new_index).reset_index()
    df = df.fillna({'district': name[0], 'delbydelid': name[1], 'date': name[2], 'mann': 0, 'kvinne': 0}, downcast='infer')
    return df


def _ratio(df):
    df['ratio'] = df['value'] / df['value'].sum()
    return df

def to_quantile(df, quantile):
    def percentile(group):
        tmp = group['Alder'].repeat(group['value'])
        return pd.Series({f'p_{quantile}': np.quantile(tmp, q=quantile).astype(int)})

    subdistrict = df.groupby(['district', 'delbydelid', 'date']).apply(percentile).reset_index()
    district = df.groupby(['district', 'date']).apply(percentile).reset_index()
    district['delbydelid'] = np.nan

    oslo = df.groupby(['date']).apply(percentile).reset_index()
    oslo['delbydelid'] = np.nan
    oslo['district'] = "00"

    all = subdistrict.append(district).append(oslo).reset_index(drop=True)
    return all


def to_json(df, series):
    list = []
    for name, group in df.groupby(['district', 'date']):
        if name[0] != "00":
            data = [geo_to_json(subgroup,series,name[1],sub, avgRow=(sub == name[0]))
                    for sub, subgroup in group.fillna(name[0]).groupby(['delbydelid'])]

            oslo = df.loc[df['district'] == "00"]
            data.append(geo_to_json(oslo, series, name[1], "00", totalRow=True))
            list.append({'district': name[0], 'data': data})

    oslo = {'district': "00", 'data': []}
    for sub, subgroup in df[df['delbydelid'].isna()].groupby(['district', 'date']):
        if sub[0] in ['16', '17', '99']:
            continue
        data = [geo_to_json(subgroup, series, sub[1], sub[0], totalRow=(sub[0] == "00"))]
        oslo['data'].append(*data)
    list.append(oslo)
    return filter(lambda x: x['district'] not in ['16', '17', '99'], list)

def geo_to_json(df, series, date, geography, avgRow = False, totalRow = False):
    values = df[series].to_dict('r')
    return {
        "aargang": date,
        "geography": geography,
        "totalRow": totalRow,
        "avgRow": avgRow,
        "values": values
    }




def create_ds(df, dataset_id):
    heading = "Aldersdistribusjon fordelt på kjønn"
    series = [{'heading': 'Aldersdistribusjon fordelt på kjønn', 'subheading': ''}]

    # To json : convert df to list of json objects
    jsonl = to_json(df, ['Alder', 'mann', 'kvinne', 'value', 'ratio'])
    output_key = 'processed/green/Alder-distribusjon-status-TqWvs/version=1-2qgmA58u/edition=EDITION-gs3fR/'
    common.aws.write_to_intermediate(
           output_key=output_key,
           output_list=jsonl,
           heading=heading,
           series=series)
    return output_key


if __name__ == '__main__':
    handler(
            {'bucket': 'ok-origo-dataplatform-dev',
             'keys': {
                 'Befolkningen_etter_bydel_delby-J7khG': 'raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv'}
             }
            , {})

