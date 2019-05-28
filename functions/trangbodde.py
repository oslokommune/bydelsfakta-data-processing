import os

import pandas as pd

import common.aws as common_aws
from common.aggregateV2 import Aggregate
import common.population_utils as population_utils
import numpy as np

from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB

os.environ["METADATA_API_URL"] = ""

pd.set_option("display.max_rows", 1000)


def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    landbakgrunn_raw = common_aws.read_from_s3(
        s3_key=event["input"]["trangbodde"],
        date_column="aar"
    )
    befolkning_raw = common_aws.read_from_s3(
        s3_key=event["input"]["befolkning-etter-kjonn-og-alder"],
        date_column="aar")

    datapoint = 'antall_trangbodde'
    input_df = generate_input_df(landbakgrunn_raw, befolkning_raw, datapoint)
    import json
    print(json.dumps(_historic(input_df, [datapoint])))
    return input_df


def generate_input_df(trangbodde_raw, population_raw, data_point):
    population_df = population_utils.generate_population_df(population_raw)

    agg = {"population": "sum"}
    population_district_df = Aggregate(agg).aggregate(df=population_df)
    trangbodde_raw['bydel_id'] = trangbodde_raw['bydel_id'].apply(convert_stupid_district_id)

    input_df = pd.merge(
        trangbodde_raw, population_district_df, how="inner", on=["bydel_id", "date", "delbydel_id"]
    ).rename(
        columns={'bydel_navn_x': 'bydel_navn',
                 'delbydel_navn_x': 'delbydel_navn'}
    )
    input_df[f'{data_point}_ratio'] = input_df['andel_som_bor_trangt']/100
    input_df[data_point] = input_df['population']*input_df[f'{data_point}_ratio']

    # Exclude Marka, Sentrum and Uten registrert adresse
    input_df = input_df[~input_df['bydel_id'].isin(['16','17','99'])]

    return input_df[['date', 'bydel_id', 'bydel_navn', 'delbydel_id', 'delbydel_navn', data_point, f'{data_point}_ratio']]



def _historic(input_df, data_points):
    metadata = Metadata(
        heading='Levekår Trangbodde',
        series=[{"heading": "Trangbodde", "subheading": ""}],
    )
    output = Output(
        values = data_points, df=input_df, metadata=metadata, template=TemplateB()
    ).generate_output()

    return output


def convert_stupid_district_id(possibly_stupid_id):
    if possibly_stupid_id == '10000':
        return '00'
    else:
        return possibly_stupid_id


if __name__ == "__main__":
    # handle(
    #     {
    #         "input": {
    #             "trangbodde": "raw/green/trangbodde/version=1/edition=20190524T112022/Trangbodde(1.1.2015-1.1.2017-v01).csv",
    #             "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190523T211529/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv"
    #         },
    #         "output": "s3/key/or/prefix",
    #         "config": {"type": "status"}
    #     },
    #     None
    # )
    handle(
        {
            "input": {
                "trangbodde": "raw/green/trangbodde/version=1/edition=20190524T112022/Trangbodde(1.1.2015-1.1.2017-v01).csv",
                "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190523T211529/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv"
            },
            "output": "s3/key/or/prefix",
            "config": {"type": "historic"}
        },
        None
    )