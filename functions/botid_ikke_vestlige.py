from common.aws import read_from_s3, write_to_intermediate
from common.templates import TemplateA, TemplateB
import common.transform
import common.util
import common.transform_output
from common.aggregateV2 import Aggregate

import pandas as pd

from common.output import Metadata, Output

pd.set_option("display.max_rows", 1000)

graph_metadata = Metadata(
    heading = "Ikke-vestlige innvandrere korttid", series = [
    {"heading": "Ikke-vestlige innvandrere som har bodd kortere tid enn 5 år per bydel og delbydel i Oslo.",
     "subheading": ""}
])

def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["botid-ikke-vestlige"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    start(s3_key, output_key, type_of_ds)
    return "OK"


def start(key, output_key, type_of_ds):
    df = read_from_s3(
        s3_key=key, date_column="aar", dtype={"bydel_id": object, "delbydel_id": object}
    )
    df = df.drop(columns=['norge'])
    df['total'] = df['asia_afrika_latin_amerika_og_ost_europa_utenfor_eu'] +  df['vest_europa_usa_canada_australia_og_new_zealand'] + df['ost_europeiske_eu_land']

    df["bydel_id"].fillna("00", inplace=True)

    df = pivot_table(df, 'botid', 'total')

    df = df.rename(columns={"Innvandrer, lang botid (>5 år)": "innvandrer_lang", "Innvandrer, kort botid (<=5 år)": "innvandrer_kort", "Øvrige befolkning": "ovrig_befolkning"})
    df = df.groupby(['delbydel_id', 'date', 'bydel_id', 'delbydel_navn', 'bydel_navn']).sum().reset_index()
    value = 'innvandrer_kort'

    df['totalt_beboere'] = df['innvandrer_lang'] + df['ovrig_befolkning']  + df['innvandrer_kort']
    agg = {value: 'sum', 'totalt_beboere': 'sum'}
    df = Aggregate(agg).aggregate(df=df)
    df = Aggregate(agg).add_ratios(df=df,data_points= [value], ratio_of=[ 'totalt_beboere'])

    status = common.transform.status(df)
    historic = common.transform.historic(df)
    if type_of_ds == "historisk":
        create_ds(
            output_key, TemplateB(), ["innvandrer_kort"], graph_metadata, *historic
        )
    elif type_of_ds == "status":
        create_ds(
            output_key, TemplateA(), ["innvandrer_kort"], graph_metadata, *status
        )

def pivot_table(df, pivot_column, value_column):
    key_columns = list(
        filter(lambda x: x not in [pivot_column, value_column], list(df))
    )
    df_pivot = pd.concat(
        (df[key_columns], df.pivot(columns=pivot_column, values=value_column)), axis=1
    )
    return df_pivot.groupby(key_columns).sum().reset_index()


def create_ds(output_key, template, values, metadata, df):
    jsonl = Output(
        df=df, template=template, metadata=metadata, values=values
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handler(
        {
            "input": {
                "botid-ikke-vestlige": "raw/green/botid-ikke-vestlige/version=1/edition=20190525T183610/Botid_ikke_vestlige(1.1.2008-1.1.2019-v01).csv"
            },
            "output": "intermediate/green/levekar-innvandrere-ikke-vestlige-kort-status/version=1/edition=20190525T183610/",
            "config": {"type": "status"},
        },
        {},
    )
