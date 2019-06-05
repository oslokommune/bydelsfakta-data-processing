import pandas as pd

import common.transform as transform
import common.aws as common_aws
from common.aggregateV2 import Aggregate
from common.util import get_latest_edition_of
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB


graph_metadata = Metadata(
    heading="Personer fra 16 til 66 år med redusert funksjonsevne",
    series=[{"heading": "Redusert funksjonsevne", "subheading": ""}],
)

key_cols = ['date', 'delbydel_id', 'delbydel_navn', 'bydel_id', 'bydel_navn']

def handle(event, context):
    s3_key_redusert_funksjonsevne = event["input"]["redusert-funksjonsevne"]
    s3_key_botid_ikke_vestlige = event["input"]["botid-ikke-vestlige"]
    s3_key_lav_utdanning = event["input"]["lav-utdanning"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]

    redusert_funksjonsevne_raw = common_aws.read_from_s3(
        s3_key=s3_key_redusert_funksjonsevne, date_column="aar"
    )
    botid_ikke_vestlige_raw = common_aws.read_from_s3(
        s3_key=s3_key_botid_ikke_vestlige, date_column="aar"
    )
    lav_utdanning_raw = common_aws.read_from_s3(
        s3_key=s3_key_lav_utdanning, date_column="aar"
    )

    redusert_funksjonsevne_input_df = generate_redusert_funksjonsevne_df(redusert_funksjonsevne_raw)
    ikke_vestlig_kort_botid_input_df = generate_ikke_vestlig_innvandrer_kort_botid_df(botid_ikke_vestlige_raw)
    lav_utdanning_input_df = generate_lav_utdanning_df(lav_utdanning_raw)

    return
    # output_list = []
    # if type_of_ds == "historisk":
    #     output_list = output_historic(input_df, [data_point])
    #
    # elif type_of_ds == "status":
    #     output_list = output_status(input_df, [data_point])
    #
    # if output_list:
    #     common_aws.write_to_intermediate(output_key=output_key, output_list=output_list)
    #     return f"Created {output_key}"
    #
    # else:
    #     raise Exception("No data in outputlist")


def generate_lav_utdanning_df(lav_utdanning_raw):
    data_point = 'lav_utdanning'
    education_categories = [
        "ingen_utdanning_uoppgitt",
        "grunnskole",
        "videregaende",
        "universitet_hogskole_kort",
        "universitet_hogskole_lang",
    ]

    lav_utdanning_raw["total"] = lav_utdanning_raw[education_categories].sum(axis=1)
    lav_utdanning_raw[data_point] = lav_utdanning_raw[
        ["ingen_utdanning_uoppgitt", "grunnskole"]
    ].sum(axis=1)

    aggregations = {data_point: "sum", "total": "sum"}
    aggregator = Aggregate(aggregations)
    input_df = aggregator.aggregate(lav_utdanning_raw)

    input_df = aggregator.add_ratios(
        input_df, data_points=[data_point], ratio_of=["total"]
    )
    return input_df

def generate_ikke_vestlig_innvandrer_kort_botid_df(botid_ikke_vestlige_raw):
    value_cols_raw = [
        "asia_afrika_latin_amerika_og_ost_europa_utenfor_eu",
        "vest_europa_usa_canada_australia_og_new_zealand",
        "ost_europeiske_eu_land"]
    df = botid_ikke_vestlige_raw.drop(columns=["norge"])
    df['total'] = df[value_cols_raw].sum(axis=1)
    df['ikke_vestlig'] = df["asia_afrika_latin_amerika_og_ost_europa_utenfor_eu"]
    df = df.drop(columns=value_cols_raw)

    df = pivot_table(df, pivot_column="botid", value_columns=["total", "ikke_vestlig"])

    df['total_befolkning']= df[
        [('total', 'Innvandrer, kort botid (<=5 år)'),
         ('total', 'Innvandrer, lang botid (>5 år)'),
         ('total', 'Øvrige befolkning')]].sum(axis=1)

    df['ikke_vestlig_kort'] = df[('ikke_vestlig', 'Innvandrer, kort botid (<=5 år)')]

    aggregator = Aggregate({
        'ikke_vestlig_kort': 'sum',
        'total_befolkning': 'sum'
    })
    df = aggregator.aggregate(df)
    df = aggregator.add_ratios(df=df, data_points=['ikke_vestlig_kort'], ratio_of=['total_befolkning'])

    return df[[*key_cols, 'ikke_vestlig_kort', 'ikke_vestlig_kort_ratio']]

def generate_redusert_funksjonsevne_df(redusert_funksjonsevne_raw):
    data_point = "antall_redusert_funksjonsevne"
    input_df = redusert_funksjonsevne_raw.rename(
        columns={"antall_personer_med_redusert_funksjonsevne": data_point}
    )
    input_df[f"{data_point}_ratio"] = (
        input_df["andel_personer_med_redusert_funksjonsevne"] / 100
    )
    return input_df[[*key_cols, data_point, f'{data_point}_ratio']]

def output_historic(input_df, data_points):
    [input_df] = transform.historic(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateB()
    ).generate_output()

    return output


def output_status(input_df, data_points):
    [input_df] = transform.status(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateA()
    ).generate_output()
    return output


def pivot_table(df, pivot_column, value_columns):
    key_columns = list(
        filter(lambda x: x not in [pivot_column, *value_columns], list(df))
    )
    df_pivot = pd.concat(
        (df[key_columns], df.pivot(columns=pivot_column, values=value_columns)), axis=1
    )
    return df_pivot.groupby(key_columns).sum().reset_index()

if __name__ == "__main__":
    redusert_funksjonsevne_s3_key = get_latest_edition_of("redusert-funksjonsevne")
    botid_ikke_vestlige_s3_key = get_latest_edition_of("botid-ikke-vestlige")
    lav_utdanning_s3_key = get_latest_edition_of("lav-utdanning")
    handle(
        {
            "input": {
                "redusert-funksjonsevne": redusert_funksjonsevne_s3_key,
                "botid-ikke-vestlige": botid_ikke_vestlige_s3_key,
                "lav-utdanning": lav_utdanning_s3_key
            },
            "output": "s3/key/or/prefix",
            "config": {"type": "status"},
        },
        None,
    )
