import pandas as pd

import common.aws as common_aws
from common.aggregateV2 import Aggregate
import common.population_utils as population_utils
import common.transform as transform
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB

pd.set_option("display.max_rows", 1000)

graph_metadata = Metadata(
    heading="Levekår Trangbodde", series=[{"heading": "Trangbodde", "subheading": ""}]
)


def handle(event, context):
    s3_key_trangbodde = event["input"]["trangbodde"]
    s3_key_befolkning = event["input"]["befolkning-etter-kjonn-og-alder"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]

    trangbodde_raw = common_aws.read_from_s3(
        s3_key=s3_key_trangbodde, date_column="aar"
    )
    befolkning_raw = common_aws.read_from_s3(
        s3_key=s3_key_befolkning, date_column="aar"
    )

    datapoint = "antall_trangbodde"

    input_df = generate_input_df(trangbodde_raw, befolkning_raw, datapoint)

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df, [datapoint])

    elif type_of_ds == "status":
        output_list = output_status(input_df, [datapoint])

    if output_list:
        common_aws.write_to_intermediate(output_key=output_key, output_list=output_list)
        return f"Created {output_key}"


def generate_input_df(trangbodde_raw, befolkning_raw, data_point):
    population_df = population_utils.generate_population_df(befolkning_raw)

    agg = {"population": "sum"}
    population_district_df = Aggregate(agg).aggregate(df=population_df)

    input_df = pd.merge(
        trangbodde_raw,
        population_district_df,
        how="inner",
        on=["bydel_id", "date", "delbydel_id"],
    ).rename(columns={"bydel_navn_x": "bydel_navn", "delbydel_navn_x": "delbydel_navn"})
    input_df[f"{data_point}_ratio"] = input_df["andel_som_bor_trangt"] / 100
    input_df[data_point] = input_df["population"] * input_df[f"{data_point}_ratio"]

    # Exclude Marka, Sentrum and Uten registrert adresse
    input_df = input_df[~input_df["bydel_id"].isin(["16", "17", "99"])]

    return input_df[
        [
            "date",
            "bydel_id",
            "bydel_navn",
            "delbydel_id",
            "delbydel_navn",
            data_point,
            f"{data_point}_ratio",
        ]
    ]


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


if __name__ == "__main__":
    handle(
        {
            "input": {
                "trangbodde": "raw/green/trangbodde/version=1/edition=20190529T121303/Trangbodde(1.1.2015-1.1.2017-v01).csv",
                "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190523T211529/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv",
            },
            "output": "s3/key/or/prefix",
            "config": {"type": "status"},
        },
        None,
    )
    # handle(
    #     {
    #         "input": {
    #             "trangbodde": "raw/green/trangbodde/version=1/edition=20190529T121303/Trangbodde(1.1.2015-1.1.2017-v01).csv",
    #             "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190523T211529/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv",
    #         },
    #         "output": "s3/key/or/prefix",
    #         "config": {"type": "historic"},
    #     },
    #     None,
    # )
