import pandas as pd
from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.transform as transform
import common.aws as common_aws
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateK
from common.population_utils import generate_population_df
from common.event import event_handler

patch_all()

value_columns = [
    "antall_redusert_funksjonsevne",
    "ikke_vestlig_kort",
    "lav_utdanning",
    "antall_fattige_barnehusholdninger",
    "antall_ikke_sysselsatte",
    "ikke_fullfort_vgs",
    "dodsrate",
    "antall_trangbodde",
]


graph_metadata = Metadata(
    heading="Personer fra 16 til 66 år med redusert funksjonsevne",
    series=[
        {"heading": "Redusert funksjonsevne", "subheading": ""},
        {
            "heading": "Innvandrere fra Afrika, Asia mv. med kort botid",
            "subheading": "",
        },
        {"heading": "Lav utdanning", "subheading": ""},
        {"heading": "Lavinntektshusholdninger med barn", "subheading": ""},
        {"heading": "Ikke sysselsatte", "subheading": ""},
        {"heading": "Ikke fullført vgs", "subheading": ""},
        {"heading": "Dødelighet", "subheading": ""},
        {"heading": "Trangbodde", "subheading": ""},
    ],
)

key_cols = ColumnNames().default_groupby_columns()


@logging_wrapper("levekar_totalt")
@xray_recorder.capture("event_handler")
@event_handler(
    redusert_funksjonsevne_raw="redusert-funksjonsevne",
    botid_ikke_vestlige_raw="botid-ikke-vestlige",
    lav_utdanning_raw="lav-utdanning",
    fattige_husholdninger_raw="fattige-husholdninger",
    sysselsatte_raw="sysselsatte",
    befolkning_raw="befolkning-etter-kjonn-og-alder",
    ikke_fullfort_vgs_raw="ikke-fullfort-vgs",
    dodsrater_raw="dodsrater",
    trangbodde_raw="trangbodde",
)
def start(
    redusert_funksjonsevne_raw,
    botid_ikke_vestlige_raw,
    lav_utdanning_raw,
    fattige_husholdninger_raw,
    sysselsatte_raw,
    befolkning_raw,
    ikke_fullfort_vgs_raw,
    dodsrater_raw,
    trangbodde_raw,
    output_prefix,
    type_of_ds,
):
    redusert_funksjonsevne_input_df = generate_redusert_funksjonsevne_df(
        redusert_funksjonsevne_raw
    )
    ikke_vestlig_kort_botid_input_df = generate_ikke_vestlig_innvandrer_kort_botid_df(
        botid_ikke_vestlige_raw, befolkning_raw.copy()
    )
    lav_utdanning_input_df = generate_lav_utdanning_df(lav_utdanning_raw)
    fattige_barnehusholdninger_input_df = generate_fattige_barnehusholdninger_df(
        fattige_husholdninger_raw
    )
    ikke_sysselsatte_input_df = generate_ikke_sysselsatte_df(
        sysselsatte_raw, befolkning_raw.copy()
    )
    ikke_fullfort_vgs_input_df = generate_ikke_fullfort_vgs_df(ikke_fullfort_vgs_raw)
    dodsrater_input_df = generate_dosrater_df(dodsrater_raw)
    trangbodde_input_df = generate_trangbodde_input_df(
        trangbodde_raw, befolkning_raw.copy()
    )

    input_df = Aggregate({}).merge_all(
        *[
            redusert_funksjonsevne_input_df,
            ikke_vestlig_kort_botid_input_df,
            lav_utdanning_input_df,
            fattige_barnehusholdninger_input_df,
            ikke_sysselsatte_input_df,
            ikke_fullfort_vgs_input_df,
            dodsrater_input_df,
            trangbodde_input_df,
        ]
    )
    output_list = []
    if type_of_ds == "status":
        output_list = output_status(input_df, value_columns)

    else:
        raise Exception(f"Invalid config type: {type_of_ds}")

    if output_list:
        common_aws.write_to_intermediate(
            output_key=output_prefix, output_list=output_list
        )
    else:
        raise Exception("No data in outputlist")


def add_relative_ratio(df, ratio_col):
    df = df[~df["bydel_id"].isin(["16", "17", "99"])]
    oslo_ratio_col = f"{ratio_col}_oslo"
    district_ratio_col = f"{ratio_col}_district"
    oslo_total_df = df[df["bydel_id"] == "00"]
    result_df = pd.DataFrame(columns=[*key_cols, district_ratio_col, oslo_ratio_col])
    for (date, district_id), group_df in df.groupby(by=["date", "bydel_id"]):
        tmp_df = group_df.copy()
        district_mean = tmp_df[tmp_df["delbydel_id"].isnull()][ratio_col].unique()[0]
        oslo_mean = oslo_total_df[oslo_total_df["date"] == date][ratio_col].unique()[0]
        tmp_df[district_ratio_col] = tmp_df[ratio_col] / district_mean
        tmp_df[oslo_ratio_col] = tmp_df[ratio_col] / oslo_mean
        result_df = result_df.append(
            tmp_df[[*key_cols, district_ratio_col, oslo_ratio_col]]
        )

    return result_df


def generate_trangbodde_input_df(trangbodde_raw, befolkning_raw):
    data_point = "antall_trangbodde"
    population_df = generate_population_df(befolkning_raw)

    agg = {"population": "sum"}
    population_district_df = Aggregate(agg).aggregate(df=population_df)

    df = pd.merge(
        trangbodde_raw,
        population_district_df[["date", "bydel_id", "delbydel_id", "population"]],
        how="inner",
        on=["bydel_id", "date", "delbydel_id"],
    )

    df[f"{data_point}_ratio"] = df["andel_som_bor_trangt"] / 100
    df[data_point] = df["population"] * df[f"{data_point}_ratio"]

    return add_relative_ratio(df, f"{data_point}_ratio")


def generate_dosrater_df(dodsrater_raw):
    data_point = "dodsrate"
    df = dodsrater_raw

    df[f"{data_point}_ratio"] = df[data_point] / 100

    return add_relative_ratio(df, f"{data_point}_ratio")


def generate_ikke_fullfort_vgs_df(ikke_fullfort_vgs_raw):
    df = ikke_fullfort_vgs_raw
    data_point = "ikke_fullfort_vgs"
    df = df.rename(
        columns={"antall_personer_ikke_fullfort_i_lopet_av_5_aar": data_point}
    )

    data_point_ratio = f"{data_point}_ratio"
    df[data_point_ratio] = df["andelen_som_ikke_har_fullfort_i_lopet_av_5_aar"] / 100

    return add_relative_ratio(df, data_point_ratio)


def generate_ikke_sysselsatte_df(sysselsatte_raw, befolkning_raw):
    data_point = "antall_ikke_sysselsatte"
    population_col = "population"

    # Numbers for "sysselsatte" is only for age 30 to 59
    befolkning_df = generate_population_df(befolkning_raw, min_age=30, max_age=59)

    sub_districts = befolkning_df["delbydel_id"].unique()

    sysselsatte_df = sysselsatte_raw
    # Value for date in "sysselsatte" was measured in 4th. quarter of 2017, while date for "befolkning" was measured 1.1.2018.
    sysselsatte_df["date"] = sysselsatte_df["date"] + 1
    sysselsatte_df = sysselsatte_df[sysselsatte_df["delbydel_id"].isin(sub_districts)]

    sysselsatte_befolkning_df = pd.merge(
        sysselsatte_df,
        befolkning_df[["date", "delbydel_id", "population"]],
        how="inner",
        on=["date", "delbydel_id"],
    )
    # Ignoring "Marka", "Sentrum" and "Uten registrert adresse"
    ignore_districts = ["16", "17", "99"]
    sysselsatte_befolkning_df = sysselsatte_befolkning_df[
        ~sysselsatte_befolkning_df["bydel_id"].isin(ignore_districts)
    ]

    sysselsatte_befolkning_df[data_point] = (
        sysselsatte_befolkning_df[population_col]
        - sysselsatte_befolkning_df["antall_sysselsatte"]
    )

    agg = Aggregate({population_col: "sum", data_point: "sum"})
    aggregated_df = agg.aggregate(sysselsatte_befolkning_df)

    input_df = agg.add_ratios(
        aggregated_df, data_points=[data_point], ratio_of=[population_col]
    )
    return add_relative_ratio(input_df, f"{data_point}_ratio")


def generate_fattige_barnehusholdninger_df(fattige_husholdninger_raw):
    data_point = "antall_fattige_barnehusholdninger"
    data_point_ratio = f"{data_point}_ratio"
    df = fattige_husholdninger_raw

    df[data_point_ratio] = (
        df["husholdninger_med_barn_under_18_aar_eu_skala_andel"] / 100
    )
    return add_relative_ratio(df, data_point_ratio)


def generate_lav_utdanning_df(lav_utdanning_raw):
    data_point = "lav_utdanning"
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
    return add_relative_ratio(input_df, f"{data_point}_ratio")


def generate_ikke_vestlig_innvandrer_kort_botid_df(
    botid_ikke_vestlige_raw, befolkning_raw
):
    data_point = "ikke_vestlig_kort"
    kort_botid = "Innvandrer, kort botid (<=5 år)"
    ikke_vestlig = "asia_afrika_latin_amerika_og_ost_europa_utenfor_eu"

    df = botid_ikke_vestlige_raw.drop(columns=["norge"])
    df = pivot_table(df, "botid", ikke_vestlig)
    df[data_point] = df[kort_botid]

    aggregator = Aggregate({data_point: "sum"})
    df = aggregator.aggregate(df)

    population_df = generate_population_df(befolkning_raw)
    population_district_df = Aggregate({"population": "sum"}).aggregate(
        df=population_df
    )

    df = pd.merge(
        df,
        population_district_df[["date", "bydel_id", "delbydel_id", "population"]],
        how="inner",
        on=["bydel_id", "date", "delbydel_id"],
    )

    df = aggregator.add_ratios(
        df=df, data_points=["ikke_vestlig_kort"], ratio_of=["population"]
    )

    return add_relative_ratio(df, "ikke_vestlig_kort_ratio")


def generate_redusert_funksjonsevne_df(redusert_funksjonsevne_raw):
    data_point = "antall_redusert_funksjonsevne"
    input_df = redusert_funksjonsevne_raw.rename(
        columns={"antall_personer_med_redusert_funksjonsevne": data_point}
    )
    input_df[f"{data_point}_ratio"] = (
        input_df["andel_personer_med_redusert_funksjonsevne"] / 100
    )
    return add_relative_ratio(input_df, f"{data_point}_ratio")


def output_status(input_df, data_points):
    [input_df] = transform.status(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateK()
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
