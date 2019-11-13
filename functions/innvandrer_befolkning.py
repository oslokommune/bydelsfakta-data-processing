import pandas as pd

from common.aws import write_to_intermediate
import common.transform_output
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC, TemplateB
from common.transform import status, historic


METADATA = {
    "alle_status": Metadata(
        heading="Innvandring befolkning",
        series=[
            {"heading": "Innvandrer", "subheading": "kort botid (<=5 år)"},
            {"heading": "Innvandrer", "subheading": "lang botid (>5 år)"},
            {"heading": "Norskfødt", "subheading": "med innvandrerforeldre"},
        ],
    ),
    "alle_historisk": Metadata(
        heading="Innvandring befolkning",
        series=[
            {"heading": "Innvandrer", "subheading": "kort botid (<=5 år)"},
            {"heading": "Innvandrer", "subheading": "lang botid (>5 år)"},
            {"heading": "Norskfødt", "subheading": "med innvandrerforeldre"},
            {"heading": "Totalt", "subheading": ""},
        ],
    ),
    "kort_status": Metadata(heading="Innvandrer med kort botid (<=5 år)", series=[]),
    "kort_historisk": Metadata(heading="Innvandrer med kort botid (<=5 år)", series=[]),
    "lang_status": Metadata(heading="Innvandrer med lang botid (>5 år)", series=[]),
    "lang_historisk": Metadata(heading="Innvandrer med lang botid (>5 år)", series=[]),
    "to_foreldre_status": Metadata(
        heading="Norskfødt med innvandrerforeldre", series=[]
    ),
    "to_foreldre_historisk": Metadata(
        heading="Norskfødt med innvandrerforeldre", series=[]
    ),
}

DATA_POITNS = {
    "alle_status": ["short", "long", "two_parents"],
    "alle_historisk": ["short", "long", "two_parents", "total_cat"],
    "kort_status": ["short"],
    "kort_historisk": ["short"],
    "lang_status": ["long"],
    "lang_historisk": ["long"],
    "to_foreldre_status": ["two_parents"],
    "to_foreldre_historisk": ["two_parents"],
}


def read_from_s3(origin_by_age_key, botid_key, befolkning_key):
    origin_by_age = common.aws.read_from_s3(origin_by_age_key)
    origin_by_age = origin_by_age[origin_by_age["delbydel_id"].notnull()]

    livage = common.aws.read_from_s3(botid_key)
    livage = livage[livage["delbydel_id"].notnull()]

    population_df = common.aws.read_from_s3(befolkning_key)
    population_df = population_df[population_df["delbydel_id"].notnull()]
    population_total = population_df.loc[:, "date":"kjonn"]
    population_total["total"] = population_df.loc[:, "0":].sum(axis=1)

    return origin_by_age, livage, population_total


def prepare(origin_by_age, livage, population_df):
    population_df = population(population_df)

    origin = by_parents(origin_by_age)
    livage = by_liveage(livage)

    merge = pd.merge(
        origin,
        livage,
        on=["date", "delbydel_id", "delbydel_navn", "bydel_id", "bydel_navn"],
    )
    merge = pd.merge(merge, population_df, on=["date", "delbydel_id", "bydel_id"])
    return merge


def population(df):
    df = df.groupby(["date", "bydel_id", "delbydel_id"])["total"].sum()
    return df.reset_index()


def by_parents(df):
    result = df.loc[:, "date":"bydel_navn"]
    result["two_parents"] = df.norskfodt_med_innvandrerforeldre
    result["one_parent"] = df.norskfodt_med_en_utenlandskfodt_forelder
    result = result.groupby(
        ["date", "delbydel_id", "delbydel_navn", "bydel_id", "bydel_navn"]
    ).sum()
    return result.reset_index()


def by_liveage(liveage):
    meta_columns = ["date", "delbydel_id", "delbydel_navn", "bydel_id", "bydel_navn"]
    liveage = liveage.groupby(
        ["date", "delbydel_id", "delbydel_navn", "bydel_id", "bydel_navn", "botid"]
    ).sum()
    total = (
        liveage[
            [
                "asia_afrika_latin_amerika_og_ost_europa_utenfor_eu",
                "norge",
                "vest_europa_usa_canada_australia_og_new_zealand",
                "ost_europeiske_eu_land",
            ]
        ]
        .sum(axis=1)
        .reset_index()
    )
    pivot = total.pivot_table(index=meta_columns, columns="botid", values=0).drop(
        columns="Øvrige befolkning"
    )
    pivot = pivot.rename(
        columns={
            "Innvandrer, kort botid (<=5 år)": "short",
            "Innvandrer, lang botid (>5 år)": "long",
        }
    )

    return pivot.reset_index()


def generate(origin_by_age_df, livage_df, population_df):
    # Create the df with only subdistricts
    sub_districts = prepare(
        origin_by_age=origin_by_age_df, livage=livage_df, population_df=population_df
    )

    aggregate_config = {
        "one_parent": "sum",
        "two_parents": "sum",
        "short": "sum",
        "long": "sum",
        "total": "sum",
    }
    agg_class = Aggregate(aggregate_config=aggregate_config)

    aggregated = agg_class.aggregate(sub_districts)

    aggregated["total_cat"] = (
        aggregated["two_parents"] + aggregated["short"] + aggregated["long"]
    )

    with_ratios = agg_class.add_ratios(
        aggregated, ["two_parents", "short", "long", "total_cat"], ["total_cat"]
    )
    result = with_ratios.drop(columns=["total"])
    return result


def write(output_list, output_key):
    common.aws.write_to_intermediate(output_key=output_key, output_list=output_list)


def handler(event, context):
    befolkning_key = event["input"]["befolkning-etter-kjonn-og-alder"]
    botid_not_western = event["input"]["botid-ikke-vestlige"]
    origin_by_age_key = event["input"]["innvandrer-befolkningen-0-15-ar"]
    print("# Handeling event #")
    print(event)
    dataset_type = event["config"]["type"]
    output_s3_key = event["output"]

    source = read_from_s3(
        origin_by_age_key=origin_by_age_key,
        botid_key=botid_not_western,
        befolkning_key=befolkning_key,
    )

    df_status = status(*source)
    df_historic = historic(*source)

    generated_status = generate(*df_status)
    generated_historic = generate(*df_historic)

    if dataset_type == "alle_status":
        create_ds(
            output_s3_key,
            df=generated_status,
            template=TemplateA(),
            type_of_ds=dataset_type,
        )
    elif dataset_type == "alle_historisk":
        create_ds(
            output_s3_key,
            df=generated_historic,
            template=TemplateC(),
            type_of_ds=dataset_type,
        )
    elif dataset_type == "kort_status":
        create_ds(
            output_s3_key,
            df=generated_status,
            template=TemplateA(),
            type_of_ds=dataset_type,
        )
    elif dataset_type == "kort_historisk":
        create_ds(
            output_s3_key,
            df=generated_historic,
            template=TemplateB(),
            type_of_ds=dataset_type,
        )
    elif dataset_type == "lang_status":
        create_ds(
            output_s3_key,
            df=generated_status,
            template=TemplateA(),
            type_of_ds=dataset_type,
        )
    elif dataset_type == "lang_historisk":
        create_ds(
            output_s3_key,
            df=generated_historic,
            template=TemplateB(),
            type_of_ds=dataset_type,
        )
    elif dataset_type == "to_foreldre_status":
        create_ds(
            output_s3_key,
            df=generated_status,
            template=TemplateA(),
            type_of_ds=dataset_type,
        )
    elif dataset_type == "to_foreldre_historisk":
        create_ds(
            output_s3_key,
            df=generated_historic,
            template=TemplateB(),
            type_of_ds=dataset_type,
        )

    return f"Complete: {output_s3_key}"


def create_ds(output_key, template, type_of_ds, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=DATA_POITNS[type_of_ds],
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handler(
        {
            "input": {
                "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190524T133230/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv",
                "botid-ikke-vestlige": "raw/green/botid-ikke-vestlige/version=1/edition=20190524T094012/Botid_ikke_vestlige(1.1.2008-1.1.2019-v01).csv",
                "innvandrer-befolkningen-0-15-ar": "raw/green/innvandrer-befolkningen-0-15-ar/version=1/edition=20190523T211529/Landbakgrunn_etter_alder(1.1.2008-1.1.2019-v01).csv",
            },
            "output": "intermediate/green/innvandrer-befolkningen-historisk/version=1/edition=20190525T143000/",
            "config": {"type": "to_foreldre_historisk"},
        },
        {},
    )
