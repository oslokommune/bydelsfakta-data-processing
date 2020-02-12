import numpy
import pandas
from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aws
import common.population_utils
import common.transform
import common.transform_output
import common.util
from common import transform
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateH
from common.event import event_handler

patch_all()

column_names = ColumnNames()
sum = Aggregate("sum")


@logging_wrapper("befolkningsutvkl_forv_utvkl__old")
@xray_recorder.capture("handler_old")
def handler_old(event, context):
    """ Assuming we recieve a complete s3 key"""
    population = event["input"]["befolkning-etter-kjonn-og-alder"]
    population = common.aws.read_from_s3(population)

    dead = event["input"]["dode"]
    dead = common.aws.read_from_s3(dead)

    born = event["input"]["fodte"]
    born = common.aws.read_from_s3(born)

    immigration = event["input"]["flytting-til-etter-alder"]
    immigration = common.aws.read_from_s3(immigration)

    emigration = event["input"]["flytting-fra-etter-alder"]
    emigration = common.aws.read_from_s3(emigration)

    pop_extrapolation = event["input"]["befolkningsframskrivninger"]
    pop_extrapolation = common.aws.read_from_s3(pop_extrapolation)

    output_key = event["output"]
    type_of_ds = event["config"]["type"]

    start(
        population,
        dead,
        born,
        immigration,
        emigration,
        pop_extrapolation,
        output_key,
        type_of_ds,
    )

    return f"Complete: {output_key}"


@logging_wrapper("befolkningsutvkl_forv_utvkl")
@xray_recorder.capture("event_handler")
@event_handler(
    population="befolkning-etter-kjonn-og-alder",
    dead="dode",
    born="fodte",
    immigration="flytting-til-etter-alder",
    emigration="flytting-fra-etter-alder",
    pop_extrapolation="befolkningsframskrivninger",
)
def _start(*args, **kwargs):
    start(*args, **kwargs)


def start(
    population,
    dead,
    born,
    immigration,
    emigration,
    pop_extrapolation,
    output_prefix,
    type_of_ds,
):
    if type_of_ds == "historisk":
        dfs = transform.historic(population, dead, born, immigration, emigration)
    else:
        raise Exception("Type should be historisk")

    df = generate(*dfs, pop_extrapolation)
    write(df, output_prefix)


def process_population(population):
    population = population.dropna()
    aggregate = sum.aggregate(
        common.population_utils.generate_population_df(population)
    )

    aggregate.loc[aggregate["bydel_id"] == "00", "change"] = aggregate[
        aggregate["bydel_id"] == "00"
    ]["population"].pct_change()

    aggregate.loc[
        (aggregate["bydel_id"] != "00") & (aggregate["delbydel_id"].isna()), "change"
    ] = (
        aggregate.loc[
            (aggregate["bydel_id"] != "00") & (aggregate["delbydel_id"].isna())
        ]
        .groupby([column_names.district_name, column_names.district_id])["population"]
        .apply(lambda x: x.pct_change())
    )

    aggregate.loc[aggregate["delbydel_id"].notna(), "change"] = (
        aggregate.loc[aggregate["delbydel_id"].notna()]
        .groupby(
            [
                column_names.district_name,
                column_names.district_id,
                column_names.sub_district_name,
                column_names.sub_district_id,
            ]
        )["population"]
        .apply(lambda x: x.pct_change())
    )

    return aggregate


def process_dead(dead):
    dead = dead.dropna()
    return sum.aggregate(dead)


def process_born(born):
    born = born.dropna()
    return sum.aggregate(born)


def process_migration(df, oslo, between_districts, output_label):
    district = (
        df.groupby(
            [column_names.date, column_names.district_id, column_names.district_name]
        )[oslo, between_districts]
        .sum()
        .reset_index()
    )
    district[output_label] = district[oslo] + district[between_districts]
    district[column_names.sub_district_id] = numpy.nan
    district[column_names.sub_district_name] = numpy.nan

    district = district.astype(
        {"delbydel_id": object, "delbydel_navn": object, "date": int}
    )

    oslo = district.groupby([column_names.date])[output_label].sum().reset_index()
    oslo[column_names.sub_district_id] = numpy.nan
    oslo[column_names.sub_district_name] = numpy.nan
    oslo[column_names.district_name] = "Oslo i alt"
    oslo[column_names.district_id] = "00"

    aggregated = pandas.concat([oslo, district])[
        column_names.default_groupby_columns() + [output_label]
    ]

    return aggregated


def process_pop_extrapolation(pop_extrapolation):
    meta = pop_extrapolation[
        [column_names.date, column_names.district_id, column_names.district_name]
    ]
    meta.loc[:, column_names.sub_district_id] = numpy.nan
    meta.loc[:, column_names.sub_district_name] = numpy.nan
    meta.loc[:, "projection"] = pop_extrapolation.loc[:, "0":"99"].sum(1)

    oslo = meta.groupby([column_names.date])["projection"].sum().reset_index()
    oslo[column_names.district_id] = "00"
    oslo[column_names.district_name] = "Oslo i alt"
    meta = pandas.concat([meta, oslo])
    return meta


def generate(population, dead, born, immigration, emigration, pop_extrapolation):
    population = process_population(population)
    dead = process_dead(dead)
    born = process_born(born)
    immigration = process_migration(
        df=immigration,
        oslo="innflytting_til_oslo",
        between_districts="innflytting_mellom_bydeler",
        output_label="immigration",
    )
    emigration = process_migration(
        df=emigration,
        oslo="utflytting_fra_oslo",
        between_districts="utflytting_mellom_bydeler",
        output_label="emigration",
    )
    pop_extrapolation = process_pop_extrapolation(pop_extrapolation)

    agg = Aggregate({})
    merged = agg.merge_all(population, dead, born, immigration, emigration, how="outer")
    merged = merged.astype(
        {
            "antall_dode": pandas.Int64Dtype(),
            "antall_fodte": pandas.Int64Dtype(),
            "immigration": pandas.Int64Dtype(),
            "emigration": pandas.Int64Dtype(),
            "population": pandas.Int64Dtype(),
        }
    )
    return pandas.concat([merged, pop_extrapolation])


def write(df, output_key):
    heading = "Befolkningsutvikling og fremskrivning"
    series = [
        {"heading": "Befolkningsutvikling", "subheading": ""},
        {"heading": "Befolkningsfremskrivning", "subheading": ""},
    ]
    # To json : convert df to list of json objects
    df = df.rename(columns={"antall_dode": "deaths", "antall_fodte": "births"})

    jsonl = Output(
        df=df,
        values=[
            ["deaths", "births", "emigration", "immigration", "population", "change"],
            ["projection"],
        ],
        template=TemplateH(),
        metadata=Metadata(heading=heading, series=series),
    ).generate_output()

    common.aws.write_to_intermediate(output_key=output_key, output_list=jsonl)
    return output_key


if __name__ == "__main__":
    handler_old(
        {
            "input": {
                "befolkning-etter-kjonn-og-alder": common.util.get_latest_edition_of(
                    "befolkning-etter-kjonn-og-alder", confidentiality="yellow"
                ),
                "dode": common.util.get_latest_edition_of("dode"),
                "fodte": common.util.get_latest_edition_of("fodte"),
                "flytting-fra-etter-alder": common.util.get_latest_edition_of(
                    "flytting-fra-etter-alder"
                ),
                "flytting-til-etter-alder": common.util.get_latest_edition_of(
                    "flytting-til-etter-alder"
                ),
                "befolkningsframskrivninger": common.util.get_latest_edition_of(
                    "befolkningsframskrivninger"
                ),
            },
            "output": "intermediate/green/befolkningsutvikling_og_forventet_utvikling/version=1/edition=20190422T211529/",
            "config": {"type": "historisk"},
        },
        {},
    )
