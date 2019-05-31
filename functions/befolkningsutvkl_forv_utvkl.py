import numpy
import pandas

import common.aws
import common.population_utils
import common.transform
import common.transform_output
import common.util
from common import transform
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateH

column_names = ColumnNames()
sum = Aggregate("sum")


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    population = event["input"]["befolkning-etter-kjonn-og-alder"]
    population = common.aws.read_from_s3(population)

    dead = event["input"]["dode"]
    dead = common.aws.read_from_s3(dead)

    born = event["input"]["fodte"]
    born = common.aws.read_from_s3(born)

    immigration_sub_district = event["input"]["flytting-til-etter-alder"]
    immigration_sub_district = common.aws.read_from_s3(immigration_sub_district)

    emigration_sub_district = event["input"]["flytting-fra-etter-alder"]
    emigration_sub_district = common.aws.read_from_s3(emigration_sub_district)

    immigration_district = event["input"]["flytting-til-etter-inn-kat"]
    immigration_district = common.aws.read_from_s3(immigration_district)

    emigration_district = event["input"]["flytting-fra-etter-inn-kat"]
    emigration_district = common.aws.read_from_s3(emigration_district)

    pop_extrapolation = event["input"]["befolkingsframskrivninger"]
    pop_extrapolation = common.aws.read_from_s3(pop_extrapolation)

    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    if type_of_ds == "historisk":
        dfs = transform.historic(
            population,
            dead,
            born,
            immigration_sub_district,
            emigration_sub_district,
            immigration_district,
            emigration_district,
        )
    else:
        raise Exception("Type should be historisk")

    df = generate(*dfs, pop_extrapolation)
    write(df, output_key)
    return f"Complete: {output_key}"


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


def process_immigration(sub_district, district):
    to_oslo = "innflytting_til_oslo"
    between_districts = "innflytting_mellom_bydeler"
    between_sub_districts = "innflytting_mellom_delbydeler"

    immigration = join_to_migration(
        district_df=district,
        sub_district_df=sub_district,
        oslo=to_oslo,
        between_districts=between_districts,
        between_sub_districts=between_sub_districts,
        output_label="immigration",
    )
    return immigration


def process_emigration(sub_district, district):
    from_oslo = "utflytting_fra_oslo"
    between_districts = "utflytting_mellom_bydeler"
    between_sub_districts = "utflytting_mellom_delbydeler"

    return join_to_migration(
        district_df=district,
        sub_district_df=sub_district,
        oslo=from_oslo,
        between_districts=between_districts,
        between_sub_districts=between_sub_districts,
        output_label="emigration",
    )


def process_pop_extrapolation(pop_extrapolation):
    meta = pop_extrapolation[
        [column_names.date, column_names.district_id, column_names.district_name]
    ]
    meta.loc[:, column_names.sub_district_id] = numpy.nan
    meta.loc[:, column_names.sub_district_name] = numpy.nan
    meta.loc[:, "projection"] = pop_extrapolation.loc[:, "0":"99"].sum(1)
    return meta


def join_to_migration(
    district_df,
    sub_district_df,
    oslo,
    between_districts,
    between_sub_districts,
    output_label,
):

    sub_district = (
        sub_district_df.groupby(column_names.default_groupby_columns())[
            oslo, between_sub_districts
        ]
        .sum()
        .reset_index()
    )
    sub_district[output_label] = (
        sub_district[oslo] + sub_district[between_sub_districts]
    )

    district = (
        district_df.groupby(
            [column_names.date, column_names.district_id, column_names.district_name]
        )[oslo, between_districts]
        .sum()
        .reset_index()
    )
    district[output_label] = district[oslo] + district[between_districts]
    district[column_names.sub_district_id] = numpy.nan
    district[column_names.sub_district_name] = numpy.nan

    oslo = district.groupby([column_names.date])[output_label].sum().reset_index()
    oslo[column_names.sub_district_id] = numpy.nan
    oslo[column_names.sub_district_name] = numpy.nan
    oslo[column_names.district_name] = "Oslo i alt"
    oslo[column_names.district_id] = "00"

    aggregated = pandas.concat([district, oslo, sub_district])[
        column_names.default_groupby_columns() + [output_label]
    ]
    return aggregated


def generate(
    population,
    dead,
    born,
    immigration_sub_district,
    emigration_sub_district,
    immigration_district,
    emigration_district,
    pop_extrapolation,
):
    population = process_population(population)
    dead = process_dead(dead)
    born = process_born(born)
    immigration = process_immigration(immigration_sub_district, immigration_district)
    emigration = process_emigration(emigration_sub_district, emigration_district)
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
    handler(
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
                "flytting-til-etter-inn-kat": common.util.get_latest_edition_of(
                    "flytting-til-etter-inn-kat"
                ),
                "flytting-fra-etter-inn-kat": common.util.get_latest_edition_of(
                    "flytting-fra-etter-inn-kat"
                ),
                "befolkingsframskrivninger": common.util.get_latest_edition_of(
                    "befolkingsframskrivninger"
                ),
            },
            "output": "intermediate/green/befolkningsutvikling_og_forventet_utvikling/version=1/edition=20190422T211529/",
            "config": {"type": "historisk"},
        },
        {},
    )
