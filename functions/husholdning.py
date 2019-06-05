import pandas as pd

from common import aws, util, transform
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC

value_columns = [
    "enfamiliehusholdninger_med_voksne_barn",
    "flerfamiliehusholdninger_med_smaa_barn",
    "flerfamiliehusholdninger_med_store_barn",
    "flerfamiliehusholdninger_uten_barn_0_til_17_aar",
    "mor_eller_far_med_smaa_barn",
    "mor_eller_far_med_store_barn",
    "par_med_smaa_barn",
    "par_med_store_barn",
    "par_uten_hjemmeboende_barn",
]

column_names = ColumnNames()


def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["husholdninger-med-barn"]
    output_key = event["output"]
    type = event["config"]["type"]
    source = aws.read_from_s3(s3_key=s3_key)

    series = [
        {"heading": "Aleneboende", "subheading": ""},
        {"heading": "Øvrige husholdninger uten barn", "subheading": ""},
        {"heading": "Husholdninger med barn", "subheading": ""},
    ]

    metadata = Metadata(heading="Husholdning total", series=series)

    if type == "status":
        [df] = transform.status(source)
        template = TemplateA()

    elif type == "historisk":
        [df] = transform.historic(source)
        template = TemplateC()
    else:
        raise Exception("Wrong dataset type")

    df = process(df)
    output = Output(
        df=df,
        values=["single_adult", "no_children", "with_children"],
        template=template,
        metadata=metadata,
    )

    write(output=output, output_key=output_key)
    return f"Complete: {output_key}"


def process(source):
    loners = source[column_names.default_groupby_columns() + ["aleneboende"]]
    loners = loners.groupby(column_names.default_groupby_columns()).sum().reset_index()
    loners = loners.rename(columns={"aleneboende": "single_adult"})

    rest = source.drop(columns=["aleneboende"])

    agg = Aggregate("sum")
    meta = rest[column_names.default_groupby_columns() + ["barn_i_husholdningen"]]
    meta["total"] = rest[value_columns].sum(axis=1)

    household_pivot = pd.pivot_table(
        meta,
        index=column_names.default_groupby_columns(),
        columns=["barn_i_husholdningen"],
        values=["total"],
    )
    household_pivot.columns = household_pivot.columns.droplevel(0)
    household_pivot = household_pivot.reset_index().rename_axis(None, axis=1)

    household_pivot["with_children"] = (
        household_pivot["1 barn i HH"]
        + household_pivot["2 barn i HH"]
        + household_pivot["3 barn i HH"]
        + household_pivot["4 barn eller mer"]
    )
    household_pivot = household_pivot.rename(columns={"Ingen barn i HH": "no_children"})

    househoulds = household_pivot[
        column_names.default_groupby_columns() + ["no_children", "with_children"]
    ]
    merged = agg.merge_all(loners, househoulds, how="outer")

    merged["with_children"] = merged["with_children"].fillna(0)

    aggregated = agg.aggregate(merged)

    aggregated = agg.add_ratios(
        aggregated,
        data_points=["no_children", "single_adult", "with_children"],
        ratio_of=["no_children", "single_adult", "with_children"],
    )
    return aggregated


def write(output: Output, output_key):
    aws.write_to_intermediate(
        output_list=output.generate_output(), output_key=output_key
    )


if __name__ == "__main__":
    handle(
        {
            "input": {
                "husholdninger-med-barn": util.get_latest_edition_of(
                    "husholdninger-med-barn"
                )
            },
            "config": {"type": "historisk"},
        },
        {},
    )
