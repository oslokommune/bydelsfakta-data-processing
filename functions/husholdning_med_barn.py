import pandas as pd

from common import aws, util, transform
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC

value_columns = [
    "par_uten_barn",
    "par_med_barn",
    "mor_far_med_barn",
    "flerfamiliehusholdninger",
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
        {"heading": "Par uten barn", "subheading": ""},
        {"heading": "Par med barn", "subheading": ""},
        {"heading": "Mor eller far", "subheading": "med barn"},
        {"heading": "Flerfamiliehusholdninger", "subheading": ""},
    ]

    metadata = Metadata(heading="Husholdninger etter husholdningstype", series=series)

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
        values=[
            "single_adult",
            "no_children",
            "one_child",
            "two_child",
            "three_or_more",
        ],
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

    rest["par_uten_barn"] = rest["par_uten_hjemmeboende_barn"]
    rest["par_med_barn"] = rest["par_med_smaa_barn"] + rest["par_med_store_barn"]
    rest["mor_far_med_barn"] = (
        rest["mor_eller_far_med_smaa_barn"] + rest["mor_eller_far_med_store_barn"]
    )
    rest["flerfamiliehusholdninger"] = (
        rest["enfamiliehusholdninger_med_voksne_barn"]
        + rest["flerfamiliehusholdninger_med_smaa_barn"]
        + rest["flerfamiliehusholdninger_med_store_barn"]
        + rest["flerfamiliehusholdninger_uten_barn_0_til_17_aar"]
    )

    rest = rest.drop(
        columns=[
            "par_uten_hjemmeboende_barn",
            "par_med_smaa_barn",
            "par_med_store_barn",
            "mor_eller_far_med_smaa_barn",
            "mor_eller_far_med_store_barn",
            "enfamiliehusholdninger_med_voksne_barn",
            "flerfamiliehusholdninger_med_store_barn",
            "flerfamiliehusholdninger_med_smaa_barn",
            "flerfamiliehusholdninger_uten_barn_0_til_17_aar",
        ]
    )

    agg = Aggregate("sum")
    meta = rest[column_names.default_groupby_columns() + ["barn_i_husholdningen"]]
    meta["total"] = rest[value_columns].sum(axis=1)

    household_pivot = pd.pivot_table(
        meta,
        index=column_names.default_groupby_columns(),
        columns=["barn_i_husholdningen"],
        values=["total"],
    )

    household_pivot = household_pivot.fillna(0)

    household_pivot.columns = household_pivot.columns.droplevel(0)
    household_pivot = household_pivot.reset_index().rename_axis(None, axis=1)

    household_pivot["one_child"] = household_pivot["1 barn i HH"]
    household_pivot["two_child"] = household_pivot["2 barn i HH"]
    household_pivot["three_or_more"] = (
        household_pivot["3 barn i HH"] + household_pivot["4 barn eller mer"]
    )

    household_pivot = household_pivot.rename(columns={"Ingen barn i HH": "no_children"})

    househoulds = household_pivot[
        column_names.default_groupby_columns()
        + ["no_children", "one_child", "two_child", "three_or_more"]
    ]
    merged = agg.merge_all(loners, househoulds, how="outer")
    aggregated = agg.aggregate(merged)

    aggregated = agg.add_ratios(
        aggregated,
        data_points=[
            "no_children",
            "single_adult",
            "one_child",
            "two_child",
            "three_or_more",
        ],
        ratio_of=[
            "no_children",
            "single_adult",
            "one_child",
            "two_child",
            "three_or_more",
        ],
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
            "output": "intermediate/green/husholdninger-totalt-status/version=1/edition=20190819T110202/",
            "config": {"type": "status"},
        },
        {},
    )
