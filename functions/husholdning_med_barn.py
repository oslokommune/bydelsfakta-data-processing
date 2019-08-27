import pandas as pd

from common import aws, util
from common.transform import status, historic
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB, TemplateC


METADATA = {
    "alle_status": Metadata(
        heading="Husholdninger med barn",
        series=[
            {"heading": "Husholdninger", "subheading": "med 1 barn"},
            {"heading": "Husholdninger", "subheading": "med 2 barn"},
            {"heading": "Husholdninger", "subheading": "med 3 barn eller flere"},
        ],
    ),
    "alle_historisk": Metadata(
        heading="Husholdninger med barn",
        series=[
            {"heading": "Husholdninger", "subheading": "med 1 barn"},
            {"heading": "Husholdninger", "subheading": "med 2 barn"},
            {"heading": "Husholdninger", "subheading": "med 3 barn eller flere"},
        ],
    ),
    "1barn_status": Metadata(heading="Husholdninger med 1 barn", series=[]),
    "1barn_historisk": Metadata(heading="Husholdninger med 1 barn", series=[]),
    "2barn_status": Metadata(heading="Husholdninger med 2 barn", series=[]),
    "2barn_historisk": Metadata(heading="Husholdninger med 2 barn", series=[]),
    "3barn_status": Metadata(heading="Husholdninger med 3 barn", series=[]),
    "3barn_historisk": Metadata(heading="Husholdninger med 3 barn", series=[]),
}

DATA_POINTS = {
    "alle_status": ["one_child", "two_child", "three_or_more"],
    "alle_historisk": ["one_child", "two_child", "three_or_more"],
    "1barn_status": ["one_child"],
    "1barn_historisk": ["one_child"],
    "2barn_status": ["two_child"],
    "2barn_historisk": ["two_child"],
    "3barn_status": ["three_or_more"],
    "3barn_historisk": ["three_or_more"],
}


value_columns = [
    "flerfamiliehusholdninger_med_smaa_barn",
    "flerfamiliehusholdninger_med_store_barn",
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

    source = source.dropna(subset=["bydel_id"])

    df = process(source)

    if type == "alle_status":
        create_ds(output_key, TemplateA(), type, *status(df))
    elif type == "alle_historisk":
        create_ds(output_key, TemplateC(), type, *historic(df))
    elif type == "1barn_status":
        create_ds(output_key, TemplateA(), type, *status(df))
    elif type == "1barn_historisk":
        create_ds(output_key, TemplateB(), type, *historic(df))
    elif type == "2barn_status":
        create_ds(output_key, TemplateA(), type, *status(df))
    elif type == "2barn_historisk":
        create_ds(output_key, TemplateB(), type, *historic(df))
    elif type == "3barn_status":
        create_ds(output_key, TemplateA(), type, *status(df))
    elif type == "3barn_historisk":
        create_ds(output_key, TemplateB(), type, *historic(df))
    else:
        raise Exception("Wrong dataset type")

    return f"Complete: {output_key}"


def create_ds(output_key, template, type_of_ds, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=DATA_POINTS[type_of_ds],
    ).generate_output()
    aws.write_to_intermediate(output_key=output_key, output_list=jsonl)


def process(source):
    source = source.fillna(0)
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
            "config": {"type": "1barn_historisk"},
        },
        {},
    )
