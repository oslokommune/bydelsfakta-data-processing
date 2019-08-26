from common import aws, util, transform
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC

DATA_POINTS = [
    "aleneboende",
    "flerfamiliehusholdninger",
    "par_uten_barn",
    "par_med_barn",
    "mor_far_med_barn",
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

    source["par_uten_barn"] = source["par_uten_hjemmeboende_barn"]
    source["par_med_barn"] = source["par_med_smaa_barn"] + source["par_med_store_barn"]
    source["mor_far_med_barn"] = (
        source["mor_eller_far_med_smaa_barn"] + source["mor_eller_far_med_store_barn"]
    )
    source["flerfamiliehusholdninger"] = (
        source["enfamiliehusholdninger_med_voksne_barn"]
        + source["flerfamiliehusholdninger_med_smaa_barn"]
        + source["flerfamiliehusholdninger_med_store_barn"]
        + source["flerfamiliehusholdninger_uten_barn_0_til_17_aar"]
    )

    source = source.drop(
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
            "barn_i_husholdningen",
        ]
    )

    source = source.groupby(column_names.default_groupby_columns(), as_index=False).agg(
        "sum"
    )

    agg = Aggregate("sum")

    source = agg.aggregate(source)

    aggregated = agg.add_ratios(source, data_points=DATA_POINTS, ratio_of=DATA_POINTS)

    if type == "status":
        [df] = transform.status(aggregated)
        template = TemplateA()

    elif type == "historisk":
        [df] = transform.historic(aggregated)
        template = TemplateC()
    else:
        raise Exception("Wrong dataset type")

    output = Output(
        df=df, values=DATA_POINTS, template=template, metadata=metadata
    ).generate_output()

    aws.write_to_intermediate(output_key=output_key, output_list=output)
    return f"Complete: {output_key}"


if __name__ == "__main__":
    handle(
        {
            "input": {
                "husholdninger-med-barn": util.get_latest_edition_of(
                    "husholdninger-med-barn"
                )
            },
            "output": "intermediate/green/husholdningstyper-status/version=1/edition=20190822T170202/",
            "config": {"type": "status"},
        },
        {},
    )