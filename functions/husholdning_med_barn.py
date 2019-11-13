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
            {"heading": "Total", "subheading": ""},
        ],
    ),
    "1barn_status": Metadata(heading="Husholdninger med 1 barn", series=[]),
    "1barn_historisk": Metadata(heading="Husholdninger med 1 barn", series=[]),
    "2barn_status": Metadata(heading="Husholdninger med 2 barn", series=[]),
    "2barn_historisk": Metadata(heading="Husholdninger med 2 barn", series=[]),
    "3barn_status": Metadata(heading="Husholdninger med 3 barn", series=[]),
    "3barn_historisk": Metadata(heading="Husholdninger med 3 barn", series=[]),
}

VALUE_CATEGORY = {
    "alle_status": ["one_child", "two_child", "three_or_more"],
    "alle_historisk": ["one_child", "two_child", "three_or_more", "total"],
    "1barn_status": ["one_child"],
    "1barn_historisk": ["one_child"],
    "2barn_status": ["two_child"],
    "2barn_historisk": ["two_child"],
    "3barn_status": ["three_or_more"],
    "3barn_historisk": ["three_or_more"],
}

DATA_POINTS = ["one_child", "two_child", "three_or_more", "no_children", "total"]

column_names = ColumnNames()


def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["husholdninger-med-barn"]
    output_key = event["output"]
    type = event["config"]["type"]
    source = aws.read_from_s3(s3_key=s3_key)

    source["one_child"] = source["ett_barn_i_hh"]
    source["two_child"] = source["to_barn_i_hh"]
    source["three_or_more"] = source["tre_barn_i_hh"] + source["fire_barn_eller_mer"]
    source["no_children"] = source["ingen_barn_i_hh"]

    source = source.drop(
        columns=[
            "ett_barn_i_hh",
            "to_barn_i_hh",
            "tre_barn_i_hh",
            "fire_barn_eller_mer",
            "ingen_barn_i_hh",
        ]
    )

    source["total"] = (
        source["one_child"]
        + source["two_child"]
        + source["three_or_more"]
        + source["no_children"]
    )

    agg = Aggregate("sum")

    source = agg.aggregate(source)

    df = agg.add_ratios(source, data_points=DATA_POINTS, ratio_of=["total"])

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
        values=VALUE_CATEGORY[type_of_ds],
    ).generate_output()
    aws.write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handle(
        {
            "input": {
                "husholdninger-med-barn": util.get_latest_edition_of(
                    "husholdninger-med-barn"
                )
            },
            "output": "intermediate/green/husholdninger-totalt-status/version=1/edition=20190819T110202/",
            "config": {"type": "alle_historisk"},
        },
        {},
    )
