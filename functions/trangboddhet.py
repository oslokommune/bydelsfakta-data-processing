from common.aws import read_from_s3, write_to_intermediate
from common.transform import status, historic
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC, TemplateB, TemplateJ
from common.util import get_latest_edition_of


METADATA = {
    "0-5-0-9_status": Metadata(heading="0,5–0,9 rom per person", series=[]),
    "0-5-0-9_historisk": Metadata(heading="0,5–0,9 rom per person", series=[]),
    "1-0-1-9_status": Metadata(heading="1,0–1,9 rom per person", series=[]),
    "1-0-1-9_historisk": Metadata(heading="Personer per rom - 1,5 - 1,9", series=[]),
    "over-2_status": Metadata(heading="2 rom eller flere per person", series=[]),
    "over-2_historisk": Metadata(heading="2 rom eller flere per person", series=[]),
    "under-0-5_status": Metadata(heading="Under 0,5 rom per preson", series=[]),
    "under-0-5_historisk": Metadata(heading="Personer per rom - under 0.5", series=[]),
    "alle_status": Metadata(
        heading="Husstander fordelt på personer per rom",
        series=[
            {"heading": "Under 0,5 rom per person", "subheading": ""},
            {"heading": "0,5–0,9 rom per person", "subheading": ""},
            {"heading": "1,0–1,9 rom per person", "subheading": ""},
            {"heading": "2 rom eller flere per person", "subheading": ""},
        ],
    ),
    "alle_historisk": Metadata(
        heading="",
        series=[
            {"heading": "Under 0,5 rom per person", "subheading": ""},
            {"heading": "0,5–0,9 rom per person", "subheading": ""},
            {"heading": "1,0–1,9 rom per person", "subheading": ""},
            {"heading": "2 rom eller flere per person", "subheading": ""},
        ],
    ),
}

DATA_POINTS = {
    "0-5-0-9_status": ["rom_per_person_0_5_til_0_9"],
    "0-5-0-9_historisk": ["rom_per_person_0_5_til_0_9"],
    "1-0-1-9_status": ["rom_per_person_1_0_til_1_9"],
    "1-0-1-9_historisk": ["rom_per_person_1_0_til_1_9"],
    "over-2_status": ["rom_per_person_2_0_og_over"],
    "over-2_historisk": ["rom_per_person_2_0_og_over"],
    "under-0-5_status": ["rom_per_person_under_0_5"],
    "under-0-5_historisk": ["rom_per_person_under_0_5"],
    "alle_status": [
        "rom_per_person_under_0_5",
        "rom_per_person_0_5_til_0_9",
        "rom_per_person_1_0_til_1_9",
        "rom_per_person_2_0_og_over",
    ],
    "alle_historisk": [
        "rom_per_person_under_0_5",
        "rom_per_person_0_5_til_0_9",
        "rom_per_person_1_0_til_1_9",
        "rom_per_person_2_0_og_over",
    ],
}

VALUE_POINTS = [
    "rom_per_person_under_0_5",
    "rom_per_person_0_5_til_0_9",
    "rom_per_person_1_0_til_1_9",
    "rom_per_person_2_0_og_over",
]


def handle(event, context):
    s3_key = event["input"]["husholdninger-etter-rom-per-person"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    start(s3_key, output_key, type_of_ds)
    return "OK"


def start(key, output_key, type_of_ds):
    df = read_from_s3(s3_key=key, date_column="aar")

    agg = Aggregate(
        {
            "rom_per_person_under_0_5": "sum",
            "rom_per_person_0_5_til_0_9": "sum",
            "rom_per_person_1_0_til_1_9": "sum",
            "rom_per_person_2_0_og_over": "sum",
            "rom_per_person_i_alt": "sum",
        }
    )

    df = df[df["bydel_id"] != "00"]
    df = agg.aggregate(df)
    df = agg.add_ratios(df, VALUE_POINTS, VALUE_POINTS)

    if type_of_ds == "0-5-0-9_status":
        create_ds(output_key, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "0-5-0-9_historisk":
        create_ds(output_key, TemplateB(), type_of_ds, *historic(df))
    elif type_of_ds == "1-0-1-9_status":
        create_ds(output_key, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "1-0-1-9_historisk":
        create_ds(output_key, TemplateB(), type_of_ds, *historic(df))
    elif type_of_ds == "over-2_status":
        create_ds(output_key, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "over-2_historisk":
        create_ds(output_key, TemplateB(), type_of_ds, *historic(df))
    elif type_of_ds == "under-0-5_status":
        create_ds(output_key, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "under-0-5_historisk":
        create_ds(output_key, TemplateA(), type_of_ds, *historic(df))
    elif type_of_ds == "alle_status":
        create_ds(output_key, TemplateJ(), type_of_ds, *status(df))
    elif type_of_ds == "alle_historisk":
        create_ds(output_key, TemplateC(), type_of_ds, *historic(df))


def create_ds(output_key, template, type_of_ds, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=DATA_POINTS[type_of_ds],
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handle(
        {
            "input": {
                "husholdninger-etter-rom-per-person": get_latest_edition_of(
                    "husholdninger-etter-rom-per-person"
                )
            },
            "output": "intermediate/green/trangboddhet/version=1/edition=20190601T093045/",
            "config": {"type": "alle_status"},
        },
        {},
    )
