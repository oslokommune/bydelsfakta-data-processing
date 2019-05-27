import json

import common.aws
import common.transform
import common.transform_output
import common.util
from common.output import Output, Metadata, TemplateA


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["boligpriser-blokkleiligheter"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    return start(s3_key, output_key, type_of_ds)


def start(key, output_key, type_of_ds):
    df = common.aws.read_from_s3(s3_key=key)
    df = df.rename(columns={"kvmpris": "value"})
    df = df.drop(columns=["antall_omsatte_blokkleiligheter"])

    # TODO: Fix in csvlt, missing id for Oslo i alt
    df.loc[df["bydel_navn"] == "Oslo i alt", "bydel_id"] = "00"

    if type_of_ds == "status":
        df = common.transform.status(df)
    elif type_of_ds == "historisk":
        df = common.transform.historic(df)
    else:
        raise Exception("Wrong dataset type. Use 'status' or 'historisk'.")

    json_lines = generate(*df, type_of_ds)

    common.aws.write_to_intermediate(output_key, json_lines)
    return f"Created {output_key}"


def generate(df, ds_type):
    heading = "Gjennomsnittpris (kr) pr kvm for blokkleilighet"
    series = [
        {"heading": "Gjennomsnittpris (kr) pr kvm for blokkleilighet", "subheading": ""}
    ]
    # To json : convert df to list of json objects
    jsonl = Output(
        df=df[~df["value"].isna()],
        values=["value"],
        template=TemplateA(),
        metadata=Metadata(heading=heading, series=series),
    ).generate_output()
    return jsonl


if __name__ == "__main__":
    handler(
        {
            "input": {
                "boligpriser-blokkleiligheter": "raw/green/boligpriser-blokkleiligheter/version=1/edition=20190523T211529/Boligpriser(2004-2017-v01).csv"
            },
            "output": "intermediate/green/boligpriser-blokkleiligheter-status/version=1/edition=20190422T211529/",
            "config": {"type": "historisk"},
        },
        {},
    )
