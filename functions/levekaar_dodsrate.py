import common.aws
import common.transform
import common.transform_output
import common.util
from common.util import get_latest_edition_of
from common import transform

from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB


def handler(event, context):
    s3_key = event["input"]["dodsrater"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    return start(s3_key, output_key, type_of_ds)


def start(key, output_key, type_of_ds):
    df = common.aws.read_from_s3(s3_key=key)

    df["dodsrate_ratio"] = df["dodsrate"] / 100

    if type_of_ds == "status":
        template = TemplateA()
        df = transform.status(df)[0]
    elif type_of_ds == "historisk":
        template = TemplateB()
        df = transform.historic(df)[0]

    metadata = Metadata(
        heading="Dødelighet (gj.snitt siste 7 år) for personer 55–79 år", series=[]
    )
    output = Output(values=["dodsrate"], df=df, template=template, metadata=metadata)
    jsonl = output.generate_output()
    common.aws.write_to_intermediate(output_key=output_key, output_list=jsonl)
    return f"Created {output_key}"


if __name__ == "__main__":
    handler(
        {
            "input": {"dodsrater": get_latest_edition_of("dodsrater")},
            "output": "intermediate/green/dodsrater-status/version=1/edition=20190822T144000/",
            "config": {"type": "historisk"},
        },
        {},
    )
