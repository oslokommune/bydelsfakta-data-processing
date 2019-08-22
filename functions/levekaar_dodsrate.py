import common.aws
import common.transform
import common.transform_output
import common.util
from common.util import get_latest_edition_of
from common import transform
import numpy as np

from common.output import Output, Metadata
from common.templates import TemplateA


def handler(event, context):
    s3_key = event["input"]["dodsrater"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    return start(s3_key, output_key, type_of_ds)


def start(key, output_key, type_of_ds):
    df = common.aws.read_from_s3(s3_key=key)

    df["dodsrate_ratio"] = df["dodsrate"] / 100

    # TODO: Hardcoded removal of wrongly categorized subdistricts
    df.iloc[95:, 1] = np.nan
    df.iloc[95:, 2] = np.nan
    if type_of_ds == "status":
        df = transform.status(df)[0]

    metadata = Metadata(
        heading="Dødsrater",
        series=[{"heading": "Dødelighet (siste 5 år) for personer 55-79 år", "sub-heading": ""}],
    )
    output = Output(values=["dodsrate"], df=df, template=TemplateA(), metadata=metadata)
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
