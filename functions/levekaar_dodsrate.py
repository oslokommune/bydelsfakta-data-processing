import common.aws
import common.transform
import common.transform_output
import common.util
from common import transform
from common.aggregateV2 import Aggregate
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

    df = Aggregate({}).add_ratios(
        df=df, data_points=["antallet_dode"], ratio_of=["antall_personer"]
    )

    # TODO: Hardcoded removal of wrongly categorized subdistricts
    df.iloc[95:, 1] = np.nan
    df.iloc[95:, 2] = np.nan
    if type_of_ds == "status":
        df = transform.status(df)[0]
    elif type_of_ds == "historisk":
        df = transform.historic(df)[0]

    metadata = Metadata(
        heading="Dødsrater", series=[{"heading": "Antall døde", "sub-heading": ""}]
    )
    output = Output(
        values=["antallet_dode"], df=df, template=TemplateA(), metadata=metadata
    )
    jsonl = output.generate_output()
    common.aws.write_to_intermediate(output_key=output_key, output_list=jsonl)
    return f"Created {output_key}"


if __name__ == "__main__":
    handler(
        {
            "input": {
                "dodsrater": "raw/green/dodsrater/version=1/edition=20190524T131109/Dodsrater(2016-v01).csv"
            },
            "output": "intermediate/green/dodsrater-historisk/version=1/edition=20190527T104900/",
            "config": {"type": "historisk"},
        },
        {},
    )