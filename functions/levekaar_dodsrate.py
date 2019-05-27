
import common.aws
import common.transform
import common.transform_output
import common.util
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata, TemplateA
import numpy as np

def handler(event, context):
    s3_key = event["input"]["dodsrater"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    return start(s3_key, output_key, type_of_ds)


def start(key, output_key, type_of_ds):
    df = common.aws.read_from_s3(s3_key=key)

    df = Aggregate({}).add_ratios(df=df, data_points=['antallet_dode'], ratio_of=['antall_personer'])

    # TODO: Fix in csvlt: add bydel_id
    df.loc[df['bydel_navn'] == "Oslo i alt", "bydel_id"] = "00"

    # TODO: Hardcoded removal of wrongly categorized subdistricts
    df.iloc[95:, 1] = np.nan
    df.iloc[95:, 2] = np.nan

    if type_of_ds == "status":
    elif type_of_ds == "historisk":

    metadata = Metadata(
            heading="Dødsrater",
            series=[{"heading": "Antall døde", "sub-heading": ""}]
    )
    output = Output(
            values=["antallet_dode"],
            df=df,
            template=TemplateA(ratios=True),
            metadata=metadata
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
                "config": { "type": "historisk"},
            },
            {},
    )
