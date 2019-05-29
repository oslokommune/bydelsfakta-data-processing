import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output
import common.util
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateE


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["befolkning-etter-kjonn-og-alder"]
    output_key = event["output"]
    df = start(s3_key)
    create_ds(df, output_key)
    return "OK"


def start(key):
    agg = Aggregate("sum")
    original = common.aws.read_from_s3(s3_key=key)
    original = common.transform.status(original)[0]
    original["total"] = original.loc[:, "0":"120"].sum(axis=1)
    aggregated = agg.aggregate(original, extra_groupby_columns=["kjonn"])
    return aggregated


def _ratio(df):
    df["ratio"] = df["value"] / df["value"].sum()
    return df


def create_ds(df, output_key):
    heading = "Aldersdistribusjon fordelt på kjønn"
    series = [{"heading": "Aldersdistribusjon fordelt på kjønn", "subheading": ""}]

    ages = [str(i) for i in range(0, 121)]
    # To json : convert df to list of json objects
    jsonl = Output(
        df=df,
        template=TemplateE(),
        metadata=Metadata(heading=heading, series=series),
        values=ages,
    ).generate_output()
    common.aws.write_to_intermediate(output_list=jsonl, output_key=output_key)
    return output_key


if __name__ == "__main__":
    handler(
        {
            "input": {
                "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190528T123302/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv"
            },
            "output": "intermediate/green/alder-distribusjon-status/version=1/edition=20190528T142500/",
        },
        {},
    )
