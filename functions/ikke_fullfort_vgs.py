import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output
from common.aggregateV2 import ColumnNames
from common.output import Metadata, Output
from common.templates import TemplateB, TemplateA
from common.util import get_latest_edition_of

column_names = ColumnNames()

ikke_fullfort_antall = "antall_personer_ikke_fullfort_i_løpet_av_5_aar"
ikke_fullfort_andel = "andelen_som_ikke_har_fullfort_i_lopet_av_5_aar"


def read(s3_key):
    # Generate the dataframe we want to start aggregating on
    df = common.aws.read_from_s3(s3_key)
    df[ikke_fullfort_andel] = df[ikke_fullfort_andel] / 100
    df = df.rename(columns={ikke_fullfort_andel: f"{ikke_fullfort_antall}_ratio"})
    return df


def process(df, type):
    metadata = Metadata(
        heading="Personer som ikke har fullført vgs",
        series=[
            {
                "Heading": "Antall personer, ikke fullført i løpet av 5 år",
                "subheading": "",
            }
        ],
    )
    if type == "historisk":
        [df] = common.transform.historic(df)
        template = TemplateB()
    elif type == "status":
        [df] = common.transform.status(df)
        template = TemplateA()
    else:
        raise Exception("type must be status or historisk")

    output = Output(
        df=df, values=[ikke_fullfort_antall], metadata=metadata, template=template
    ).generate_output()

    return output


def write(output, s3_key):
    print(output)
    common.aws.write_to_intermediate(output_list=output, output_key=s3_key)
    return s3_key


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["ikke-fullfort-vgs"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    df = read(s3_key)
    output = process(df, type_of_ds)
    return write(output, output_key)


if __name__ == "__main__":
    handler(
        {
            "input": {"ikke-fullfort-vgs": get_latest_edition_of("ikke-fullfort-vgs")},
            "output": "intermediate/green/ikke-fullfort-vgs-status/version=1/edition=20110531T102550/",
            "config": {"type": "status"},
        },
        {},
    )
