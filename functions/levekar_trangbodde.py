import pandas as pd

import common.aws as common_aws
import common.transform as transform
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_latest_edition_of


pd.set_option("display.max_rows", 1000)

graph_metadata = Metadata(
    heading="Levekår Trangbodde", series=[{"heading": "Trangbodde", "subheading": ""}]
)


def handle(event, context):
    s3_key_trangbodde = event["input"]["trangbodde"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]

    trangbodde_raw = common_aws.read_from_s3(
        s3_key=s3_key_trangbodde, date_column="aar"
    )
    datapoint = "antall_som_bor_trangt"

    trangbodde_raw["antall_som_bor_trangt_ratio"] = (
        trangbodde_raw["andel_som_bor_trangt"] / 100
    )

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(trangbodde_raw, [datapoint])

    elif type_of_ds == "status":
        output_list = output_status(trangbodde_raw, [datapoint])

    if output_list:
        common_aws.write_to_intermediate(output_key=output_key, output_list=output_list)
        return f"Created {output_key}"

    else:
        raise Exception("No data in outputlist")


def output_historic(input_df, data_points):
    [input_df] = transform.historic(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateB()
    ).generate_output()

    return output


def output_status(input_df, data_points):
    [input_df] = transform.status(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateA()
    ).generate_output()
    return output


if __name__ == "__main__":
    handle(
        {
            "input": {"trangbodde": get_latest_edition_of("trangbodde")},
            "output": "intermediate/green/levekar-trangbodde-status/version=1/edition=20190821T124040/",
            "config": {"type": "status"},
        },
        {},
    )
    # handle(
    #     {
    #         "input": {
    #             "trangbodde": "raw/green/trangbodde/version=1/edition=20190529T121303/Trangbodde(1.1.2015-1.1.2017-v01).csv",
    #             "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190523T211529/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv",
    #         },
    #         "output": "s3/key/or/prefix",
    #         "config": {"type": "historic"},
    #     },
    #     None,
    # )
