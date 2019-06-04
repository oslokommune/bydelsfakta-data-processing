import common.transform as transform
import common.aws as common_aws
from common.util import get_latest_edition_of
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB


graph_metadata = Metadata(
    heading="Personer fra 16 til 66 år med redusert funksjonsevne",
    series=[{"heading": "Redusert funksjonsevne", "subheading": ""}],
)


def handle(event, context):
    s3_key_redusert_funksjonsevne = event["input"]["redusert-funksjonsevne"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]

    data_point = "antall_personer_med_redusert_funksjonsevne"

    input_df = common_aws.read_from_s3(
        s3_key=s3_key_redusert_funksjonsevne, date_column="aar"
    )

    input_df[f"{data_point}_ratio"] = (
        input_df["andel_personer_med_redusert_funksjonsevne"] / 100
    )

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df, [data_point])

    elif type_of_ds == "status":
        output_list = output_status(input_df, [data_point])

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
    redusert_funksjonsevne_s3_key = get_latest_edition_of("redusert-funksjonsevne")
    handle(
        {
            "input": {"redusert-funksjonsevne": redusert_funksjonsevne_s3_key},
            "output": "s3/key/or/prefix",
            "config": {"type": "status"},
        },
        None,
    )
