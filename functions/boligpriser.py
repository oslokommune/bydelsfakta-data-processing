import common.aws
import common.transform
import common.util
import common.transform_output


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["boligpriser-blokkleiligheter"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    start(s3_key, output_key, type_of_ds)
    return "OK"


def start(key, output_key, type_of_ds):
    df = common.aws.read_from_s3(
        s3_key=key, date_column="aar", dtype={"bydel_id": object, "delbydel_id": object}
    )
    df = df.rename(
        columns={
            "kvmpris": "value",
            "bydel_id": "district",
            "delbydel_id": "delbydelid",
        }
    )
    df = df.drop(columns=["antall_omsatte_blokkleiligheter"])
    df = df[df.value.notnull()]
    df["district"].fillna("00", inplace=True)

    status = common.transform.status(df)
    historic = common.transform.historic(df)
    if type_of_ds == "historisk":
        create_ds(output_key, "c", *historic)
    elif type_of_ds == "status":
        create_ds(output_key, "a", *status)


def create_ds(output_key, template, df):
    heading = "Gjennomsnittpris (kr) pr kvm for blokkleilighet"
    series = [
        {"heading": "Gjennomsnittpris (kr) pr kvm for blokkleilighet", "subheading": ""}
    ]

    # To json : convert df to list of json objects
    jsonl = common.transform_output.generate_output_list(df, template, ["value"])
    common.aws.write_to_intermediate(
        output_key=output_key, heading=heading, series=series, output_list=jsonl
    )
    return


if __name__ == "__main__":
    handler(
        {
            "input": {
                "boligpriser-blokkleiligheter": "raw/green/boligpriser-blokkleiligheter/version=1/edition=20190520T114926/Boligpriser(2004-2017-v01).csv"
            },
            "output": "intermediate/green/boligpriser-blokkleiligheter-status/version=1/edition=20190520T114926/",
            "config": {"type": "status"},
        },
        {},
    )
