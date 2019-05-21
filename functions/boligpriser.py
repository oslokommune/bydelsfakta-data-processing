import common.aws
import common.transform
import common.util
import common.transform_output


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["keys"]["boligpriser-blokkleiligheter"]
    type_of_ds = event["type"]
    start(s3_key, type_of_ds)
    return "OK"


def start(key, type_of_ds):
    df = common.aws.read_from_s3(s3_key=key, date_column="aar", dtype={"bydel_id": object, "delbydel_id": object})
    df = df.rename(
        columns={
            "kvmpris": "value",
            "bydel_id": "district",
            "delbydel_id": "delbydelid",
        }
    )
    df = df.drop(
        columns=["antall_omsatte_blokkleiligheter"]
    )
    df = df[df.value.notnull()]
    df["district"].fillna("00", inplace=True)

    print(df.to_string())

    status = common.transform.status(df)
    historic = common.transform.historic(df)
    if type_of_ds == "historic":
        create_ds("boligpriser-blokkleiligheter-historic", "c", *historic)
    elif type_of_ds == "status":
        create_ds("boligpriser-blokkleiligheter-status", "a", *status)


def create_ds(dataset_id, template, df):
    heading = "Gjennomsnittpris (kr) pr kvm for blokkleilighet"
    series = [
        {"heading": "Gjennomsnittpris (kr) pr kvm for blokkleilighet", "subheading": ""}
    ]

    print(df.to_string())

    # To json : convert df to list of json objects
    jsonl = common.transform_output.generate_output_list(df, template, ["value"])
    common.aws.write_to_intermediate(
        output_key=f"processed/green/{dataset_id}/version=1/edition=20190520T144900/",
        heading=heading,
        series=series,
        output_list=jsonl,
    )
    return


if __name__ == "__main__":
    handler(
        {
            "bucket": "ok-origo-dataplatform-dev",
            "keys": {
                "boligpriser-blokkleiligheter": "raw/green/boligpriser-blokkleiligheter/version=1/edition=20190520T114926/Boligpriser(2004-2017-v01).csv"
            },
            "type": "status",
        },
        {},
    )
