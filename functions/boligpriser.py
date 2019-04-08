import common.aws
import common.transform
import common.util
import common.transform_output


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["keys"]["boligpriser-urFqK"]
    type_of_ds = event["type"]
    start(s3_key, type_of_ds)
    return "OK"


def start(key, type_of_ds):
    original = common.aws.read_from_s3(s3_key=key).rename(columns={"kvmpris": "value"})
    original = common.transform.add_district_id(original, "Oslo-Bydelsnavn")
    original = original.drop(
        columns=[
            "antall omsatte blokkleieligheter",
            "Oslo-Bydelsnavn",
            "Delbydelnummer",
            "Delbydelsnavn",
        ]
    )
    status = common.transform.status(original)
    historic = common.transform.historic(original)
    if type_of_ds == "historic":
        create_ds("boligpriser_historic-4owcY", "c", *historic)
    elif type_of_ds == "status":
        create_ds("boligpriser_status-pD7ZV", "a", *status)


def create_ds(dataset_id, template, df):
    heading = "Gjennomsnittpris (kr) pr kvm for blokkleilighet"
    series = [
        {"heading": "Gjennomsnittpris (kr) pr kvm for blokkleilighet", "subheading": ""}
    ]

    # To json : convert df to list of json objects
    jsonl = common.transform_output.generate_output_list(df, template, ["value"])
    common.aws.write_to_intermediate(
        output_key=f"processed/green/{dataset_id}/version=1-xf57skLF/edition=EDITION-5VnJy/",
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
                "boligpriser-urFqK": "raw/green/boligpriser-urFqK/version=1-NrHiJxwf/edition=EDITION-EbL7P/Boligpriser.csv"
            },
            "type": "status",
        },
        {},
    )
