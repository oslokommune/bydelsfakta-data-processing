from common import aws
from common import transform
from common import aggregate_dfs
from common.transform_output import generate_output_list


DATASETS = [
    {
        "data_points": ["innvandrer"],
        "heading": "Andel innvandrere (under 16)",
        "status": {
            "dataset_id": "Andel-innvandrere-under-16-sta-imHqA",
            "version_id": "1-XCUTiZRV",
            "edition_id": "EDITION-X9RWM",
        },
        "historic": {
            "dataset_id": "Andel-innvandrere-under-16-his-5EUaC",
            "version_id": "1-HFJhCiEE",
            "edition_id": "EDITION-YeWHS",
        },
    },
    {
        "data_points": ["en_forelder"],
        "heading": "Andel (under 16) med én innvandrerforelder",
        "status": {
            "dataset_id": "Andel-under-16-med-en-innvandr-rRuib",
            "version_id": "1-trUGzZxu",
            "edition_id": "EDITION-bc3AZ",
        },
        "historic": {
            "dataset_id": "Andel-under-16-med-en-innvandr-JnGas",
            "version_id": "1-89Kk4ZjT",
            "edition_id": "EDITION-9VN6b",
        },
    },
    {
        "data_points": ["to_foreldre"],
        "heading": "Andel (under 16) med to innvandrerforeldre",
        "status": {
            "dataset_id": "Andel-under-16-med-to-innvandr-jVhZm",
            "version_id": "1-XPgSftSS",
            "edition_id": "EDITION-J5SWX",
        },
        "historic": {
            "dataset_id": "Andel-under-16-med-to-innvandr-Zgj32",
            "version_id": "1-TF7MurAo",
            "edition_id": "EDITION-x8PmL",
        },
    },
]

DATA_POINTS = [point for d in DATASETS for point in d["data_points"]]


def handle(event, context):
    landbakgrunn_key = event["keys"]["Landbakgrunn_etter_alder-Yh7UC"]
    start(landbakgrunn_key)
    return "OK"


def start(landbakgrunn_key):
    raw = aws.read_from_s3(landbakgrunn_key)
    df = (
        raw.pivot_table(
            index=["date", "delbydelid", "Alder"],
            columns="Innvandringskategori",
            values="Antall personer",
            fill_value=0,
            aggfunc="sum",
            margins=True,
            margins_name="total",
        )
        .reset_index()
        .rename_axis(columns="")
    )

    df = df[df["Alder"] == "0-15 år"].reset_index()
    df["en_forelder"] = (
        df["Norskfødt med en utenlandskfødt forelder"]
        + df["Utenlandsfødt m/en norsk forelder"]
    )
    df = df.rename(
        columns={
            "Innvandrer": "innvandrer",
            "Norskfødt med innvandrerforeldre": "to_foreldre",
        }
    )
    df = df[["date", "delbydelid", "innvandrer", "en_forelder", "to_foreldre", "total"]]
    df = transform.add_district_id(df)

    aggregations = [
        {"agg_func": "sum", "data_points": dp} for dp in DATA_POINTS + ["total"]
    ]
    df = aggregate_dfs.aggregate_from_subdistricts(df, aggregations)
    df = aggregate_dfs.add_ratios(df, DATA_POINTS, ["total"])

    status_df = transform.status(df)
    historic_df = transform.historic(df)

    for dataset in DATASETS:
        status_output_key = _output_key(**dataset["status"])
        historic_output_key = _output_key(**dataset["historic"])

        status_outout_list = generate_output_list(
            *status_df, template="a", data_points=dataset["data_points"]
        )
        historic_output_list = generate_output_list(
            *historic_df, template="b", data_points=dataset["data_points"]
        )

        aws.write_to_intermediate(
            status_output_key, status_outout_list, dataset["heading"], []
        )
        aws.write_to_intermediate(
            historic_output_key, historic_output_list, dataset["heading"], []
        )


def _output_key(dataset_id, version_id, edition_id):
    return f"processed/green/{dataset_id}/version={version_id}/edition={edition_id}/"


if __name__ == "__main__":
    handle(
        {
            "bucket": "ok-origo-dataplatform-dev",
            "keys": {
                "Landbakgrunn_etter_alder-Yh7UC": "raw/green/Landbakgrunn_etter_alder-Yh7UC/version=1-tidZ36sM/edition=EDITION-BFMSo/Landbakgrunn_etter_alder(1.1.2008-1.1.2018-v01).csv"
            },
        },
        None,
    )
