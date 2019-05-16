import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output


def read_from_s3(s3_key):

    # read from s3, renames `value_column` to value
    df = common.aws.read_from_s3(s3_key)

    return df


def prepare(df):

    # Generate the dataframe we want to start aggregating on
    df = population(df)

    return df


def population(df):

    df = df.groupby(["district", "delbydelid", "date"])["value"].sum()
    return df.reset_index()


def generate(df, value_labels):

    # Only generate the aggregated set, not (yet) create the outputs

    aggregations = [{"agg_func": "sum", "data_points": vl} for vl in value_labels]

    agg_df = common.aggregate_dfs.aggregate_from_subdistricts(df, aggregations)

    agg_df = common.aggregate_dfs.add_ratios(
        agg_df, data_points=value_labels, ratio_of=value_labels
    )

    return agg_df


def write(output_list, output_key, value_labels, heading):

    # Fix proper headings
    series = [{"heading": vl, "subheading": ""} for vl in value_labels]
    heading = heading

    common.aws.write_to_intermediate(
        output_key=output_key, output_list=output_list, heading=heading, series=series
    )


def data_processing(df, value_labels):

    # This function is testable.

    # We use only sub districts
    df = df[df["delbydelid"].notnull()]

    # Add district number
    df = common.transform.add_district_id(df)

    # Create historic and status data (at sub_district level)
    historic = common.transform.historic(df)
    status = common.transform.status(df)

    # Generate the aggregated datasets
    historic_agg = generate(*historic, value_labels)
    status_agg = generate(*status, value_labels)

    # Make output
    output_data = {}

    output_data[
        "trangboddhet-alle-status"
    ] = common.transform_output.generate_output_list(status_agg, "j", value_labels)
    output_data[
        "trangboddhet_alle_historisk-4DAEn"
    ] = common.transform_output.generate_output_list(historic_agg, "c", value_labels)

    output_data[
        "trangboddhet-under-0-5-status"
    ] = common.transform_output.generate_output_list(
        status_agg, "a", ["Personer per rom - Under 0,5"]
    )
    output_data[
        "trangboddhet-under-0-5-historisk"
    ] = common.transform_output.generate_output_list(
        historic_agg, "b", ["Personer per rom - Under 0,5"]
    )

    output_data[
        "trangboddhet-0-5-0-9-status"
    ] = common.transform_output.generate_output_list(
        status_agg, "a", ["Personer per rom - 0,5 - 0,9"]
    )
    output_data[
        "trangboddhet-0-5-0-9-historisk"
    ] = common.transform_output.generate_output_list(
        historic_agg, "b", ["Personer per rom - 0,5 - 0,9"]
    )

    output_data[
        "trangboddhet-1-0-1-9-status"
    ] = common.transform_output.generate_output_list(
        status_agg, "a", ["Personer per rom - 1,0 - 1,9"]
    )
    output_data[
        "trangboddhet-1-0-1-9-historisk"
    ] = common.transform_output.generate_output_list(
        historic_agg, "b", ["Personer per rom - 1,0 - 1,9"]
    )

    output_data[
        "trangboddhet-over-2-status"
    ] = common.transform_output.generate_output_list(
        status_agg, "a", ["Personer per rom - 2,0 og over"]
    )
    output_data[
        "trangboddhet-over-2-historisk"
    ] = common.transform_output.generate_output_list(
        historic_agg, "b", ["Personer per rom - 2,0 og over"]
    )

    return output_data


def handler(event, context):

    # These keys should be extracted from "event", but since we do not have the new pipeline yet
    # it needs to be hardcoded

    s3_key = r"raw/green/Husholdninger_etter_rom_per_pe-48LKF/version=1-oPutm8TS/edition=EDITION-3mQwN/Husholdninger_etter_rom_per_person(1.1.2015-1.1.2017-v01).csv"

    source = read_from_s3(s3_key=s3_key)

    value_labels = [
        "Personer per rom - Under 0,5",
        "Personer per rom - 0,5 - 0,9",
        "Personer per rom - 1,0 - 1,9",
        "Personer per rom - 2,0 og over",
    ]

    output_data = data_processing(source, value_labels)

    set_IDs = {
        "trangboddhet-alle-status": {
            "ver_ID": "1-uozVNtqz",
            "edition": "EDITION-NhDso",
        },
        "trangboddhet_alle_historisk-4DAEn": {
            "ver_ID": "1-NpEWu8Kp",
            "edition": "EDITION-ukGfK",
        },
        "trangboddhet-under-0-5-status": {
            "ver_ID": "1-SiFGuLvX",
            "edition": "EDITION-hv76W",
        },
        "trangboddhet-under-0-5-historisk": {
            "ver_ID": "1-QymhyHjz",
            "edition": "EDITION-Brjio",
        },
        "trangboddhet-0-5-0-9-status": {
            "ver_ID": "1-ksEYDMnT",
            "edition": "EDITION-S2Lcs",
        },
        "trangboddhet-0-5-0-9-historisk": {
            "ver_ID": "1-aFiHVUW4",
            "edition": "EDITION-WNAes",
        },
        "trangboddhet-1-0-1-9-status": {
            "ver_ID": "1-kuU9GsfB",
            "edition": "EDITION-MHzJr",
        },
        "trangboddhet-1-0-1-9-historisk": {
            "ver_ID": "1-4z6WVuiv",
            "edition": "EDITION-9rUHD",
        },
        "trangboddhet-over-2-status": {
            "ver_ID": "1-nmNo2hv3",
            "edition": "EDITION-aAJPT",
        },
        "trangboddhet-over-2-historisk": {
            "ver_ID": "1-qbmHy82x",
            "edition": "EDITION-ew29J",
        },
    }

    assert sorted(list(set_IDs.keys())) == sorted(list(output_data.keys()))

    # Write to processed at S3
    for output_data_name in output_data.keys():

        ver_ID = set_IDs[output_data_name]["ver_ID"]
        edition = set_IDs[output_data_name]["edition"]

        output_key = (
            f"processed/green/{output_data_name}/version={ver_ID}/edition={edition}/"
        )

        write(output_data[output_data_name], output_key, value_labels, output_data_name)

    output_keys = list(
        output_data.keys()
    )  # Note - this currently returns the titles, not the keys.

    return output_keys


if __name__ == "__main__":
    handler({}, {})
