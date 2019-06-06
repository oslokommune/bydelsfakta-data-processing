import numpy as np
import common.aws
import common.aggregate_dfs  # as aggregate
import common.transform  # as transform
from common.aggregateV2 import Aggregate
from common.templates import TemplateA
from common.output import Output, Metadata
from common.util import get_latest_edition_of


def handle(event, context):

    """ Assuming we receive a complete s3 key"""

    s3_key = event["input"]["husholdninger-med-barn"]
    output_key = event["output"]
    output_set = event["config"]["type"]
    start(s3_key, output_key, output_set)

    return "OK"


def start(key, output_key, output_set):

    number_type = (
        "float64"
    )  # Fails when reading directly as "int64", convert after reading
    dtype = {
        "delbydel_id": object,
        "delbydel_navn": object,
        "bydel_id": object,
        "bydel_navn": object,
        "barn_i_husholdningen": object,
        "aleneboende": number_type,
        "enfamiliehusholdninger_med_voksne_barn": number_type,
        "flerfamiliehusholdninger_med_smaa_barn": number_type,
        "flerfamiliehusholdninger_med_store_barn": number_type,
        "flerfamiliehusholdninger_uten_barn_0_til_17_aar": number_type,
        "mor_eller_far_med_smaa_barn": number_type,
        "mor_eller_far_med_store_barn": number_type,
        "par_med_smaa_barn": number_type,
        "par_med_store_barn": number_type,
        "par_uten_hjemmeboende_barn": number_type,
    }

    df = common.aws.read_from_s3(s3_key=key, date_column="aar", dtype=dtype)

    for col in df.columns:
        if df[col].dtype == number_type:
            df[col] = df[col].astype("int64")

    df = _clean_df(df)

    df = _remap_number_of_children(df)

    # Pivot - values for categories in "barnekategori" move to separate columns
    df = df.pivot_table(
        index=["date", "bydel_id", "bydel_navn", "delbydel_id", "delbydel_navn"],
        columns="barnekategori",
        values="antall_husholdninger",
        aggfunc=np.sum,
        fill_value=0,
    ).reset_index(drop=False)

    # Keep only data from the last year in the data set
    df = common.transform.status(df)[0].reset_index(drop=True)

    # Aggregate from delbydeler to bydel and Oslo total
    CHILD_CATEGORIES = ["Barn 0", "Barn 1", "Barn 2", "Barn 3+"]
    aggregation = {cc: "sum" for cc in CHILD_CATEGORIES}
    agg = Aggregate(aggregation)
    df = agg.aggregate(df)
    df = agg.add_ratios(df, CHILD_CATEGORIES, CHILD_CATEGORIES)

    output_structures = {
        "husholdninger-med-1-barn": {
            "heading": "Husholdninger med 1 barn",
            "value_col": "Barn 1",
        },
        "husholdninger-med-2-barn": {
            "heading": "Husholdninger med 2 barn",
            "value_col": "Barn 2",
        },
        "husholdninger-med-3-barn": {
            "heading": "Husholdninger med 3 eller flere barn",
            "value_col": "Barn 3+",
        },
    }

    try:
        assert output_set in output_structures.keys()
    except AssertionError:
        raise ValueError(f"{output_set} is not a valid output data set.")

    output = generate_output(
        df,
        value_col=output_structures[output_set]["value_col"],
        heading=output_structures[output_set]["heading"],
    )

    common.aws.write_to_intermediate(output_key, output)  # Can't do this yet!

    return f"Created {output_key}"


def _clean_df(df):

    """Various operations to clean and secure correct format."""

    mandatory = {
        "date",
        "delbydel_id",
        "delbydel_navn",
        "bydel_id",
        "bydel_navn",
        "barn_i_husholdningen",
    }
    for m in mandatory:
        assert m in df.columns

    number_cols = set(df.columns).difference(mandatory)
    for nc in number_cols:
        df[nc] = (
            df[nc].round().astype("int64")
        )  # This line should be removed when the input format is secured.
        assert df[nc].dtype == "int64"

    df["antall_husholdninger"] = df[number_cols].sum(axis=1)

    df = df[[*mandatory, "antall_husholdninger"]]

    return df


def _remap_number_of_children(df):

    """Map to the required format for number of children in the household."""

    m = {
        "Ingen barn i HH": "Barn 0",
        "1 barn i HH": "Barn 1",
        "2 barn i HH": "Barn 2",
        "3 barn i HH": "Barn 3+",
        "4 barn eller mer": "Barn 3+",
    }
    df["barnekategori"] = df["barn_i_husholdningen"].map(m).fillna(np.nan)
    if df["barnekategori"].isnull().any():
        raise ValueError(
            "Unmapped category found in the column 'barn_i_husholdningen'. See dict 'm'."
        )
    df = df.drop("barn_i_husholdningen", axis=1)

    return df


def generate_output(df, value_col, heading):

    series = [{"heading": heading, "subheading": ""}]

    # To json : convert df to list of json objects
    j = Output(
        df=df,
        values=[value_col],
        template=TemplateA(),
        metadata=Metadata(heading=heading, series=series),
    ).generate_output()

    return j


def _output_key(dataset_id, version_id, edition_id):
    return f"processed/green/{dataset_id}/version={version_id}/edition={edition_id}/"


def _write_to_intermediate(
    dataset_id, version_id, edition_id, output_list, series_heading
):

    series = [{"heading": series_heading, "subheading": ""}]
    heading = "Husholdninger"
    output_key = _output_key(dataset_id, version_id, edition_id)
    common.aws.write_to_intermediate(output_key, output_list, heading, series)


if __name__ == "__main__":

    input_data = get_latest_edition_of("husholdninger-med-barn")

    # Test writing one set
    handle(
        {
            "input": {"husholdninger-med-barn": input_data},
            "output": "intermediate/green/husholdninger-med-1-barn/version=1/edition=20190520T114926/",
            "config": {"type": "husholdninger-med-1-barn"},
        },
        {},
    )
