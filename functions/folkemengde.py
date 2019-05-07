import numpy as np
from common.aws import read_from_s3, write_to_intermediate
from common.transform import add_district_id
from common.aggregate_dfs import aggregate_from_subdistricts, merge_dfs
from common.transform_output import generate_output_list


POPULATION_HISTORIC = {
    "heading": "Folkemengde utvikling historisk",
    "dataset_id": "folkemengde-utvikling-historisk",
    "version_id": "1-eft8iuhY",
    "edition_id": "EDITION-eoFpU",
}

POPULATION_HISTORIC_CHANGE = {
    "heading": "Folkemengde utvikling historisk prosent",
    "dataset_id": "folkemengde-utvikling-historisk-prosent",
    "version_id": "1-nDiEAbgc",
    "edition_id": "EDITION-22Yvk",
}


class Folkemengde(object):
    _df = None

    def __init__(self, df):
        self._df = df

    @classmethod
    def from_s3(
        cls,
        key="raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv",
    ):
        df = read_from_s3(key)
        df["delbydelid"].fillna("0301999999", inplace=True)
        df = add_district_id(df)

        df = df.rename(
            columns={
                "Antall personer": "value",
                # "Delbydel": "delbydel_id",
                # "district": "bydel_id",
            }
        )

        return cls(df)

    def filter_districts(self, districts=["16", "17"]):
        # TODO: Districts mappings for readability
        df = self._df.copy()
        df = df[~df["district"].isin(districts)]
        return Folkemengde(df)

    def filter_age(self, minimum, maximum):
        df = self._df.copy()
        df = df[(df["Alder"] >= minimum) & (df["Alder"] < maximum)]
        return Folkemengde(df)

    def to_sum(self):
        df = (
            self._df.groupby(["district", "delbydelid", "date"])["value"]
            .sum()
            .reset_index()
        )
        return df


def calculate_change(df):
    indexed = df.set_index(["district", "delbydelid", "date"])
    grouped = indexed.groupby(level="delbydelid")
    change_value = grouped.diff().rename(columns={"value": "change"}).reset_index()
    return merge_dfs(df, change_value)


def calculate_change_ratio(df):
    df["change_ratio"] = df["change"] / df["value"].shift(1)
    return df


def calculate(df):
    df = calculate_change(df)
    df = aggregate_from_subdistricts(
        df,
        [
            {"agg_func": "sum", "data_points": "value"},
            {"agg_func": sum_nans, "data_points": "change"},
        ],
    )
    df = calculate_change_ratio(df)
    return df


def sum_nans(df):
    """
    Sum aggregation function with special NaN handling. Only accepts series
    that are either all NaN or contains no NaNs.

    Args:
        df: pandas series.

    Returns:
        sum of the series if all values are not NaN, or NaN if all values in
        the series are NaN.

    Raises:
        ValueError: If the series contains a mix of values and NaNs.
    """
    no_nans = df.notna().all()
    all_nans = df.isna().all()

    if no_nans:
        return np.sum(df)
    elif all_nans:
        return np.nan
    else:
        raise ValueError("Mix of NaN and values")


def handler(event, context):
    df = Folkemengde.from_s3(
        key=event["keys"]["Befolkningen_etter_bydel_delby-J7khG"]
    ).to_sum()
    df = calculate(df)

    population = generate_output_list(df, template="b", data_points=["value"])
    population_change = generate_output_list(
        df.dropna(axis=0, how="any", subset=["change", "change_ratio"]),
        template="b",
        data_points=["change"],
    )

    write_to_intermediate(
        _output_key(POPULATION_HISTORIC), population, POPULATION_HISTORIC["heading"], []
    )
    write_to_intermediate(
        _output_key(POPULATION_HISTORIC_CHANGE),
        population_change,
        POPULATION_HISTORIC_CHANGE["heading"],
        [],
    )


def _output_key(dataset):
    return f"processed/green/{dataset['dataset_id']}/version={dataset['version_id']}/edition={dataset['edition_id']}/"


if __name__ == "__main__":
    handler(
        {
            "bucket": "ok-origo-dataplatform-dev",
            "keys": {
                "Befolkningen_etter_bydel_delby-J7khG": "raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv"
            },
        },
        None,
    )
