from dataclasses import dataclass

import pandas as pd

from common.aggregateV2 import ColumnNames

pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)
column_names = ColumnNames()
dtype = {
    column_names.date: int,
    column_names.district_id: object,
    column_names.district_name: object,
    column_names.sub_district_id: object,
    column_names.sub_district_name: object,
}


@dataclass
class Dataset:
    path: str

    def content(self):
        if not self.path:
            raise NotImplementedError
        return pd.read_csv(self.path, sep=";", dtype=dtype).rename(
            columns={"aar": "date"}
        )


husholdinger = Dataset(path="tests/husholdning_test_input.csv")
