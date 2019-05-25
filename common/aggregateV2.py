from dataclasses import dataclass

import pandas as pd



@dataclass
class ColumnNames:
    date: str = "date"
    district_name: str = "bydel_navn"
    district_id: str = "bydel_id"
    sub_district_id: str = "delbydel_id"
    sub_district_name: str = "delbydel_navn"


"""
Column names should probably be left as default ( see above )

:aggregate_config is a dict with key = colum name and value can be any numpy aggregation functions: e.g. sum / max / min / mean /

"""
@dataclass
class Aggregate:
    column_names: ColumnNames
    aggregate_config: dict


    def aggregate(self, df):
        groupby_columns = [
            self.column_names.date,
            self.column_names.district_id,
            self.column_names.district_name,
        ]
        districts = df.groupby(groupby_columns).agg(self.aggregate_config).reset_index()
        oslo = districts.groupby(self.column_names.date).agg(self.aggregate_config).reset_index()
        oslo[self.column_names.district_id] = "00"
        oslo[self.column_names.district_name] = "Oslo i alt"
        return pd.concat([districts, oslo, df])

    def add_ratios(self, df, data_points, ratio_of):
        sums = df[ratio_of].sum(axis=1)
        for dp in data_points:
            col_name = "{dp}_ratio".format(dp=dp)
            df[col_name] = df[dp] / sums
        return df


