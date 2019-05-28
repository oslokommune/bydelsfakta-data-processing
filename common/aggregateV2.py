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
    aggregate_config: any
    column_names: ColumnNames = ColumnNames()

    def meta_columns(self, extra_groupby_columns):
        return [
            self.column_names.date,
            self.column_names.district_id,
            self.column_names.district_name,
            self.column_names.sub_district_id,
            self.column_names.sub_district_name,
        ] + extra_groupby_columns

    def district_columns(self, extra_groupby_columns):
        return [
            self.column_names.date,
            self.column_names.district_id,
            self.column_names.district_name,
        ] + extra_groupby_columns

    def oslo_columns(self, extra_groupby_columns):
        return [self.column_names.date] + extra_groupby_columns

    def aggregate(self, df, extra_groupby_columns=None):
        if not extra_groupby_columns:
            extra_groupby_columns = []

        sub_districts = (
            df.groupby(self.meta_columns(extra_groupby_columns))
            .agg(self.aggregate_config)
            .reset_index()
        )
        districts = (
            df.groupby(self.district_columns(extra_groupby_columns))
            .agg(self.aggregate_config)
            .reset_index()
        )
        oslo = (
            districts.groupby(self.oslo_columns(extra_groupby_columns))
            .agg(self.aggregate_config)
            .reset_index()
        )

        oslo[self.column_names.district_id] = "00"
        oslo[self.column_names.district_name] = "Oslo i alt"
        return pd.concat([districts, oslo, sub_districts])

    def merge(self, df, df2):
        return pd.merge(left=df, right=df2)

    def add_ratios(self, df, data_points, ratio_of):
        sums = df[ratio_of].sum(axis=1)
        for dp in data_points:
            col_name = "{dp}_ratio".format(dp=dp)
            df[col_name] = df[dp] / sums
        return df
