import itertools

from common.aggregateV2 import ColumnNames


def value_with_ratio(date, value, ratio):
    return {
        "date": date,
        "value": value,
        "ratio": ratio
    }

def value(date, value):
    return {
        "date": date,
        "value": value
    }


class Template:
    def values(self, df, series, column_names: ColumnNames):
        """
        Method for creating the values used in the output json structure
        :param df: dataframe
        :param series: list of columns to use as values
        """
        raise NotImplementedError()


class TemplateC(Template):
    def __init__(self, ratios):
        self.ratios = ratios

    def _value(self, df, column_names, value_column, ratios):
        if ratios:
            value_collection = df.apply(
                    lambda row: value_with_ratio(date=row[column_names.date],
                                                 value=row[value_column],
                                                 ratio=row[f"{value_column}_ratio"]),
                    axis=1)
        else:
            value_collection = df.apply(
                    lambda row: value(date=row[column_names.date], value=row[value_column]),
                    axis=1)
        return value_collection.tolist()

    def values(self, df, series, column_names):
        return [self._value(df, column_names=column_names,  value_column=s, ratios=self.ratios) for s in series]


class TemplateA(Template):
    def __init__(self, ratios):
        self.ratios = ratios

    def _value(self, df, column_names, value_column, ratios):
        if ratios:
            value_collection = df.apply(
                    lambda row: value_with_ratio(date=row[column_names.date],
                                                 value=row[value_column],
                                                 ratio=row[f"{value_column}_ratio"]),
                    axis=1)
        else:
            value_collection = df.apply(
                    lambda row: value(date=row[column_names.date], value=row[value_column]),
                    axis=1)
        return value_collection.tolist()

    def values(self, df, series, column_names):
        list_of_lists = [self._value(df, column_names=column_names, value_column=s, ratios=self.ratios) for s in series]
        return list(itertools.chain(*list_of_lists)) # flatten list
