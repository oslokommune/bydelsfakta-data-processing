import itertools

from common.aggregateV2 import ColumnNames


def value(date, value, ratio=None):
    if ratio:
        return {"value": value, "ratio": ratio, "date": date}
    else:
        return {"value": value, "date": date}


class Template:
    def values(self, df, series, column_names: ColumnNames = ColumnNames()):
        """
        Method for creating the values used in the output json structure
        :param df: dataframe
        :param series: list of columns to use as values
        """
        raise NotImplementedError()


class TemplateC(Template):
    def _value(self, df, column_names, value_column):
        value_collection = df.apply(
            lambda row: value(
                date=row[column_names.date],
                value=row[value_column],
                ratio=row.get(f"{value_column}_ratio", None),
            ),
            axis=1,
        )
        if value_collection.empty:
            return []
        return value_collection.tolist()

    def values(self, df, series, column_names=ColumnNames()):
        return [
            self._value(df, column_names=column_names, value_column=s) for s in series
        ]


class TemplateA(Template):
    def _value(self, df, column_names, value_column):
        value_collection = df.apply(
            lambda row: value(
                date=row[column_names.date],
                value=row[value_column],
                ratio=row.get(f"{value_column}_ratio", None),
            ),
            axis=1,
        )
        if value_collection.empty:
            return []
        return value_collection.tolist()

    def values(self, df, series, column_names=ColumnNames()):
        list_of_lists = [
            self._value(df, column_names=column_names, value_column=s) for s in series
        ]
        return list(itertools.chain(*list_of_lists))  # flatten list


class TemplateB(TemplateA):
    pass


class TemplateI(TemplateA):
    pass


class TemplateJ(TemplateA):
    pass
