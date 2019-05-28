import datetime
from dataclasses import dataclass, asdict

import pandas

from common.aggregateV2 import ColumnNames
from common.templates import Template


@dataclass
class Metadata:
    heading: str
    series: list
    publishedDate: str = str(datetime.date.today())
    help: str = "Dette er en beskrivelse for hvordan dataene leses"
    scope: str = "bydel"


@dataclass
class Output:
    values: list
    df: pandas.DataFrame
    template: Template
    metadata: Metadata
    column_names: ColumnNames = ColumnNames()

    def generate_output(self) -> list:
        if self.df[self.values].isnull().values.any():
            raise Exception("Some values have nan or null")
        # For each district create an output object. Oslo i alt is special so we append it later
        districts = [
            district_id
            for district_id in self.df[self.column_names.district_id].dropna().unique()
            if district_id not in ["16", "17", "99", "00"]
        ]
        print(districts)

        output_list = [
            self._generate_district(district, asdict(self.metadata))
            for district in districts
        ]
        output_list.append(self._generate_oslo_i_alt("00", asdict(self.metadata)))

        return output_list

    def _generate_district(self, district_id, metadata):
        df = self.df
        district = df[
            df[self.column_names.district_id] == district_id
        ]  # get data from the current district
        sub_districts = (
            district[self.column_names.sub_district_id].dropna().unique()
        )  # get all sub districts in current district

        district_name = (
            district[self.column_names.district_name].dropna().unique()[0]
        )  # Get the district name

        oslo = df[df[self.column_names.district_id] == "00"]  # Also get oslo i alt

        data = [
            self._generate_data(
                district[district[self.column_names.sub_district_id] == sub_district],
                district_id=district_id,
                geography_id=sub_district,
                name_column=self.column_names.sub_district_name,
            )
            for sub_district in sub_districts
        ]  # Create a data object for each sub district

        data.append(
            self._generate_data(
                district[district[self.column_names.sub_district_id].isna()],
                # Create a data object for the district itself
                district_id=district_id,
                geography_id=district_id,
                geography_name=district_name,
            )
        )

        data.append(
            self._generate_data(
                oslo,
                district_id=district_id,
                geography_id="00",
                geography_name=district_name,
            )
        )  # Create a data object for oslo i alt

        return {"bydel_id": district_id, "data": data, "meta": metadata}

    def _generate_oslo_i_alt(self, district_id, metadata):
        df = self.df
        all = df[df[self.column_names.sub_district_name].isna()]
        districts = all[self.column_names.district_id].dropna().unique()

        data = [
            self._generate_data(
                all[all[self.column_names.district_id] == district],
                district_id=district_id,
                geography_id=district,
                name_column=self.column_names.district_name,
            )
            for district in districts
            if district not in ["16", "17", "99"]
        ]
        metadata = {**metadata, "scope": "oslo i alt"}
        return {"bydel_id": district_id, "data": data, "meta": metadata}

    def _generate_data(
        self, df, district_id, geography_id, name_column=None, geography_name=None
    ):
        print(f"District: {district_id}")
        print(f"Geography: {geography_id}")
        if not geography_name:
            if len(df[name_column].unique()) > 1:
                raise Exception("Multiple names for one geography id")
            geography_name = df[name_column].unique()[
                0
            ]  # if no geo name is provided extrapolate from df

        avgRow = False
        totalRow = False

        # If the data is aggregated to district level
        if len(geography_id) == 2:
            if geography_id == "00":
                totalRow = True
                geography_name = "Oslo i alt"

            elif district_id != "00":  # If we are creating Oslo i alt district
                avgRow = True

        # If we are creating oslo i alt, in any district
        if geography_id == "00":
            avgRow = False
            totalRow = True

        # Use the provided template to generate a list of values
        values = self.template.values(
            df=df, series=self.values, column_names=self.column_names
        )

        # For each values needed, generate a value
        data = {
            "id": geography_id,
            "geography": geography_name,
            "values": values,
            "avgRow": avgRow,
            "totalRow": totalRow,
        }
        return data
