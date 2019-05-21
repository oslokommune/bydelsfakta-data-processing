"""
generate_output_list:
    Arguments:
    * df: input dataframe
        Input dataframe must have the following columns:
        | delbydelid | district | date | data_point1 - n (values) |

        delbydelid: 10-character code for sub_district
        district: 2-digit code for district
        date: year
        data_point1- n: One column for each data_point

        The user can optionally pass in 'bydel_navn' and 'delbydel_navn'
        columns and have these be used to populate the 'geography' fields.
        The 'delbydelid' and 'district' columns will then instead be used
        to populate a new 'id' field in the output.

    * template: template to be used for generating output json
        valid values: 'a', 'c', 'i' so far...

    * data_points: List of column labels for the values that will be aggregated
        The labels in this list must correspond to existing column labels in the input dataframe.
        If the dataframe also contains ratio columns('data_point_ration'), these should not be in the data_points parameter list
"""


def generate_output_list(df, template, data_points):
    if "bydel_navn" not in df:
        df["bydel_navn"] = None
        district_name_all = None
    else:
        district_name_all = "Oslo i alt"

    district_list = [
        (x.district, x.bydel_navn)
        for x in set(df.loc[:, ["district", "bydel_navn"]].itertuples(index=False))
        if x.district not in ["00", "16", "17", "99"]
    ]
    district_list.sort()

    output_list = []
    oslo_total = [
        district_time_series(
            df,
            "00",
            template,
            data_points,
            district_name=district_name_all,
            total_row=True,
        )
    ]
    for (district_id, district_name) in district_list:
        obj = {
            "district": district_name or district_id,
            "template": template,
            "data": district_time_series_list(
                df, district_id, template, data_points, district_name=district_name
            ),
        }
        if district_name:
            obj["id"] = district_id

        output_list.append(obj)
        oslo_total.append(
            district_time_series(
                df, district_id, template, data_points, district_name=district_name
            )
        )

    if district_name_all:
        output_list.append(
            {
                "id": "00",
                "district": "Oslo i alt",
                "template": template,
                "data": oslo_total,
            }
        )
    else:
        output_list.append({"district": "00", "template": template, "data": oslo_total})

    return output_list


def district_time_series_list(
    df, district_id, template, data_points, district_name=None
):
    district_name_all = "Oslo i alt" if district_name else None

    time_series = [
        district_time_series(
            df,
            "00",
            template,
            data_points,
            district_name=district_name_all,
            total_row=True,
        ),
        district_time_series(
            df,
            district_id,
            template,
            data_points,
            district_name=district_name,
            avg_row=True,
        ),
    ]

    district_df = df[df["district"] == district_id]

    sub_districts_df = district_df[district_df["delbydelid"].notnull()]
    if "delbydel_navn" not in sub_districts_df:
        sub_districts_df["delbydel_navn"] = None
    sub_districts_df = sub_districts_df[["delbydelid", "delbydel_navn"]]

    sub_districts = [
        (x.delbydelid, x.delbydel_navn)
        for x in set(sub_districts_df.itertuples(index=False))
    ]
    sub_districts.sort()

    for (sub_district_id, sub_district_name) in sub_districts:
        time_series.append(
            sub_district_time_series(
                district_df,
                sub_district_id,
                template,
                data_points,
                sub_district_name=sub_district_name,
            )
        )

    return time_series


def district_time_series(
    df,
    district_id,
    template,
    data_points,
    district_name=None,
    avg_row=False,
    total_row=False,
):
    district_df = df[df["district"] == district_id]
    district_df = district_df[district_df["delbydelid"].isnull()]
    return df_to_template(
        district_id,
        district_df,
        template,
        data_points,
        geography_name=district_name,
        avg_row=avg_row,
        total_row=total_row,
    )


def sub_district_time_series(
    district_df, sub_district_id, template, data_points, sub_district_name=None
):
    sub_district_df = district_df[district_df["delbydelid"] == sub_district_id]
    return df_to_template(
        sub_district_id,
        sub_district_df,
        template,
        data_points,
        geography_name=sub_district_name,
    )


def df_to_template(
    geography_id,
    df,
    template,
    data_points,
    geography_name=None,
    avg_row=False,
    total_row=False,
):
    if template.lower() == "a":
        template_fun = df_to_template_a
    elif template.lower() == "b":
        template_fun = df_to_template_b
    elif template.lower() == "c":
        template_fun = df_to_template_c
    elif template.lower() == "i":
        template_fun = df_to_template_i
    elif template.lower() == "j":
        template_fun = df_to_template_j
    else:
        raise Exception(f"Template {template} does not exist")

    return template_fun(
        geography_id,
        df,
        data_points,
        geography_name=geography_name,
        avg_row=avg_row,
        total_row=total_row,
    )


def df_to_template_a(
    geography_id,
    df,
    data_points,
    geography_name=None,
    avg_row=False,
    total_row=False,
    link_to=False,
):

    obj_a = {
        "geography": geography_name or geography_id,
        "linkTo": link_to,
        "avgRow": avg_row,
        "totalRow": total_row,
        "values": [],
    }
    if geography_name:
        obj_a["id"] = geography_id

    series = {}
    for values in df.to_dict("r"):
        for data_point in data_points:
            series[data_point] = value_entry(values, data_point)
    [obj_a["values"].append(series[data_point]) for data_point in data_points if series]
    return obj_a


def df_to_template_b(
    geography_id,
    df,
    data_points,
    geography_name=None,
    avg_row=False,
    total_row=False,
    link_to=False,
):
    if len(data_points) > 1:
        raise Exception("Template B only takes one datapoint")
    obj_b = {
        "geography": geography_name or geography_id,
        "avgRow": avg_row,
        "totalRow": total_row,
        "values": [],
    }
    if geography_name:
        obj_b["id"] = geography_id

    value_list = []
    data_point = data_points[0]
    for values in df.to_dict("r"):
        value_list.append(value_entry(values, data_point))
    obj_b["values"] = value_list
    return obj_b


def df_to_template_c(
    geography_id, df, data_points, geography_name=None, avg_row=False, total_row=False
):

    obj_c = {
        "geography": geography_name or geography_id,
        "values": [],
        "avgRow": avg_row,
        "totalRow": total_row,
    }
    if geography_name:
        obj_c["id"] = geography_id

    time_series = list_to_time_series(data_points)
    for values in df.to_dict("r"):
        [
            time_series[data_point].append(value_entry(values, data_point))
            for data_point in data_points
        ]

    [obj_c["values"].append(time_series[data_point]) for data_point in data_points]
    return obj_c


def df_to_template_i(
    geography_id, df, data_points, geography_name=None, avg_row=False, total_row=False
):

    obj_i = {
        "geography": geography_name or geography_id,
        "values": [],
        "avgRow": avg_row,
        "totalRow": total_row,
    }
    if geography_name:
        obj_i["id"] = geography_id

    series = {}
    for values in df.to_dict("r"):
        for data_point in data_points:
            series[data_point] = value_entry(values, data_point)

    [obj_i["values"].append(series[data_point]) for data_point in data_points]
    return obj_i


def df_to_template_j(
    geography_id, df, data_points, geography_name=None, avg_row=False, total_row=False
):

    obj_j = {
        "geography": geography_name or geography_id,
        "values": [],
        "avgRow": avg_row,
        "totalRow": total_row,
    }
    if geography_name:
        obj_j["id"] = geography_id

    data_row = df.to_dict("records")[
        0
    ]  # df has only one row - the status for a geography
    values = []

    for data_point in data_points:
        single_value = {"date": data_row["date"], "value": data_row[data_point]}
        ratio_field = f"{data_point}_ratio"
        if ratio_field in data_row.keys():
            single_value["ratio"] = data_row[ratio_field]
        values.append(single_value)

    obj_j["values"] = values

    return obj_j


def list_to_time_series(data_points):
    d = {}
    for data_point in data_points:
        d[data_point] = []
    return d


def value_entry(values, data_point):

    if f"{data_point}_ratio" in values:
        return {
            "value": values[data_point],
            "ratio": values[f"{data_point}_ratio"],
            "date": values["date"],
        }
    else:
        return {"value": values[data_point], "date": values["date"]}
