
"""
generate_output_list:
    Arguments:
    * df: input dataframe
        Input dataframe must have the following columns:
        | sub_district | district | date | data_point1 - n (values) |

        sub_district: 4-character code for sub_district
        district: 2-digit code for district
        date: year
        data_point1- n: One column for each data_point

    * template: template to be used for generating output json
        valid values: 'a', 'c', 'i' so far...

    * data_points: List of column labels for the values that will be aggregated
        The labels in this list must correspond to existing column labels in the input dataframe.
        If the dataframe also contains ratio columns('data_point_ration'), these should not be in the data_points parameter list
"""


def generate_output_list(df, template, data_points):
    district_list =  list(
        filter(lambda x: x not in ['00', '16', '17', '99'], df['district'].unique())
        )
    output_list = []
    oslo_total = [district_time_series(df, '00', template, data_points)]
    for district in district_list:
        output_list.append({
            'district': district,
            'template': template,
            'data': district_time_series_list(df, district, template, data_points)
        })
        oslo_total.append(district_time_series(df, district, template, data_points))
    output_list.append({'district': '00', 'template': template, 'data': oslo_total})

    return output_list

def district_time_series_list(df, district, template, data_points):
    time_series = [district_time_series(df, '00', template, data_points), district_time_series(df, district, template, data_points)]
    sub_districts = list(
        filter(lambda x: x is not None, df[df['district'] == district]['sub_district'].unique())
        )
    for sub_district in sub_districts:
        time_series.append(sub_district_time_series(df, sub_district, template, data_points))
    return time_series


def district_time_series(df, district, template, data_points):
    district_df = df[df['district'] == district]
    district_df = district_df[district_df['sub_district'].isnull()]
    return df_to_template(district, district_df, template, data_points)


def sub_district_time_series(df, sub_district, template, data_points):
    sub_district_df = df[df['sub_district'] == sub_district]
    return df_to_template(sub_district, sub_district_df, template, data_points)


def df_to_template(geography, df, template, data_points):
    if template.lower() == 'a':
        return df_to_template_a(geography, df, data_points)
    elif template.lower() == 'c':
        return df_to_template_c(geography, df, data_points)
    elif template.lower() == 'i':
        return df_to_template_i(geography, df, data_points)
    else:
        raise Exception(f'Template {template} does not exist')


def df_to_template_a(geography, df, data_points):
    obj_a = {
        'linkTo': False,
        'geography': geography,
        'values': []
    }
    series = {}
    for values in df.to_dict('r'):
        for data_point in data_points:
            series[data_point] = value_entry_a(values, data_point)
    [obj_a['values'].append(series[data_point]) for data_point in data_points]
    return obj_a


def df_to_template_c(geography, df, data_points):
    obj_c = {
        'geography': geography,
        'values': [],
        'avgRow': False,
        'totalRow': False
    }
    time_series = list_to_time_series(data_points)
    for values in df.to_dict('r'):
        [time_series[data_point].append(value_entry_c(values, data_point))
         for data_point in data_points]


    [obj_c['values'].append(time_series[data_point]) for data_point in data_points]
    return obj_c


def df_to_template_i(geography, df, data_points):
    obj_i = {
        'geography': geography,
        'values': []
    }
    series = {}
    for values in df.to_dict('r'):
        for data_point in data_points:
            series[data_point] = value_entry_a(values, data_point)

    [obj_i['values'].append(series[data_point]) for data_point in data_points]
    return obj_i


def list_to_time_series(data_points):
    d = {}
    for data_point in data_points:
        d[data_point] = []
    return d


def value_entry_a(values, data_point):
    if f'{data_point}_ratio' in values:
        return {
            'value': values[data_point],
            'ratio': values[f'{data_point}_ratio']
        }
    else:
        return {
            'value': values[data_point]
        }


def value_entry_c(values, data_point):
    if f'{data_point}_ratio' in values:
        return {
            'value': values[data_point],
            'ratio': values[f'{data_point}_ratio'],
            'date': values['date']
        }
    else:
        return {
            'value': values[data_point],
            'date': values['date']
        }