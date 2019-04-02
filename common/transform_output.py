
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

    * template: template to be used for generating output json
        valid values: 'a', 'c', 'i' so far...

    * data_points: List of column labels for the values that will be aggregated
        The labels in this list must correspond to existing column labels in the input dataframe.
        If the dataframe also contains ratio columns('data_point_ration'), these should not be in the data_points parameter list
"""

def generate_output_list(df, template, data_points):
    district_list = [x for x in df['district'].unique() if x not in ['00', '16', '17', '99']]
    output_list = []
    oslo_total = [district_time_series(df, '00', template, data_points, total_row=True)]
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
    time_series = [district_time_series(df, '00', template, data_points, total_row=True),
                   district_time_series(df, district, template, data_points, avg_row=True)]

    district_df = df[df['district'] == district]
    sub_districts = district_df[district_df['delbydelid'].notnull()]['delbydelid'].unique()
    for sub_district in sub_districts:
        time_series.append(sub_district_time_series(district_df, sub_district, template, data_points))
    return time_series


def district_time_series(df, district, template, data_points, avg_row=False, total_row=False):
    district_df = df[df['district'] == district]
    district_df = district_df[district_df['delbydelid'].isnull()]
    return df_to_template(district, district_df, template, data_points, avg_row=avg_row, total_row=total_row)


def sub_district_time_series(district_df, sub_district, template, data_points):
    sub_district_df = district_df[district_df['delbydelid'] == sub_district]
    return df_to_template(sub_district, sub_district_df, template, data_points)


def df_to_template(geography, df, template, data_points, avg_row=False, total_row=False):
    if template.lower() == 'a':
        return df_to_template_a(geography, df, data_points, avg_row=avg_row, total_row=total_row)
    elif template.lower() == 'c':
        return df_to_template_c(geography, df, data_points, avg_row=avg_row, total_row=total_row)
    elif template.lower() == 'i':
        return df_to_template_i(geography, df, data_points, avg_row=avg_row, total_row=total_row)
    else:
        raise Exception(f'Template {template} does not exist')


def df_to_template_a(geography, df, data_points, avg_row=False, total_row=False, link_to=False):
    obj_a = {
        'linkTo': link_to,
        'geography': geography,
        'avgRow': avg_row,
        'totalRow': total_row,
        'values': []
    }
    series = {}
    for values in df.to_dict('r'):
        for data_point in data_points:
            series[data_point] = value_entry(values, data_point)
    [obj_a['values'].append(series[data_point]) for data_point in data_points]
    return obj_a


def df_to_template_b(geography, df, data_points, avg_row=False, total_row=False, link_to=False):
    if len(data_points) > 1:
        raise Exception('Template B only takes one datapoint')
    obj_a = {
        'geography': geography,
        'avgRow': avg_row,
        'totalRow': total_row,
        'values': []
    }
    value_list = []
    data_point = data_points[0]
    for values in df.to_dict('r'):
        value_list.append(value_entry(values, data_point))
    obj_a['values'] = value_list
    return obj_a


def df_to_template_c(geography, df, data_points, avg_row=False, total_row=False):
    obj_c = {
        'geography': geography,
        'values': [],
        'avgRow': avg_row,
        'totalRow': total_row
    }
    time_series = list_to_time_series(data_points)
    for values in df.to_dict('r'):
        [time_series[data_point].append(value_entry(values, data_point))
         for data_point in data_points]


    [obj_c['values'].append(time_series[data_point]) for data_point in data_points]
    return obj_c


def df_to_template_i(geography, df, data_points, avg_row=False, total_row=False):
    obj_i = {
        'geography': geography,
        'values': [],
        'avgRow': avg_row,
        'totalRow': total_row
    }
    series = {}
    for values in df.to_dict('r'):
        for data_point in data_points:
            series[data_point] = value_entry(values, data_point)

    [obj_i['values'].append(series[data_point]) for data_point in data_points]
    return obj_i


def list_to_time_series(data_points):
    d = {}
    for data_point in data_points:
        d[data_point] = []
    return d


def value_entry(values, data_point):
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
