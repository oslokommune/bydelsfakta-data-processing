from numpy import nan

with_data_points_expected = [
    {
        'delbydelid': '0301010101',
        'date': 2008,
        'district': '01',
        'no_children': 1.0,
        'single_adult': 1.0,
        'with_children': 0.0
    },
    {
        'delbydelid': '0301010101',
        'date': 2009,
        'district': '01',
        'no_children': 2.0,
        'single_adult': 1.0,
        'with_children': 0.0
    },
    {
        'delbydelid':'0301010103',
        'date': 2008,
        'district': '01',
        'no_children': 0.0,
        'single_adult': 0.0,
        'with_children': 1.0
    },
    {
        'delbydelid': '0301010103',
        'date': 2009,
        'district': '01',
        'no_children': 0.0,
        'single_adult': 0.0,
        'with_children': 2.0
    }
]

input_df_expected = '[{"delbydelid": "0301010101", "date": 2008, "district": "01", "no_children": 1.0, "single_adult": 1.0, "with_children": 0.0, "single_adult_ratio": 0.5, "no_children_ratio": 0.5, "with_children_ratio": 0.0}, {"delbydelid": "0301010101", "date": 2009, "district": "01", "no_children": 2.0, "single_adult": 1.0, "with_children": 0.0, "single_adult_ratio": 0.3333333333333333, "no_children_ratio": 0.6666666666666666, "with_children_ratio": 0.0}, {"delbydelid": "0301010103", "date": 2008, "district": "01", "no_children": 0.0, "single_adult": 0.0, "with_children": 1.0, "single_adult_ratio": 0.0, "no_children_ratio": 0.0, "with_children_ratio": 1.0}, {"delbydelid": "0301010103", "date": 2009, "district": "01", "no_children": 0.0, "single_adult": 0.0, "with_children": 2.0, "single_adult_ratio": 0.0, "no_children_ratio": 0.0, "with_children_ratio": 1.0}, {"delbydelid": NaN, "date": 2008, "district": "01", "no_children": 1.0, "single_adult": 1.0, "with_children": 1.0, "single_adult_ratio": 0.3333333333333333, "no_children_ratio": 0.3333333333333333, "with_children_ratio": 0.3333333333333333}, {"delbydelid": NaN, "date": 2009, "district": "01", "no_children": 2.0, "single_adult": 1.0, "with_children": 2.0, "single_adult_ratio": 0.2, "no_children_ratio": 0.4, "with_children_ratio": 0.4}, {"delbydelid": NaN, "date": 2008, "district": "00", "no_children": 1.0, "single_adult": 1.0, "with_children": 1.0, "single_adult_ratio": 0.3333333333333333, "no_children_ratio": 0.3333333333333333, "with_children_ratio": 0.3333333333333333}, {"delbydelid": NaN, "date": 2009, "district": "00", "no_children": 2.0, "single_adult": 1.0, "with_children": 2.0, "single_adult_ratio": 0.2, "no_children_ratio": 0.4, "with_children_ratio": 0.4}]'