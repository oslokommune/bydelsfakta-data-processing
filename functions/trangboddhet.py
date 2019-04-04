import math
import time

import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output


def read_from_s3(s3_key):

    # read from s3, renames `value_column` to value
    df = common.aws.read_from_s3(s3_key)

    # We use only sub districts
    # df = df[df['delbydelid'].notnull()]

    # Add district number
    # df = common.transform.add_district_id(df)
    return df


def prepare(df):

    # Generate the dataframe we want to start aggregating on
    df = population(df)

    return df


def population(df):

    df = df.groupby(['district', 'delbydelid', 'date'])['value'].sum()
    return df.reset_index()


def generate(df, value_labels):

    # Only generate the aggregated set, not (yet) create the outputs

    aggregations = [{'agg_func': 'sum', 'data_points': vl} for vl in value_labels]

    agg_df = common.aggregate_dfs.aggregate_from_subdistricts(df, aggregations)

    agg_df = common.aggregate_dfs.add_ratios(agg_df, data_points=value_labels, ratio_of=value_labels)

    return agg_df


def write(output_list, output_key):

    # Fix proper headings
    series = [
        {"heading": "!! Some Heading for this Series !! ", "subheading": ""},
    ]
    heading = "Some Heading"

    common.aws.write_to_intermediate(
            output_key=output_key,
            output_list=output_list,
            heading=heading,
            series=series
    )


def data_processing(df):

    # This function is testable.

    # We use only sub districts
    df = df[df['delbydelid'].notnull()]

    # Add district number
    df = common.transform.add_district_id(df)

    value_labels = ['Personer per rom - 0,5 - 0,9',
                    'Personer per rom - 2,0 og over',
                    'Personer per rom - 1,0 - 1,9',
                    'Personer per rom - Under 0,5']

    # Create historic and status data (at sub_district level)
    historic = common.transform.historic(df)
    status = common.transform.status(df)

    # Generate the aggregated datasets
    historic_agg = generate(*historic, value_labels)
    status_agg = generate(*status, value_labels)

    print('STATUS_AGG')
    print(status_agg)
    print('HISTORIC_AGG')
    print(historic_agg)

    # Make output
    output_data = {}

    output_data['trangboddhet_alle_status'] = \
        common.transform_output.generate_output_list(status_agg, 'j', value_labels)
    output_data['trangboddhet_alle_historisk'] = \
        common.transform_output.generate_output_list(historic_agg, 'c', value_labels)

    output_data['trangboddhet_under0.5_status'] = \
        common.transform_output.generate_output_list(status_agg, 'a', ['Personer per rom - Under 0,5'])
    output_data['trangboddhet_under0.5_historisk'] = \
        common.transform_output.generate_output_list(historic_agg, 'b', ['Personer per rom - Under 0,5'])

    output_data['trangboddhet_0.5-0.9_status'] = \
        common.transform_output.generate_output_list(status_agg, 'a', ['Personer per rom - 0,5 - 0,9'])
    output_data['trangboddhet_0.5-0.9_historisk'] = \
        common.transform_output.generate_output_list(historic_agg, 'b', ['Personer per rom - 0,5 - 0,9'])

    output_data['trangboddhet_1.0-1.9_status'] = \
        common.transform_output.generate_output_list(status_agg, 'a', ['Personer per rom - 1,0 - 1,9'])
    output_data['trangboddhet_1.0-1.9_historisk'] = \
        common.transform_output.generate_output_list(historic_agg, 'b', ['Personer per rom - 1,0 - 1,9'])

    output_data['trangboddhet_over2_status'] = \
        common.transform_output.generate_output_list(status_agg, 'a', ['Personer per rom - 2,0 og over'])
    output_data['trangboddhet_over2_historisk'] = \
        common.transform_output.generate_output_list(historic_agg, 'b', ['Personer per rom - 2,0 og over'])

    return output_data


def handler(event, context):

    # These keys should be extracted from "event", but since we do not have the new pipeline yet
    # it needs to be hardcoded

    s3_key = r'raw/green/Husholdninger_etter_rom_per_pe-48LKF/version=1-oPutm8TS/edition=EDITION-3mQwN/Husholdninger_etter_rom_per_person(1.1.2015-1.1.2017-v01).csv'

    source = read_from_s3(s3_key=s3_key)

    output_data = data_processing(source)

    # Write to intermediate, with timestamp as edition
    for output_data_name in output_data:

        timestamp = math.floor(time.time())

        if output_data_name == 'trangboddhet_alle_historisk':
            # These values are read manually from the data set overview.
            # Will soon get them from a solution based on metadata.
            dataset_ID = 'trangboddhet_alle_historisk-4DAEn'
            ver_ID = '1-NpEWu8Kp'
        else:
            continue  # Just temporarily until the other dataset_IDs are found.

        output_key = f'intermediate/green/{dataset_ID}/version={ver_ID}/edition={timestamp}/'

        # Write back to s3
        write(output_data[output_data_name], output_key)

    # To be removed - temporary dump for provide the Frontend guys with the latest output.
    #import json
    #with open(r'C:\CURRENT FILES\dump.json', 'wt', encoding='utf-8') as f:
    #    json.dump(output_data, f, indent=4)

    output_keys = list(output_data.keys())

    return output_keys


if __name__ == '__main__':
    handler({}, {})
