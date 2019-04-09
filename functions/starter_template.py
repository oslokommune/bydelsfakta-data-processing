import math
import time

import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output


def read_from_s3(s3_key):
    # read from s3, renames `value_column` to value
    df = common.aws.read_from_s3(s3_key, value_column='Antall personer')

    # We use only sub districts
    df = df[df['delbydelid'].notnull()]

    # Add district number
    df = common.transform.add_district_id(df, 'Bydel')
    return df


def prepare(df):
    # Generate the dataframe we want to start aggregating on
    df = population(df)

    # Merging potentially more data frames
    # merge = common.aggregate_dfs.merge_dfs(df_1, df_2, suffixes=['_a', '_b'])
    # merge = common.aggregate_dfs.merge_dfs(merge, df_3, suffixes=['_b', '_c'])

    return df


def population(df):
    df = df.groupby(['district', 'delbydelid', 'date'])['value'].sum()
    return df.reset_index()


def generate(df):
    # list of labels containing values
    value_labels = ['value']

    # Create the df with only subdistricts
    sub_districts = prepare(df)

    # Create df with only districts
    districts = sub_districts.copy().groupby(['district', 'date'])[value_labels].sum().reset_index()

    # Create oslo total
    oslo = sub_districts.copy().groupby(['date'])[value_labels].sum().reset_index()
    oslo['district'] = "00"

    # Append them all to one dataframe
    sub_districts = sub_districts.append(districts)
    sub_districts = sub_districts.append(oslo)

    # Add ratios if needed
    # sub_districts = common.aggregate_dfs.add_ratios(sub_districts,
    #                                                data_points=['value'],
    #                                                ratio_of=['value_total'])

    # Convert data frame to list of dict
    output_list = common.transform_output.generate_output_list(sub_districts, 'c',
                                                               value_labels)

    return output_list


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


def handler(event, context):
    # These keys should be extracted from "event", but since we do not have the new pipeline yet
    # it needs to be hardcoded
    s3_key = 'raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv'

    source = read_from_s3(s3_key=s3_key)

    # Create historic and status
    historic = common.transform.historic(source)
    status = common.transform.status(source)

    # Generate the datasets
    historic = generate(*historic)
    status = generate(*status)

    # Write to intermediate, with timestamp as edition
    timestamp = math.floor(time.time())
    historic_output_key = f"intermediate/green/innvandring_befolkning_histori-Sq5Se/version=1-B87VtKUW/edition={timestamp}/"

    # Write back to s3
    #write(historic, historic_output_key)
    print(historic)
    # Write to intermediate, with timestamp as edition
    timestamp = math.floor(time.time())
    status_output_key = f"intermediate/green/STATUSID/version=1-B87VtKUW/edition={timestamp}/"

    # Write back to s3
    #write(status, status_output_key)

    return [historic_output_key, status_output_key]


if __name__ == '__main__':
    handler({}, {})
