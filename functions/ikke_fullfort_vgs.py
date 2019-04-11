import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output


def read_from_s3(s3_key):

    # read from s3, renames `value_column` to value
    df = common.aws.read_from_s3(s3_key)

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

    # Writes to AWS through boto (requires saml login)

    heading = 'Personer som ikke har fullført vgs'

    # Fix proper headings
    series = []

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

    value_labels = ['Antall personer, fullført i løpet av 5 år',
                    'Antall personer, ikke fullført i løpet av 5 år']

    df = df[['delbydelid', 'district', 'date', *value_labels]]

    # Create historic and status data (at sub_district level)
    historic = common.transform.historic(df)
    status = common.transform.status(df)

    # Generate the aggregated datasets
    historic_agg = generate(*historic, value_labels)
    status_agg = generate(*status, value_labels)

    # Make output
    output_data = {}

    output_data['levekar_vgs_status'] = \
        common.transform_output.generate_output_list(status_agg, 'a',
                                                     ['Antall personer, ikke fullført i løpet av 5 år'])
    output_data['levekar_vgs_historisk'] = \
        common.transform_output.generate_output_list(historic_agg, 'b',
                                                     ['Antall personer, ikke fullført i løpet av 5 år'])

    return output_data


def handler(event, context):

    # The key both for read_from and write_to should be extracted from incoming data, but since we do not have the new pipeline yet
    # it needs to be hardcoded

    s3_key = r'raw/green/Ikke_fullfort_vgs-Ax4GK/version=1-HfSeVeXa/edition=EDITION-C4ewk/Ikke_fullfort_vgs(2012-2017-v01).csv'

    source = read_from_s3(s3_key=s3_key)

    output_data = data_processing(source)

    # Write to intermediate (temporarily to processed)
    output_keys = []

    edition_ID = 'EDITION-oknXp'
    dataset_ID = 'Levekar-vgs-status-vFLio'
    ver_ID = '1-mrMP8MYz'
    # output_key = f'intermediate/green/{dataset_ID}/version={ver_ID}/edition={edition_ID}/'
    output_key = f'processed/green/{dataset_ID}/version={ver_ID}/edition={edition_ID}/'  # Temporary until the pipeline can handle data from intermediate
    write(output_data['levekar_vgs_status'], output_key)

    edition_ID = 'EDITION-QL4ph'
    dataset_ID = 'Levekar-vgs-historisk-t2ZTn'
    ver_ID = '1-wjTMpzic'
    # output_key = f'intermediate/green/{dataset_ID}/version={ver_ID}/edition={edition_ID}/'
    output_key = f'processed/green/{dataset_ID}/version={ver_ID}/edition={edition_ID}/'    # Temporary until the pipeline can handle data from intermediate
    write(output_data['levekar_vgs_historisk'], output_key)

    return output_keys


if __name__ == '__main__':
    handler({}, {})
