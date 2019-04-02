import math
import time

import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output


def read_from_s3(origin_by_age_key, botid_key, befolkning_key):
    # read from s3
    origin_by_age = common.aws.read_from_s3(origin_by_age_key, value_column='Antall personer')
    # We only want to keep sub-districts in this case
    origin_by_age = origin_by_age[origin_by_age['delbydelid'].notnull()]
    # Add district number
    origin_by_age = common.transform.add_district_id(origin_by_age, 'Bydel')

    # repeat for each input ds
    livage = common.aws.read_from_s3(botid_key, value_column='antall')
    livage = livage[livage['delbydelid'].notnull()]
    livage = common.transform.add_district_id(livage, 'Bydel')

    population_df = common.aws.read_from_s3(befolkning_key, value_column='Antall personer')
    population_df = population_df[population_df['delbydelid'].notnull()]
    population_df = common.transform.add_district_id(population_df, 'Bydel')

    return origin_by_age, livage, population_df


def prepare(origin_by_age, livage, population_df):
    # generates each of the datasets, and merge them to one data frame
    population_df = population(population_df)
    one_parent, two_parents = by_parents(origin_by_age)
    short_liveage, long_liveage = by_liveage(livage)

    merge = common.aggregate_dfs.merge_dfs(one_parent, two_parents, suffixes=['_a', '_b'])
    merge = common.aggregate_dfs.merge_dfs(merge, short_liveage, suffixes=['_b', '_c'])
    merge = common.aggregate_dfs.merge_dfs(merge, long_liveage, suffixes=['_c', '_d'])
    merge = common.aggregate_dfs.merge_dfs(merge, population_df)

    return merge


def population(df):
    df = df.groupby(['district', 'delbydelid', 'date'])['value'].sum()
    return df.reset_index()


def by_parents(df):
    df = df.groupby(['district', 'delbydelid', 'date', 'Innvandringskategori'])['value'].sum().reset_index()
    two_parents = df[df['Innvandringskategori'] == "Norskfødt med innvandrerforeldre"]
    one_parent = df[df['Innvandringskategori'] == "Norskfødt med en utenlandskfødt forelder"]
    return one_parent.drop(columns=['Innvandringskategori']), two_parents.drop(columns=['Innvandringskategori'])


def by_liveage(liveage):
    liveage = liveage.groupby(['district', 'delbydelid', 'date', 'Botid'])['value'].sum().reset_index()

    long_liveage_str = 'Innvandrer, lang botid (>5 år)'
    short_liveage_str = 'Innvandrer, kort botid (<=5 år)'

    long_liveage = liveage[liveage['Botid'] == long_liveage_str]
    short_liveage = liveage[liveage['Botid'] == short_liveage_str]

    return short_liveage.drop(columns=['Botid']), long_liveage.drop(columns=['Botid'])


def generate(origin_by_age_df, livage_df, population_df):
    # list of labels containing values
    value_labels = ['value_a', 'value_b', 'value_c', 'value_d', 'value']

    # Create the df with only subdistricts
    sub_districts = prepare(origin_by_age=origin_by_age_df, livage=livage_df, population_df=population_df)

    aggregations = [
        {'agg_func': 'sum',
         'data_points': 'value_a'},
        {'agg_func': 'sum',
         'data_points': 'value_b'},
        {'agg_func': 'sum',
         'data_points': 'value_c'},
        {'agg_func': 'sum',
         'data_points': 'value_d'},
        {'agg_func': 'sum',
         'data_points': 'value'}
    ]

    common.aggregate_dfs.aggregate_from_subdistricts(sub_districts, aggregations)

    sub_districts = common.aggregate_dfs.add_ratios(sub_districts,
                                                    data_points=['value_a', 'value_b', 'value_c', 'value_d'],
                                                    ratio_of=['value'])

    output_list = common.transform_output.generate_output_list(sub_districts, 'c',
                                                               value_labels)

    return output_list


def write(output_list, output_key):
    series = [
        {"heading": "Norskfødt med en utenlandskfødt forelder", "subheading": ""},
        {"heading": "Norskfødt med innvandrerforeldre", "subheading": ""},
        {"heading": "Innvandrer, kort botid (<=5 år)", "subheading": ""},
        {"heading": "Innvandrer, lang botid (>5 år)", "subheading": ""},
        {"heading": "Befolkning", "subheading": ""},
    ]
    heading = "Innvandring befolkning"

    common.aws.write_to_intermediate(
            output_key=output_key,
            output_list=output_list,
            heading=heading,
            series=series
    )


def handler(event, context):
    # These keys should be extracted from "event", but since we do not have the new pipeline yet
    # it needs to be hardcoded
    befolkning_key = 'raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv'
    botid_key = 'raw/green/botid_ikke_vestlige-nHwGw/version=1-m3xuXU7K/edition=EDITION-jUzi5/Botid_ikke_vestlige(1.1.2008-1.1.2018-v01).csv'
    origin_by_age_key = 'raw/green/Landbakgrunn_etter_alder-Yh7UC/version=1-tidZ36sM/edition=EDITION-BFMSo/Landbakgrunn_etter_alder(1.1.2008-1.1.2018-v01).csv'

    source = read_from_s3(origin_by_age_key=origin_by_age_key,
                          botid_key=botid_key,
                          befolkning_key=befolkning_key)

    historic = common.transform.historic(*source)
    status = common.transform.status(*source)

    historic = generate(*historic)
    status = generate(*status)

    timestamp = math.floor(time.time())
    historic_output_key = f"intermediate/green/innvandring_befolkning_histori-Sq5Se/version=1-B87VtKUW/edition={timestamp}/"

    # Write back to s3
    write(historic, historic_output_key)

    timestamp = math.floor(time.time())
    status_output_key = f"intermediate/green/STATUSID/version=1-B87VtKUW/edition={timestamp}/"

    # Write back to s3
    write(status, status_output_key)

    return [historic_output_key, status_output_key]


if __name__ == '__main__':
    handler({}, {})
