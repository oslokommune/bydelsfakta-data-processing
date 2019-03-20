import common.aws
import common.transform
import common.util


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event['keys']['boligpriser-urFqK']
    bucket = event['bucket']
    start(bucket, s3_key)
    return "OK"


def start(bucket, key):
    original = common.aws.read_from_s3(
            s3_key=key,
            value_column="kvmpris",
            date_column="År"
    )
    original = common.transform.add_district_id(original, "Oslo-Bydelsnavn")
    original = original.drop(
        columns=['antall omsatte blokkleieligheter', 'Oslo-Bydelsnavn', 'Delbydelnummer', 'Delbydelsnavn'])

    status = common.transform.status(original)
    historic = common.transform.historic(original)

    create_ds(bucket, "boligpriser_historic-4owcY", *historic)
    create_ds(bucket, "boligpriser_status-pD7ZV", *status)


def create_ds(bucket, dataset_id, df):
    heading = "Gjennomsnittpris (kr) pr kvm for blokkleilighet"
    series = [{'heading': 'Gjennomsnittpris (kr) pr kvm for blokkleilighet', 'subheading': ''}]

    # To json : convert df to list of json objects
    json = [{'x': """to_json(df)"""}]

    common.aws.write_to_intermediate(
            output_key=f"intermediate/green/{dataset_id}/version=1/edition=???/",
            heading=heading,
            series=series,
            output_list=json)
    return


if __name__ == '__main__':
    handler(
            {'bucket': 'ok-origo-dataplatform-dev',
             'keys': {
                 'boligpriser-urFqK': 'raw/green/boligpriser-urFqK/version=1-NrHiJxwf/edition=EDITION-EbL7P/Boligpriser.csv'}
             }
            , {})
