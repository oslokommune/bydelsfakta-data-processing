import common.aws
import common.transform
import common.util
import common.transform_output


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event['keys']['boligpriser-urFqK']
    start(s3_key)
    return "OK"


def start(key):
    original = common.aws.read_from_s3(
            s3_key=key
    )
    original = common.transform.add_district_id(original, "Oslo-Bydelsnavn")
    original = original.drop(
        columns=['antall omsatte blokkleieligheter', 'Oslo-Bydelsnavn', 'Delbydelnummer', 'Delbydelsnavn'])

    status = common.transform.status(original)
    historic = common.transform.historic(original)

    create_ds("boligpriser_historic-4owcY", *historic)
    create_ds("boligpriser_status-pD7ZV", *status)


def create_ds(dataset_id, df):
    heading = "Gjennomsnittpris (kr) pr kvm for blokkleilighet"
    series = [{'heading': 'Gjennomsnittpris (kr) pr kvm for blokkleilighet', 'subheading': ''}]

    # To json : convert df to list of json objects
    json = common.transform_output.generate_output_list(df, 'c', ['value'])
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
