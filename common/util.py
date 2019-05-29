import requests

_districts_id = {
    "01": "Bydel Gamle Oslo",
    "02": "Bydel Grünerløkka",
    "03": "Bydel Sagene",
    "04": "Bydel St. Hanshaugen",
    "05": "Bydel Frogner",
    "06": "Bydel Ullern",
    "07": "Bydel Vestre Aker",
    "08": "Bydel Nordre Aker",
    "09": "Bydel Bjerke",
    "10": "Bydel Grorud",
    "11": "Bydel Stovner",
    "12": "Bydel Alna",
    "13": "Bydel Østensjø",
    "14": "Bydel Nordstrand",
    "15": "Byel Søndre Nordstrand",
    "16": "Sentrum",
    "17": "Marka",
    "99": "Uten registrert adresse",
}

_districts_name = {
    "Bydel Gamle Oslo": "01",
    "Bydel Grünerløkka": "02",
    "Bydel Sagene": "03",
    "Bydel St.Hanshaugen": "04",
    "Bydel St. Hanshaugen": "04",
    "Bydel St Hanshaugen": "04",
    "Bydel Frogner": "05",
    "Bydel Ullern": "06",
    "Bydel Vestre Aker": "07",
    "Bydel Nordre Aker": "08",
    "Bydel Bjerke": "09",
    "Bydel Grorud": "10",
    "Bydel Stovner": "11",
    "Bydel Alna": "12",
    "Bydel Østensjø": "13",
    "Bydel Nordstrand": "14",
    "Bydel Søndre Nordstrand": "15",
    "Sentrum": "16",
    "Marka": "17",
    "Uten registrert adresse": "99",
    "Oslo i alt": "00",
}


def get_district_id(name):
    if not name.startswith("Bydel ") and name not in [
        "Marka",
        "Sentrum",
        "Oslo i alt",
        "Uten registrert adresse",
    ]:
        name = f"Bydel {name}"
    return _districts_name[name]


def get_district_name(id):
    return _districts_id[id]

def get_latest_edition_of(dataset_id, confidentiality="green"):
    response = requests.get(f"https://metadata.api-test.oslo.kommune.no/dev/datasets/{dataset_id}/versions/1/editions").json()
    latest_edition = max(response, key=lambda x: x["Id"] if "Id" in x else -1)
    dataset_id, version, edition = latest_edition['Id'].split("/")
    distribution = requests.get(f"https://metadata.api-test.oslo.kommune.no/dev/datasets/{dataset_id}/versions/1/editions/{edition}/distributions").json()
    file_name = distribution[0]['filename']
    s3_key = f"raw/{confidentiality}/{dataset_id}/versions/1/editions/{edition}/{file_name}"
    return s3_key
