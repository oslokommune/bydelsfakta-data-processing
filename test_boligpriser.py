from unittest import TestCase
import boligpriser as bp
import os, json, unittest
import logging
#logging.basicConfig(level=logging.DEBUG)

class TestRead_csv(TestCase):
    def test_read_csv(self):
        bp.read_csv()
        for file in os.listdir("out/"):
            logging.debug("File: {file}".format(file=file))
            with open("out/{file}".format(file=file), 'r') as jsonfile:
                json_list: list = json.loads(jsonfile.read())
                if not file.startswith('Marka'):
                    self.assertTrue(contains_all_json(json_list))


def contains_all_json(json_list):
    districts = True # TODO: Check that all districts are present
    area = False
    oslo = False

    for item in json_list['data']:
        if 'totalRow' in item:
            oslo = True
        if 'avgRow' in item:
            area = True
        if districts & area & oslo:
            return districts & area & oslo

    logging.debug("district: {file}".format(file=districts))
    logging.debug("area: {file}".format(file=area))
    logging.debug("oslo: {file}".format(file=oslo))
    return False
