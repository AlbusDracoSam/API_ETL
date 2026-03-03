import requests

from service.api_service import APIService
from resources.resources import URL, PARAMS
from utils.util import UtilService
from service.db_service import DBService

APIService = APIService()
UtilService = UtilService()
DBService = DBService()

def handler():

    data = APIService.get_request(URL, params=PARAMS)
    transformed = UtilService.transform_data(data)
    conn = DBService.connect()
    no_of_rows = UtilService.insert_data(conn, transformed)


if __name__ == "__main__":
    handler()
