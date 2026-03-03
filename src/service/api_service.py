import requests

class APIService:

    def get_request(self, url, params):
        try:
            response = requests.get(url, params=params)
            data = response.json()
            return data
        except:
            print("Error in get_request")
            raise