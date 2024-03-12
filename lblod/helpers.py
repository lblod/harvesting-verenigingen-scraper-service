import requests
import os


def get_access_token():
    authorization_key = os.environ['AUTHORIZATION_KEY']
    scope = os.environ['SCOPE']
    url = os.environ['ACCESS_TOKEN_URL']
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic " + authorization_key
    }
    data = {
        "grant_type": "client_credentials",
        "scope": scope
    }

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print("Error:", response.status_code)
        return None


def get_context(url):
    response = requests.get(url)
    if response.status_code == 200:
        context = response.json()
        context["doel"] = "https://data.vlaanderen.be/ns/"
        context["loc"] = "http://data.lblod.info/id/vestigingen/"
        return context
    else:
        print(
            f"Error fetching context url: {response.status_code}")
        return None
