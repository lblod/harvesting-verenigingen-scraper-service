import requests
import os
import jwt
from datetime import datetime, timedelta
import uuid
import subprocess
import json
import glob
from helpers import logger

def get_access_token():
    client_id = os.environ["CLIENT_ID"]
    environment = os.environ["MODE"]
    aud = os.environ["AUD"]
    host = os.environ["HOST"]
    scope = os.environ["SCOPE"]

    if(environment != "PROD"):
        authorization_key = os.environ["AUTHORIZATION_KEY"]
        url = f"{aud}/v1/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + authorization_key,
        }
        data = {"grant_type": "client_credentials", "scope": scope}

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json().get("access_token")
        else:
            print("Error:", response.status_code)
            return None
    else:
        iat = datetime.now().astimezone()
        exp = iat + timedelta(minutes=9)

        payload = {
            "iss": client_id,
            "sub": client_id,
            "aud": aud,
            "exp": int(exp.timestamp()),
            "jti": str(uuid.uuid4()),
            "iat": int(iat.timestamp())
        }
        config_path = '/config'
        if os.path.exists(config_path):
            pem_files = glob.glob(os.path.join(config_path, '*.pem'))

            if pem_files:
                first_pem_file = pem_files[0]
                with open(first_pem_file, 'r') as file:
                    key_test = file.read()
                print("First .pem file read successfully.")
            else:
                print("No .pem files found in the directory.")
        else:
            print(f"Directory '{config_path}' does not exist.")

        if(key_test):
            token = jwt.encode(payload, key_test, algorithm="RS256")

            curl_command = [
                "curl", "-v", "-X", "POST", f"https://{host}/op/v1/token",
                "-H", "Accept: application/json",
                "-H", "Content-Type: application/x-www-form-urlencoded",
                "--data-urlencode", "grant_type=client_credentials",
                "--data-urlencode", "client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "--data-urlencode", f"scope={scope}",
                "--data-urlencode", f"client_assertion={token}"
            ]
            curl_request_str = ' '.join(curl_command)
            print("\nCurl request:\n", curl_request_str)
            result = subprocess.run(curl_command, capture_output=True, text=True)
            access_token = json.loads(result.stdout)['access_token']
            return access_token


def get_context(url):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        context = response.json()
        context.update({
            "doel": "https://data.vlaanderen.be/ns/",
            "loc": "http://data.lblod.info/id/vestigingen/",
            "concept": "http://data.vlaanderen.be/id/concept/",
            "gestructureerdeIdentificator": "generiek:gestructureerdeIdentificator",
            "bestaatUit": "https://data.vlaanderen.be/ns/organisatie#bestaatUit",
            "startdatum": "pav:createdOn",
            "contactgegeventype": "foaf:name",
            "primairContact": "schema:contactType",
            "description": "dc:description",
            "vertegenwoordigers": "org:hasMembership",
            "lidmaatschap": "http://data.lblod.info/id/lidmaatschap/",
            "vertegenwoordigerPersoon": "org:member",
            "ere": "http://data.lblod.info/vocabularies/erediensten/",
            "adresvoorstelling": "locn:fullAddress",
            "datumLaatsteAanpassing": "pav:lastUpdateOn"
        })

        logger.info(f"Successfully fetched and updated context from {url}")
        return context

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching context from {url}: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred while fetching context from {url}: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred while fetching context from {url}: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request exception occurred while fetching context from {url}: {req_err}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching context from {url}: {e}")

    return None