import requests
import os
import jwt
from datetime import datetime, timedelta
import uuid
import json
import glob
from helpers import logger
from constants import MUTATIEDIENST_URL

# Cache for authentication response
# Structure: { "access_token": "...", "expires_in": 3600, "request_datetime": datetime(...) }
_cached_authentication = None

def get_cached_token():
    """
    Returns a valid cached access token if available, otherwise None.
    A token is valid if it exists, has request_datetime and expires_in,
    and has not expired (with a 60 second margin).
    """
    if _cached_authentication and _cached_authentication.get("access_token") and _cached_authentication.get("request_datetime") and _cached_authentication.get("expires_in"):
        issued_at = _cached_authentication["request_datetime"]
        expires_in = int(_cached_authentication["expires_in"])
        now = datetime.now()
        # 60 seconds margin
        expiry = issued_at + timedelta(seconds=expires_in - 60)
        if now < expiry:
            return _cached_authentication["access_token"]

    return None

def get_access_token():
    global _cached_authentication

    # First, try to return a valid cached token
    cached_token = get_cached_token()
    if cached_token:
        return cached_token

    # If no valid cached token, proceed to fetch a new one
    print("Fetching new access token...")

    # required
    aud = os.environ["AUD"]
    scope = os.environ["SCOPE"]

    # optional
    client_id = os.environ.get("CLIENT_ID")
    host = os.environ.get("HOST")

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
    key_test = None
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

    if not key_test:
        raise RuntimeError(
            f"No .pem signing key found in '{config_path}'; cannot mint JWT client assertion."
        )

    token = jwt.encode(payload, key_test, algorithm="RS256")

    url = f"https://{host}/op/v1/token"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "scope": scope,
        "client_assertion": token
    }

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        response_json = response.json()
        # Cache the full response with request datetime
        _cached_authentication = {
            "access_token": response_json.get("access_token"),
            "expires_in": response_json.get("expires_in"),
            "request_datetime": datetime.now()
        }
        return response_json.get("access_token")
    else:
        print("Error:", response.status_code)
        return None


def get_context(url):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        context = response.json()
        context.update({
            "doel": "https://data.vlaanderen.be/ns/",
            "loc": "http://data.lblod.info/id/vestigingen/",
            "concept": "http://data.vlaanderen.be/id/concept/",
            "lblodconcept": "http://lblod.data.gift/concepts/",
            "gestructureerdeIdentificator": "generiek:gestructureerdeIdentificator",
            "bestaatUit": "https://data.vlaanderen.be/ns/organisatie#bestaatUit",
            "startdatum": "pav:createdOn",
            "contactgegeventype": "foaf:name",
            "primairContact": "schema:contactType",
            "primaireVertegenwoordiger": "org:role",
            "description": "dc:description",
            "vertegenwoordigers": "org:hasMembership",
            "lidmaatschap": "http://data.lblod.info/id/lidmaatschap/",
            "vertegenwoordigerPersoon": "org:member",
            "ere": "http://data.lblod.info/vocabularies/erediensten/",
            "adresvoorstelling": "locn:fullAddress",
            "datumLaatsteAanpassing": "pav:lastUpdateOn",
            "etag": "pav:version",
            "lastSequenceMutatiedienst": "http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/lastSequenceMutatiedienst"
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

def fetch_data_mutatiedienst(since=0):
    try:
        target_url = f"{MUTATIEDIENST_URL}?sinds={since}"
        response = requests.get(target_url)
        response.raise_for_status()
        changes_json = response.json()
        return changes_json
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
