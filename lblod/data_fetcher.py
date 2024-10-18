import requests
import json
import os
import concurrent.futures
from lblod.helpers import get_access_token, get_context
import uuid

api_url = os.environ["API_URL"]


def fetch_data(access_token, postcode, limit=100):
    correlation_id = uuid.uuid4()
    print(f"x-correlation-id: {correlation_id}")
    url = f"{api_url}verenigingen/zoeken?q=locaties.postcode:{postcode}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-correlation-id": f"{correlation_id}"
    }
    offset = 0
    v_codes = []
    while True:
        pagination_params = f"&offset={offset}&limit={limit}"
        paginated_url = url + pagination_params
        print("Paginated URL:", paginated_url)
        try:
            response = requests.get(paginated_url, headers=headers, timeout=30)
            # Raise an exception for non-2xx statuses
            response.raise_for_status()
            data = response.json()
            verenigingen = data.get("verenigingen", [])
            v_codes.extend(
                [vereniging.get("vCode") for vereniging in verenigingen]
            )

            if data["metadata"]["pagination"]["totalCount"] > (offset + limit):
                offset += limit
                print("Offset:", offset)
            else:
                break
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Request failed for {paginated_url}: {e}")
    return v_codes


def fetch_vcodes():
    current_directory = os.path.dirname(os.path.realpath(__file__))
    json_file_path = os.path.join(current_directory, "postal_codes.json")
    access_token = get_access_token()

    if access_token:
        print("Access token:", "success")
        with open(json_file_path, "r") as file:
            postal_codes_data = json.load(file)
            belgium_postal_codes = (
                postal_codes_data["postal_codes_brussels"]
                + postal_codes_data["postal_codes_flanders"]
            )

        all_vcodes = []

        def fetch_vcodes_single(postcode):
            print("Postcode:", postcode)
            return fetch_data(access_token, postcode, 160)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(fetch_vcodes_single, belgium_postal_codes)
            for v_codes in results:
                all_vcodes.extend(v_codes)

        return all_vcodes


def fetch_context():
    context_url = "https://publiek.verenigingen.staging-vlaanderen.be/v1/contexten/beheer/detail-vereniging-context.json"
    return get_context(context_url)
