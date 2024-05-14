import requests
import os
import concurrent.futures
from lblod.helpers import get_access_token
import uuid

api_url = os.environ["API_URL"]


def fetch_detail_url(access_token, v_code):
    url = f"{api_url}verenigingen/{v_code}"
    correlation_id = uuid.uuid4()
    print(f"x-correlation-id: {correlation_id}")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-correlation-id": f"{correlation_id}"
        # "vr-api-key": os.environ["API_KEY"]
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        association = data.get("vereniging")
        metadata = data.get("metadata")
        association["metadata"] = metadata
        return association
    else:
        print(f"Error fetching detail URL for v_code {v_code}: {response.status_code}")
        return None


def fetch_detail_urls(all_vcodes):
    access_token = get_access_token()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(
            executor.map(fetch_detail_url, [access_token] * len(all_vcodes), all_vcodes)
        )
        while None in results:
            results.remove(None)
    return results
