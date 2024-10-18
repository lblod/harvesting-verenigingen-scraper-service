import requests
import os
import concurrent.futures
from lblod.helpers import get_access_token
import uuid

api_url = os.environ["API_URL"]


def fetch_detail_url(access_token, v_code):
    url = f"{api_url}verenigingen/{v_code}"
    correlation_id = uuid.uuid4()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-correlation-id": f"{correlation_id}"
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for non-2xx status
        data = response.json()
        association = data.get("vereniging")
        metadata = data.get("metadata")
        association["metadata"] = metadata
        print(f"vCode: {v_code}")
        return association
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
        raise RuntimeError(f"Request failed for {url}: {e}")


def fetch_detail_urls(all_vcodes):
    access_token = get_access_token()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(
            executor.map(fetch_detail_url, [access_token] * len(all_vcodes), all_vcodes)
        )
        while None in results:
            results.remove(None)
    return results
