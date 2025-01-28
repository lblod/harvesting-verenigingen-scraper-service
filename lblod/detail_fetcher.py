import requests
import os
import concurrent.futures
from lblod.helpers import get_access_token
import uuid
from helpers import logger
from lblod.job import update_task_status
from constants import TASK_STATUSES
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry # type: ignore

api_url = os.environ["API_URL"]


def fetch_detail_url(access_token, v_code, task):
    url = f"{api_url}verenigingen/{v_code}"
    correlation_id = uuid.uuid4()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-correlation-id": str(correlation_id)
    }
    retry_attempts = 5

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()
            association = data.get("vereniging")
            metadata = data.get("metadata")

            if association is not None:
                association["metadata"] = metadata
                logger.info(f"Successfully fetched data for vCode: {v_code}")
                return association
            else:
                logger.error(f"No association data found for vCode: {v_code}")
                return None

        except requests.exceptions.ConnectionError as conn_err:
            logger.error(f"Connection error occurred for vCode {v_code}: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            logger.error(f"Timeout error occurred for vCode {v_code}: {timeout_err}")
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred for vCode {v_code}: {http_err}")
            break
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Request exception occurred for vCode {v_code}: {req_err}")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred for vCode {v_code}: {e}")
            break

        logger.info(f"Retrying... ({attempt + 1}/{retry_attempts})")

    logger.error(f"Encountered exception while trying to write data to triplestore - {task['uri']}")
    update_task_status(task["uri"], TASK_STATUSES["FAILED"])
    return None


def fetch_detail_urls(all_vcodes, task):
    try:
        access_token = get_access_token()
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            results = list(
                executor.map(lambda v_code: fetch_detail_url(access_token, v_code, task), all_vcodes)
            )
    except Exception as e:
        logger.error(f"Unexpected error while fetching association detail URLs: {e}")
        update_task_status(task["uri"], TASK_STATUSES["FAILED"])
        return None

    return results