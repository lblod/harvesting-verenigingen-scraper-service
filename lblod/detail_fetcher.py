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
import time
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
            etag = response.headers.get("etag")
            association = data.get("vereniging")
            metadata = data.get("metadata")

            if etag is None:
                message = f"""The association data response did not have an ETag.
                  This header is required. vCode: {v_code}, correlation_id: {correlation_id}"""
                raise Exception(message)

            if association is not None:
                association["etag"] = etag
                association["metadata"] = metadata
                logger.info(f"Successfully fetched data for vCode: {v_code}")
                return association
            else:
                message = f"No association data found for vCode: {v_code}, correlation_id: {correlation_id}"
                raise Exception(message)

        except requests.exceptions.ConnectionError as conn_err:
            logger.error(f"Connection error occurred for vCode {v_code}, correlation_id: {correlation_id}: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            logger.error(f"Timeout error occurred for vCode {v_code}, correlation_id: {correlation_id}: {timeout_err}")
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred for vCode {v_code}, correlation_id: {correlation_id}: {http_err}")

            response = http_err.response
            fail_body = try_json_from_request_response(response) or {}
            if is_removed_resource_response(fail_body):
                logger.warning(f"We've found a removed vCode {v_code}. Skipping.")
                # TODO: we need to revise the pipeline.
                #   For now we created an adhoc object so we can work with this further down the line.
                return { "type": 'RemovedResource', "vCode": v_code }
            else:
                logger.error(f"Unexpected http error: {str(fail_body)}")

            break
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Request exception occurred for vCode {v_code}, correlation_id: {correlation_id}: {req_err}")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred for vCode {v_code}, correlation_id: {correlation_id}: {e}")
            break

        logger.info(f"Retrying... ({attempt + 1}/{retry_attempts})")
        sleep_time = 5
        logger.info(f"Sleeping ${sleep_time} seconds")
        time.sleep(sleep_time)

    error_message = f"Encountered exception while trying to fetch details for vCode: {v_code}, correlation_id: {correlation_id}"
    logger.error(error_message)
    raise Exception(error_message)

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

def try_json_from_request_response(response):
    try:
        return response.json()
    except:
        return None

def is_removed_resource_response(response):
    removed_resource_response_template = {
        'type': 'urn:associationregistry.admin.api:validation',
        'title': 'Er heeft zich een fout voorgedaan!',
        'detail': 'Source: Deze vereniging werd verwijderd.',
        'status': 404
    }
    if int(response.get('status')) == removed_resource_response_template.get('status')\
       and response.get('type') == removed_resource_response_template.get('type')\
       and response.get('detail') == removed_resource_response_template.get('detail'):
        return True
    else:
        return False
