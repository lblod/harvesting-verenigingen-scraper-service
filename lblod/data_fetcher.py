import requests
import json
import os
import concurrent.futures
from lblod.helpers import get_access_token, get_context
import uuid
from helpers import logger
from lblod.job import update_task_status
from constants import TASK_STATUSES

api_url = os.environ["API_URL"]


def fetch_data(access_token, postcode, task, limit=100):
    correlation_id = uuid.uuid4()
    logger.info(f"x-correlation-id: {correlation_id}")
    url = f"{api_url}verenigingen/zoeken?q=locaties.postcode:{postcode}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-correlation-id": str(correlation_id)
    }
    offset = 0
    v_codes = []

    while True:
        pagination_params = f"&offset={offset}&limit={limit}"
        paginated_url = url + pagination_params
        logger.info(f"Paginated URL: {paginated_url}")

        retry_attempts = 5

        for attempt in range(retry_attempts):

            try:
                response = requests.get(
                    paginated_url,
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()

                data = response.json()
                v_codes.extend(
                    [vereniging.get("vCode") for vereniging in data.get("verenigingen", [])]
                )

                if data["metadata"]["pagination"]["totalCount"] > (offset + limit):
                    offset += limit
                    logger.info(f"Offset: {offset}")
                else:
                    break # Break out of retry loop if no more data to fetch

            except requests.exceptions.HTTPError as http_err:
                logger.error(f"HTTP error occurred for postcode {postcode}: {http_err}")
            except requests.exceptions.ConnectionError as conn_err:
                logger.error(f"Connection error occurred for postcode {postcode}: {conn_err}")
            except requests.exceptions.Timeout as timeout_err:
                logger.error(f"Timeout error occurred for postcode {postcode}: {timeout_err}")
            except requests.exceptions.RequestException as req_err:
                logger.error(f"Request exception occurred for postcode {postcode}: {req_err}")
            except Exception as e:
                logger.error(f"An unexpected error occurred for postcode {postcode}: {e}")
            logger.info(f"Retrying... ({attempt + 1}/{retry_attempts})")
        if attempt == retry_attempts - 1:
            logger.error(f"Encountered exception while trying to fetch data - {task['uri']}")
            logger.error(f"Failed to fetch data for postcode: {postcode} after {retry_attempts} attempts.")
            update_task_status(task["uri"], TASK_STATUSES["FAILED"])
            raise requests.exceptions.RetryError(f"Failed to fetch data for postcode: {postcode} after {retry_attempts} attempts.")
        else:
            logger.info(f"Data fetched successfully for postcode: {postcode}")
            return v_codes



def fetch_vcodes(task):
    current_directory = os.path.dirname(os.path.realpath(__file__))
    json_file_path = os.path.join(current_directory, "postal_codes.json")
    access_token = get_access_token()

    if access_token:
        logger.info("Access token: success")

        with open(json_file_path, "r") as file:
            postal_codes_data = json.load(file)
            belgium_postal_codes = (
                postal_codes_data["postal_codes_brussels"]
                + postal_codes_data["postal_codes_flanders"]
            )

        all_vcodes = []

        def fetch_vcodes_single(postcode):
            logger.info(f"Postcode: {postcode}")
            return fetch_data(access_token, postcode, task, 160)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                results = executor.map(fetch_vcodes_single, belgium_postal_codes)
                for v_codes in results:
                    all_vcodes.extend(v_codes)
                return all_vcodes
        except Exception as e:
            logger.error(f"Unexpected error while fetching vcodes: {e}")
            update_task_status(task["uri"], TASK_STATUSES["FAILED"])
            raise e
    else:
        logger.error("Failed to obtain access token")
        update_task_status(task["uri"], TASK_STATUSES["FAILED"])
        return None


def fetch_context(task):
    context_url = "https://publiek.verenigingen.staging-vlaanderen.be/v1/contexten/beheer/detail-vereniging-context.json"

    try:
        try:
            context = get_context(context_url)
            if context is not None:
                logger.info("Context successfully fetched and updated.")
                return context
            else:
                update_task_status(task["uri"], TASK_STATUSES["FAILED"])
                return None
        except Exception as e:
            logger.error(f"Error occurred while fetching context: {e}")
            update_task_status(task[uri], TASK_STATUSES["FAILED"])
            return None

    except Exception as e:
        logger.error(f"Unexpected error occurred while fetching context: {e}")
        update_task_status(task[uri], TASK_STATUSES["FAILED"])
        return None
