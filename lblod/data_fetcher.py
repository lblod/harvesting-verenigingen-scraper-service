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


def fetch_data(access_token, postcode, limit=100):
    correlation_id = uuid.uuid4()
    logger.info(f"x-correlation-id: {correlation_id}")
    url = f"{api_url}verenigingen/zoeken?q=locaties.postcode:{postcode}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-correlation-id": str(correlation_id),
    }
    offset = 0
    v_codes = []
    max_retries = 5

    while True:
        pagination_params = f"&offset={offset}&limit={limit}"
        paginated_url = url + pagination_params
        logger.info(f"Paginated URL: {paginated_url}")

        for attempt in range(max_retries):
            try:
                response = requests.get(paginated_url, headers=headers, timeout=30)
                response.raise_for_status()

                data = response.json()
                v_codes.extend(
                    [
                        vereniging.get("vCode")
                        for vereniging in data.get("verenigingen", [])
                    ]
                )

                if data["metadata"]["pagination"]["totalCount"] > (offset + limit):
                    offset += limit
                    logger.info(f"Offset: {offset}")
                else:
                    return v_codes  # All data fetched, exit function
                break  # Success, break out of retry loop

            except requests.exceptions.Timeout as timeout_err:
                logger.error(
                    f"Timeout error occurred for postcode {postcode} (attempt {attempt+1}/{max_retries}), correlation_id: {correlation_id}: {timeout_err}"
                )
                if attempt == max_retries - 1:
                    logger.error(
                        f"Encountered exception while trying to fetch associations codes, correlation_id: {correlation_id}"
                    )
                    raise
                logger.info("Retrying due to timeout...")
            except (
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.RequestException,
            ) as req_err:
                logger.error(
                    f"Request error occurred for postcode {postcode}, correlation_id: {correlation_id}: {req_err}"
                )
                raise
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred for postcode {postcode}, correlation_id: {correlation_id}: {e}"
                )
                raise


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
            return fetch_data(access_token, postcode, 160)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                results = executor.map(fetch_vcodes_single, belgium_postal_codes)
                for v_codes in results:
                    all_vcodes.extend(v_codes)
                return all_vcodes
        except Exception as e:
            logger.error(f"Unexpected error while fetching vcodes: {e}")
            update_task_status(task["uri"], TASK_STATUSES["FAILED"])
            return None
    else:
        logger.error("Failed to obtain access token")
        update_task_status(task["uri"], TASK_STATUSES["FAILED"])
        return None


def fetch_context(task):
    # TODO: bring to environment variable.
    context_url = "https://publiek.verenigingen.staging-vlaanderen.be/v1/contexten/beheer/detail-vereniging-context.json"

    try:
        context = get_context(context_url)
        if context is not None:
            logger.info("Context successfully fetched and updated.")
            return context
        else:
            logger.error("Failed to fetch context.")
            update_task_status(task, TASK_STATUSES["FAILED"])
            return None

    except Exception as e:
        logger.error(f"Unexpected error occurred while fetching context: {e}")
        update_task_status(task, TASK_STATUSES["FAILED"])
        return None
