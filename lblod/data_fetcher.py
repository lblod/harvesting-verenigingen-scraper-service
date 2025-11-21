import requests
import json
import os
from lblod.helpers import get_access_token, get_context, fetch_data_mutatiedienst
from helpers import logger
from lblod.job import update_task_status
from constants import (
    TASK_STATUSES,
    API_URL
)

def fetch_vcodes(task):
    try:
        mutaties = fetch_data_mutatiedienst(since=0)
        vcodes = sorted(set(mutatie["vCode"] for mutatie in mutaties))
        logger.info(f"Fetched {len(vcodes)} vCodes from mutatiedienst.")
        return vcodes

    except Exception as e:
        update_task_status(task["uri"], TASK_STATUSES["FAILED"])
        logger.error(f"Error in fetch_vcodes: {e}")
        raise

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
