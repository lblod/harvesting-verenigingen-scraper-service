import json
import datetime
import os
import uuid
import gzip

from constants import DEFAULT_GRAPH, TASK_STATUSES, MUTATIEDIENST_URL

from lblod.file import STORAGE_PATH, construct_insert_file_query
from lblod.harvester import collection_has_collected_files, create_results_container
from lblod.job import update_task_status
from sudo_query import update_sudo
from helpers import logger
from lblod.data_fetcher import fetch_vcodes, fetch_context
from lblod.detail_fetcher import fetch_detail_urls
from lblod.transform_data import transform_data
import json

def process_task(task, vcodes, api_url = API_URL, last_sequence = None):
    try:
        if not vcodes:
            raise ValueError("No vCodes found for the given postal codes.")
        data = fetch_detail_urls(vcodes, task)
        if not data:
            raise ValueError("No data fetched for the provided vCodes.")
        context = fetch_context(task)
        if not context:
            raise ValueError("No context fetched for the task.")
        transformed_data = transform_data(data)
        if not transformed_data:
            raise ValueError("No transformed data available.")
        all_data = {
            "@context": context,
            "verenigingen": transformed_data,
            "url": api_url,
        }
        if last_sequence:
            all_data["sequence"] = last_sequence
        return json.dumps(all_data)
    except KeyError as e:
        logger.error(f"Missing environment variable: {e}")
        raise
    except Exception as e:
        update_task_status(task["uri"], TASK_STATUSES["FAILED"])
        logger.error(f"Error in process_task: {e}")
        raise

def save_json_file_in_triplestore(physical_file_data):
    virtual_resource_uuid = str(uuid.uuid4())
    virtual_resource_uri = f"http://data.lblod.info/files/{virtual_resource_uuid}"
    virtual_resource_name = f"{virtual_resource_uuid}.{physical_file_data['extension']}"
    file = {
        "uri": virtual_resource_uri,
        "uuid": virtual_resource_uuid,
        "name": virtual_resource_name,
        "mimetype": physical_file_data["format"],
        "created": physical_file_data["file_created"],
        "modified": physical_file_data["file_created"],  # currently unused
        "size": physical_file_data["size"],
        "extension": physical_file_data["extension"],
        "remote_data_object": physical_file_data["rdo"]["uri"],
    }
    physical_resource_uri = physical_file_data["physical_file_path"].replace("/share/", "share://")
    physical_file = {
        "uuid": physical_file_data["uuid"],
        "uri": physical_resource_uri,
        "name": physical_file_data["physical_file_name"],
    }
    ins_file_q_string = construct_insert_file_query(file, physical_file, DEFAULT_GRAPH)
    update_sudo(ins_file_q_string)


def close_item(collection, task):
    try:
        if collection_has_collected_files(collection):
            create_results_container(task["uri"], collection)
            update_task_status(task["uri"], TASK_STATUSES["SUCCESS"])
        else:
            logger.error("no files collected closed without collecting files")
            update_task_status(task["uri"], TASK_STATUSES["FAILED"])
    except Exception as e:
        logger.error(e)
        update_task_status(task["uri"], TASK_STATUSES["FAILED"])
