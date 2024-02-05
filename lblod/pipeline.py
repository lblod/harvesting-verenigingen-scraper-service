import json
import datetime
import os
import uuid
import gzip
from rdflib import Graph

import requests
from constants import DEFAULT_GRAPH, TASK_STATUSES

from lblod.file import STORAGE_PATH, construct_insert_file_query
from lblod.harvester import collection_has_collected_files, create_results_container
from lblod.job import update_task_status
from sudo_query import update_sudo
from helpers import logger


def get_item(rdo, task):
    # Get all possible urls
    json_urls = {"urls": ["https://verenigingen.oscart-dev.s.redhost.be/json-ld?page=1",
                          "https://verenigingen.oscart-dev.s.redhost.be/json-ld?page=2",
                          "https://verenigingen.oscart-dev.s.redhost.be/json-ld?page=3"]}
    return json.dumps(json_urls)


def process_item(content, rdo):
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)

    _uuid = str(uuid.uuid4())
    json_file_name = f"{_uuid}.json.gz"
    json_file_path = os.path.join(STORAGE_PATH, json_file_name)

    with gzip.open(json_file_path, "wt", encoding='utf-8') as f:
        f.write(content)

    size = os.stat(json_file_path).st_size
    file_created = datetime.datetime.now()

    adapter = {}
    adapter["rdo"] = rdo
    adapter["uuid"] = _uuid
    adapter["size"] = size
    adapter["file_created"] = file_created
    adapter["extension"] = "html"
    adapter["format"] = "application/gzip"
    adapter["physical_file_name"] = json_file_name
    adapter["physical_file_path"] = json_file_path
    return adapter


def push_item_to_triplestore(item):
    virtual_resource_uuid = str(uuid.uuid4())
    virtual_resource_uri = f"http://data.lblod.info/files/{virtual_resource_uuid}"
    virtual_resource_name = f"{virtual_resource_uuid}.{item['extension']}"
    file = {
        "uri": virtual_resource_uri,
        "uuid": virtual_resource_uuid,
        "name": virtual_resource_name,
        "mimetype": item["format"],
        "created": item["file_created"],
        "modified": item["file_created"],  # currently unused
        "size": item["size"],
        "extension": item["extension"],
        "remote_data_object": item["rdo"]["uri"]
    }
    physical_resource_uri = item["physical_file_path"].replace(
        "/share/", "share://")
    physical_file = {
        "uuid": item["uuid"],
        "uri": physical_resource_uri,
        "name": item["physical_file_name"]
    }
    ins_file_q_string = construct_insert_file_query(file,
                                                    physical_file,
                                                    DEFAULT_GRAPH)
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
