import os

from flask import jsonify, request
from flask_executor import Executor

from lblod.pipeline import close_item, get_item, process_item, push_item_to_triplestore
from lblod.job import load_task, update_task_status, TaskNotFoundException
from lblod.harvester import get_harvest_collection_for_task, get_initial_remote_data_object
from constants import OPERATIONS, TASK_STATUSES
from helpers import logger

AUTO_RUN = os.getenv("AUTO_RUN") in ["yes", "on", "true", True, "1", 1]
DEFAULT_GRAPH = os.getenv(
    "DEFAULT_GRAPH", "http://mu.semte.ch/graphs/scraper-graph")
MU_APPLICATION_FILE_STORAGE_PATH = os.getenv(
    "MU_APPLICATION_FILE_STORAGE_PATH", "")

executor = Executor(app)

@app.route("/delta", methods=["POST"])
def delta_handler():
    executor.submit(process_delta)
    return jsonify({"message": "thanks for all the fish!"})

def process_delta():
    try:
        request_data = request.get_json()
        inserts, *_ = [changeset["inserts"]
                       for changeset in request_data if "inserts" in changeset]
        scheduled_tasks = [
            insert["subject"]["value"]
            for insert in inserts
            if insert["predicate"]["value"] == "http://www.w3.org/ns/adms#status"
            and insert["object"]["value"] == TASK_STATUSES["SCHEDULED"]
        ]
        if not scheduled_tasks:
            return jsonify({"message": "delta didn't contain download jobs, ignoring"})

        for uri in scheduled_tasks:
            try:
                task = load_task(uri)
                if task["operation"] == OPERATIONS["COLLECTING"]:
                    update_task_status(task["uri"], TASK_STATUSES["BUSY"])
                    collection = get_harvest_collection_for_task(task)
                    rdo = get_initial_remote_data_object(collection)
                    data = get_item(rdo, task)
                    item = process_item(data, rdo)
                    try:
                        push_item_to_triplestore(item)
                        close_item(collection, task)
                    except Exception as e:
                        logger.error(
                            f"Encountered exception while trying to write data to triplestore - {task['uri']}")
                        update_task_status(
                            task["uri"], TASK_STATUSES["FAILED"])
                        raise e from None
                else:
                    print("Task is not in the 'COLLECTING' state.")
            except TaskNotFoundException:
                print(f"Task not found for {uri}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
