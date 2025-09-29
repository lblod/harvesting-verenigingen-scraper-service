import os

from flask import jsonify, request
from flask_executor import Executor

from lblod.pipeline import close_item, process_task, save_json_file_in_triplestore
from lblod.file import save_json_on_disk
from lblod.job import load_task, update_task_status, TaskNotFoundException
from lblod.harvester import get_harvest_collection_for_task, get_initial_remote_data_object
from lblod.data_fetcher import fetch_vcodes
from constants import OPERATIONS, \
    TASK_STATUSES, \
    MUTATIEDIENST_SYNC_INTERVAL_SECONDS, \
    MUTATIEDIENST_SYNC_INTERVAL_ACTIVITY_WINDOW, \
    API_URL

from lblod.helpers import fetch_data_mutatiedienst
from lblod.mutatiedienst_scheduler import run_mutatiedienst_pipeline
from helpers import logger
from sudo_query import query_sudo

AUTO_RUN = os.getenv("AUTO_RUN") in ["yes", "on", "true", True, "1", 1]
DEFAULT_GRAPH = os.getenv(
    "DEFAULT_GRAPH", "http://mu.semte.ch/graphs/scraper-graph")
MU_APPLICATION_FILE_STORAGE_PATH = os.getenv(
    "MU_APPLICATION_FILE_STORAGE_PATH", "")

executor = Executor(app)

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler()

logger.info(f"Mutatiedienst sync interval (in seconds): {MUTATIEDIENST_SYNC_INTERVAL_SECONDS}")
logger.info(f"Mutatiedienst sync interval activity window of the day: {MUTATIEDIENST_SYNC_INTERVAL_ACTIVITY_WINDOW}")

def wrapped_job_mutatiedienst():
    try:
        run_mutatiedienst_pipeline()
    except Exception as err:
        # TODO: store error
        logger.error(f"An error occured during the scheduling of mutatiedienst pipeline. (i.e. before the real job starts)")
        logger.error(f"{err}")

scheduler.add_job(wrapped_job_mutatiedienst,
                  'cron',
                  second=MUTATIEDIENST_SYNC_INTERVAL_SECONDS,
                  hour=MUTATIEDIENST_SYNC_INTERVAL_ACTIVITY_WINDOW,
                  max_instances=1,
                  coalesce=True)
# Note : while running this service in development mode, you might notice that the jobs are executed twice
# It's related to the debug mode of Flask, which does not apply to the built version.)
scheduler.start()


@app.route("/delta", methods=["POST"])
def delta_handler():
    executor.submit(process_delta)
    return jsonify({"message": "thanks for all the fish!"})

def process_delta():
    try:
        request_data = request.get_json()
        inserts = [
            insert
            for changeset in request_data
            for insert in changeset["inserts"]
        ]
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

                    # Order matters here. We want the sequence before we start full sync
                    #  It doesn't matter if the sequence still evolves meanwhile
                    sequence_data = help_generate_mutatiedienst_new_sequence_object()
                    collection = get_harvest_collection_for_task(task)
                    rdo = get_initial_remote_data_object(collection)
                    vcodes = fetch_vcodes(task)

                    data = process_task(task, vcodes, API_URL, sequence_data)
                    json_file_data = save_json_on_disk(data, rdo)
                    try:
                        save_json_file_in_triplestore(json_file_data)
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


def help_generate_mutatiedienst_new_sequence_object():
    # Get the URI of the MutatiedienstStateInfo
    query_string = """
      SELECT DISTINCT ?subject WHERE {
         ?subject a <http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/MutatiedienstStateInfo>.
      }
    """
    result = query_sudo(query_string)
    if not len(result["results"]["bindings"]) == 1:
        raise Exception(f"Too many MutatiedienstStateInfo resources stored in database: {len(result['results']['bindings'])}")

    subject = result["results"]["bindings"][0]["subject"]["value"]
    mutatiedienst_changes = fetch_data_mutatiedienst();
    last_sequence = mutatiedienst_changes[-1]["sequence"] # Assumes it's a sorted list

    if not last_sequence:
        raise Exception(f"No last sequence found from mutatiedienst. Failing full harvest.")

    return { "@id": subject, "lastSequenceMutatiedienst": last_sequence }
