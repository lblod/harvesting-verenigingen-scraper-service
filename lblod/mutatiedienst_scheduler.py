import os
from helpers import logger
from lblod.helpers import fetch_data_mutatiedienst
from sudo_query import query_sudo
from lblod.job import create_job, create_task, load_task, update_task_status, any_other_harvest_jobs_running
from lblod.detail_fetcher import fetch_detail_urls
from constants import OPERATIONS, MUTATIEDIENST_URL, TASK_STATUSES
from lblod.pipeline import process_task
from lblod.harvester import create_results_container
from lblod.file import save_json_on_disk, save_json_file_in_triplestore

def fetch_last_successful_sequence_number():
    query_string = """
      SELECT DISTINCT ?subject ?since WHERE {
         ?subject a <http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/MutatiedienstStateInfo>;
           <http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/lastSequenceMutatiedienst> ?since.
      }
    """
    result = query_sudo(query_string)
    if len(result["results"]["bindings"]) == 1:
        return {
            "since": result["results"]["bindings"][0]["since"]["value"],
            "subject": result["results"]["bindings"][0]["subject"]["value"]
        }
    else:
        raise Exception(f"Unexpected amount of sequence numbers stored in database: {len(result['results']['bindings'])}")

def run_mutatiedienst_pipeline():
    if(any_other_harvest_jobs_running()):
        logger.warning(f"Other jobs are running that might affect the mutatiedienst job. Skipping...")
        return

    sequenceData = fetch_last_successful_sequence_number()

    if not sequenceData:
        logger.info(f"""
          No sequence number was found.
          This means an initial full sync hasn't started yet.
          Skipping iteration.
        """)
        return

    mutatiedienst_changes = fetch_data_mutatiedienst(sequenceData["since"])

    # Assumes there is one job at the time!
    if len(mutatiedienst_changes) == 0:
        logger.info(f"No changes found since: {sequenceData['since']}. Skipping")
        return

    last_sequence = mutatiedienst_changes[-1]["sequence"] # Assumes it's a sorted list

    # TODO: assume this is a set, and mutatiedienst does the hard work for you in folding themselves
    vCodes = { item["vCode"] for item in mutatiedienst_changes }

    job_uri = create_job(OPERATIONS["INCREMENTAL_COLLECTING"])
    task_uri = create_task(job_uri, OPERATIONS["INCREMENTAL_COLLECTING_TASK_OPERATION"])

    try:
        task = load_task(task_uri)
        json_data = process_task(task, vCodes, MUTATIEDIENST_URL,
                                 { "@id": sequenceData["subject"],
                                   "lastSequenceMutatiedienst": last_sequence
                                  })
        json_file_data = save_json_on_disk(json_data)
        json_file_uri = save_json_file_in_triplestore(json_file_data)
        data_container_uri = create_results_container(task_uri, logical_json_file_uri = json_file_uri)

        update_task_status(task["uri"], TASK_STATUSES["SUCCESS"])

    except Exception as err:
        # TODO: store error
        logger.error(f"An error occured during the execution of mutatiedienst pipeline.")
        logger.error(f"TASK: {task_uri}")
        logger.error(f"ERROR: {err}")
        update_task_status(task_uri, TASK_STATUSES["FAILED"])
        raise err
