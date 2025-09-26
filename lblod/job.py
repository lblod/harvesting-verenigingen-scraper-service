import os
from datetime import datetime, timezone
from string import Template

from helpers import generate_uuid
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string, sparql_escape_int
from sudo_query import auth_update_sudo, update_sudo, query_sudo

from constants import PREFIXES, RESOURCE_BASE, JOB_TYPE, TASK_TYPE, TASK_STATUSES, DEFAULT_GRAPH, JOB_CREATOR_URI, OPERATIONS


############################################################
# TODO: keep this generic and extract into packaged module later
############################################################

class TaskNotFoundException(Exception):
    "Raised when task is not found"
    pass

def load_task(subject, graph = DEFAULT_GRAPH):
    query_template = Template("""
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  SELECT DISTINCT ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
      GRAPH $graph {
        $subject a task:Task .
        $subject dct:isPartOf ?job;
                      mu:uuid ?id;
                      dct:created ?created;
                      dct:modified ?modified;
                      adms:status ?status;
                      task:index ?index;
                      task:operation ?operation.
        OPTIONAL { $subject task:error ?error. }
      }
    }

    """)

    query_string = query_template.substitute(
        graph = sparql_escape_uri(graph),
        subject = sparql_escape_uri(subject)
    )

    results = query_sudo(query_string)
    bindings = results["results"]["bindings"]
    if len(bindings) == 1:
        item = bindings[0]
        id = item['id']['value']
        job = item['job']['value']
        status = item['status']['value']
        index = item['index']['value']
        operation = item['operation']['value']
        error = item.get('error', {}).get('value', None)
        return {
            'id': id,
            'job': job,
            'status': status,
            'operation': operation,
            'index': index,
            'error': error,
            'uri': subject
        }
    elif len(bindings) == 0:
        raise TaskNotFoundException()
    else:
        raise Exception(f"Unexpected result loading task: {results}")

def update_task_status (task, status, graph=DEFAULT_GRAPH):
    query_template = Template("""
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    DELETE {
      GRAPH $graph {
        $subject adms:status ?status .
        $subject dct:modified ?modified.
      }
    }
    INSERT {
      GRAPH $graph {
        $subject adms:status $status.
        $subject dct:modified $modified.
      }
    }
    WHERE {
      GRAPH $graph {
        $subject a task:Task.
        $subject adms:status ?status .
        OPTIONAL { $subject dct:modified ?modified. }
      }
    }
    """)
    time = datetime.now(timezone.utc)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        subject=sparql_escape_uri(task),
        modified=sparql_escape_datetime(datetime.now(timezone.utc)),
        status=sparql_escape_uri(status)
    )
    update_sudo(query_string)

def create_job(job_operation_uri):
    """
    Creates a new job in the specified graph.
    """
    job_id = str(generate_uuid())
    job_uri = RESOURCE_BASE + job_id
    created = datetime.now(timezone.utc)

    create_job_query = f"""
    {PREFIXES}
    INSERT DATA {{
      GRAPH {sparql_escape_uri(DEFAULT_GRAPH)}{{
        {sparql_escape_uri(job_uri)} a {sparql_escape_uri(JOB_TYPE)};
                                      mu:uuid {sparql_escape_string(job_id)};
                                      dct:creator {sparql_escape_uri(JOB_CREATOR_URI)};
                                      adms:status {sparql_escape_uri(TASK_STATUSES["BUSY"])};
                                      dct:created {sparql_escape_datetime(created)};
                                      dct:modified {sparql_escape_datetime(created)};
                                      task:operation {sparql_escape_uri(job_operation_uri)}.
      }}
    }}
    """
    update_sudo(create_job_query)

    return job_uri

def create_task(job_uri, task_operation_uri, task_index="0"):
    """
    Schedules a new task for a given job.
    """
    task_id = str(generate_uuid())
    task_uri = RESOURCE_BASE + task_id
    created = datetime.now(timezone.utc)

    create_task_query = f"""
    {PREFIXES}
    INSERT DATA {{
      GRAPH {sparql_escape_uri(DEFAULT_GRAPH)} {{
          {sparql_escape_uri(task_uri)} a {sparql_escape_uri(TASK_TYPE)};
                                       mu:uuid {sparql_escape_string(task_id)};
                                       adms:status {sparql_escape_uri(TASK_STATUSES["BUSY"])};
                                       dct:created {sparql_escape_datetime(created)};
                                       dct:modified {sparql_escape_datetime(created)};
                                       task:operation {sparql_escape_uri(task_operation_uri)};
                                       task:index {sparql_escape_string(task_index)};
                                       dct:isPartOf {sparql_escape_uri(job_uri)}.
      }}
    }}"""

    update_sudo(create_task_query)

    return task_uri


def any_other_harvest_jobs_running():
    query_string = f"""
      {PREFIXES}
      ASK {{
        VALUES ?operation {{
         {sparql_escape_uri(OPERATIONS['FULL_HARVEST_JOB'])}
         {sparql_escape_uri(OPERATIONS['INCREMENTAL_COLLECTING'])}
        }}
        VALUES ?status {{
         {sparql_escape_uri(TASK_STATUSES['BUSY'])}
         {sparql_escape_uri(TASK_STATUSES['SCHEDULED'])}
        }}
        ?s a cogs:Job;
          task:operation ?operation;
          adms:status ?status .
      }}
    """
    result = query_sudo(query_string)
    return result["boolean"]

