from string import Template

from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_datetime
from helpers import logger, generate_uuid
from sudo_query import auth_update_sudo, update_sudo, query_sudo
import uuid
from datetime import datetime, timezone
import re
from urllib.parse import urldefrag

from constants import DEFAULT_GRAPH, RESOURCE_BASE, FILE_STATUSES, PREFIXES

def ensure_remote_data_object(collection, url):
    rdo = get_remote_data_object(collection, url)
    if rdo:
        return rdo
    else:
        return create_remote_data_object(collection, url)

def cleanUrl(url):
    """
    Workaround to avoid extracting the same url multiple times because a `jsessionid`
    is set in the url. This is only relevant for urls using a Java backend.
    todo check in the future if that's still the case, otherwise this could be completely removed.
    Not that we also cleanup the hash, e.g http://foo.com#blabla, this shouldn't affect
    extraction. We keep the other query parameters that are necessary for extraction.
    """
    url = urldefrag(url.strip()).url # remove eventual hash
    return re.sub(";jsessionid=[a-zA-Z;0-9]*", "", url)

def create_remote_data_object(collection, url):
    query_template = Template("""

    $prefixes

    INSERT DATA {
      GRAPH $graph {
        $collection dct:hasPart $uri.
        $uri a nfo:RemoteDataObject .
        $uri mu:uuid $uuid;
             nie:url $url;
             dct:created $created;
             dct:creator <http://lblod.data.gift/services/scraper>;
             dct:modified $modified;
             adms:status $status.
      }
    }
""")
    uuid = generate_uuid()
    uri = RESOURCE_BASE.rstrip("/") + f"/remote-data-objects/{uuid}"
    created = datetime.now(timezone.utc)
    q_string = query_template.substitute(
        prefixes = PREFIXES,
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        uri = sparql_escape_uri(uri),
        uuid = sparql_escape_string(uuid),
        url = sparql_escape_uri(cleanUrl(url)),
        status = sparql_escape_uri(FILE_STATUSES['READY']),
        created = sparql_escape_datetime(created),
        modified = sparql_escape_datetime(created),
        collection = sparql_escape_uri(collection)

    )
    update_sudo(q_string)
    return {
        'uuid': uuid,
        'url': url,
        'uri': uri,
        'status': FILE_STATUSES['READY']
    }

def create_results_container(task_uri, collection_uri = None, logical_json_file_uri = None):
    uuid = generate_uuid()
    uri = RESOURCE_BASE.rstrip("/") + f"/data-containers/{uuid}"

    if collection_uri:
      query_template = Template("""

      $prefixes

      INSERT {
        GRAPH $graph {
          $task task:resultsContainer $result_container.
          $result_container a nfo:DataContainer;
                            mu:uuid $uuid;
                            task:hasFile ?rdo.
        }
      }
      WHERE {
        GRAPH $graph {
          $task a task:Task;
                task:inputContainer ?container.
          ?container task:hasHarvestingCollection $collection.
          $collection dct:hasPart ?rdo.
          ?rdo adms:status $status_collected.
        }
      }
      """)
      query_s = query_template.substitute(
          prefixes = PREFIXES,
          graph = sparql_escape_uri(DEFAULT_GRAPH),
          result_container = sparql_escape_uri(uri),
          uuid = sparql_escape_string(uuid),
          status_collected = sparql_escape_uri(FILE_STATUSES['COLLECTED']),
          collection = sparql_escape_uri(collection_uri),
          task = sparql_escape_uri(task_uri)
      )
    elif logical_json_file_uri:
      query_template = Template("""

      $prefixes

      INSERT DATA {
        GRAPH $graph {
          $task task:resultsContainer $result_container.
          $result_container a nfo:DataContainer;
                            mu:uuid $uuid;
                            task:hasFile $logical_json_file.
        }
      }
      """)
      query_s = query_template.substitute(
          prefixes = PREFIXES,
          graph = sparql_escape_uri(DEFAULT_GRAPH),
          result_container = sparql_escape_uri(uri),
          uuid = sparql_escape_string(uuid),
          task = sparql_escape_uri(task_uri),
          logical_json_file = sparql_escape_uri(logical_json_file_uri)
      )

    else:
        raise Exception("Unexpected number of arguments for create_results_container")

    update_sudo(query_s)
    return uri

"""
get remote data object in a harvesting collection that matches remote url. Expects 1 RDO
"""
def get_remote_data_object(collection_uri, remote_url):
    query_template = Template("""
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

    SELECT DISTINCT ?dataObject ?uuid ?status
    WHERE {
      GRAPH $graph {
        $collection dct:hasPart ?dataObject.
        ?dataObject a nfo:RemoteDataObject;
             mu:uuid ?uuid;
             nie:url $url.
        OPTIONAL { ?dataObject adms:status ?status.}
      }
    }
""")
    query_string = query_template.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        collection = sparql_escape_uri(collection_uri),
        url = sparql_escape_uri(cleanUrl(remote_url))
    )
    results = query_sudo(query_string)
    bindings = results["results"]["bindings"]
    if len(bindings) == 1:
        item = bindings[0]
        uuid = item['uuid']['value']
        uri = item['dataObject']['value']
        status = item.get('status', {}).get('value', None)
        return {
            'uuid': uuid,
            'url': remote_url,
            'uri': uri,
            'status': status
        }
    elif len(bindings) == 0:
        return None
    else:
        raise Exception(f"Unexpected result {results}")


"""
get remote data object in a harvesting collection. Expects 1 RDO
"""
def get_initial_remote_data_object(collection_uri):
    query_template = Template("""
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

    SELECT DISTINCT ?dataObject ?url ?uuid ?status
    WHERE {
      GRAPH $graph {
        $collection dct:hasPart ?dataObject.
        ?dataObject a nfo:RemoteDataObject;
             mu:uuid ?uuid;
             nie:url ?url.
      }
    }
""")
    query_string = query_template.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        collection = sparql_escape_uri(collection_uri)
    )
    results = query_sudo(query_string)
    bindings = results["results"]["bindings"]
    if len(bindings) == 1:
        item = bindings[0]
        uuid = item['uuid']['value']
        url = item['url']['value']
        uri = item['dataObject']['value']
        return {
            'uuid': uuid,
            'url': url,
            'uri': uri
        }
    else:
        raise Exception(f"Unexpected result {results}")

def get_harvest_collection_for_task(task, graph = DEFAULT_GRAPH):
    task_uri = task["uri"]
    query_template = Template("""
    PREFIX tasks: <http://redpencil.data.gift/vocabularies/tasks/>
    SELECT ?collection
    WHERE {
      GRAPH $graph  {
        $task tasks:inputContainer ?inputContainer.
        ?inputContainer tasks:hasHarvestingCollection ?collection.
      }
    }
    """)
    query_s = query_template.substitute(
        graph = sparql_escape_uri(graph),
        task = sparql_escape_uri(task_uri)
    )
    results = query_sudo(query_s)
    bindings = results["results"]["bindings"]
    if (len(bindings) == 1):
        return bindings[0]["collection"]["value"]
    else:
        raise Exception(f"Unexpected result {results}")


def collection_has_collected_files(collection):
    query_template = Template("""
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    ASK { GRAPH $graph {
      $collection dct:hasPart ?remoteDataObject.
      ?remoteDataObject adms:status $status
    }}
    """)
    query_s = query_template.substitute(
        graph = sparql_escape_uri(DEFAULT_GRAPH),
        collection = sparql_escape_uri(collection),
        status = sparql_escape_uri(FILE_STATUSES["COLLECTED"])
    )
    result = query_sudo(query_s)
    return result["boolean"]
