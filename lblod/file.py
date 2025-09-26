import os
import gzip
from string import Template
from datetime import datetime, timezone
from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_int, sparql_escape_datetime
from constants import FILE_STATUSES, RESOURCE_BASE, PREFIXES, DEFAULT_GRAPH, JOB_CREATOR_URI
from helpers import generate_uuid
from sudo_query import update_sudo

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
RELATIVE_STORAGE_PATH = os.environ.get("MU_APPLICATION_FILE_STORAGE_PATH", "").rstrip("/")
STORAGE_PATH = f"/share/{RELATIVE_STORAGE_PATH}"

############################################################
# TODO: keep this generic and extract into packaged module later
############################################################


def construct_insert_file_query(file, physical_file, graph=MU_APPLICATION_GRAPH):
    """
    Construct a SPARQL query for inserting a file.

    :param file: dict containing properties for file
    :param share_uri:
    :returns: string containing SPARQL query
    """
    query_template = Template("""
    $prefixes

DELETE {
    GRAPH $graph {
      $data_source_uri adms:status ?status.
      $data_source_uri dct:modified ?modified .
    }
}
INSERT  {
    GRAPH $graph {
        $physical_uri a nfo:FileDataObject ;
            mu:uuid $physical_uuid ;
            nfo:fileName $physical_name ;
            nie:dataSource $data_source_uri;
            ndo:copiedFrom $data_source_uri;
            dct:format $mimetype ;
            dct:created $created ;
            nfo:fileSize $size ;
            dbpedia:fileExtension $extension .
        $data_source_uri adms:status $new_status.
        $data_source_uri dct:modified $created.
        $data_source_uri a nfo:FileDataObject;
                         nfo:fileName $physical_name;
                         nfo:fileSize $size.
    }
}
WHERE {
    GRAPH $graph {
      OPTIONAL { $data_source_uri adms:status ?status. }
      $data_source_uri dct:modified ?modified.
    }
}
""")
    return query_template.substitute(
        prefixes=PREFIXES,
        graph=sparql_escape_uri(graph),
        name=sparql_escape_string(file["name"]),
        mimetype=sparql_escape_string(file["mimetype"]),
        created=sparql_escape_datetime(file["created"]),
        size=sparql_escape_int(file["size"]),
        extension=sparql_escape_string(file["extension"]),
        physical_uri=sparql_escape_uri(physical_file["uri"]),
        physical_uuid=sparql_escape_string(physical_file["uuid"]),
        physical_name=sparql_escape_string(physical_file["name"]),
        data_source_uri=sparql_escape_uri(file["remote_data_object"]),
        new_status=sparql_escape_uri(FILE_STATUSES["COLLECTED"])
    )


# Ported from https://github.com/mu-semtech/file-service/blob/dd42c51a7344e4f7a3f7fba2e6d40de5d7dd1972/web.rb#L228
def shared_uri_to_path(uri):
    return uri.replace('share://', '/share/')

# Ported from https://github.com/mu-semtech/file-service/blob/dd42c51a7344e4f7a3f7fba2e6d40de5d7dd1972/web.rb#L232
def file_to_shared_uri(file_name):
    if RELATIVE_STORAGE_PATH:
        return f"share://{RELATIVE_STORAGE_PATH}/{file_name}"
    else:
        return f"share://{file_name}"

def save_json_on_disk(content, rdo = None):
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)

    _uuid = generate_uuid()
    json_file_name = f"{_uuid}.json.gz"
    json_file_path = os.path.join(STORAGE_PATH, json_file_name)

    with gzip.open(json_file_path, "wt", encoding="utf-8") as f:
        f.write(content)

    size = os.stat(json_file_path).st_size
    file_created = datetime.now(timezone.utc)

    adapter = {}
    adapter["uuid"] = _uuid
    adapter["size"] = size
    adapter["file_created"] = file_created
    adapter["extension"] = "json.gz"
    adapter["format"] = "application/gzip"
    adapter["physical_file_name"] = json_file_name
    adapter["physical_file_path"] = json_file_path
    if(rdo):
        adapter["rdo"] = rdo
    return adapter

def save_json_file_in_triplestore(file_metadata):
    """
    Saves file metadata to a triplestore based on the provided file_metadata dictionary.

    :param file_metadata: A dictionary containing file metadata, typically returned
                          by a function like save_json_on_disk.
    """
    physical_file_uuid = file_metadata["uuid"]
    physical_file_uri = file_to_shared_uri(file_metadata["physical_file_name"])
    physical_file_name = file_metadata["physical_file_name"]
    file_size = file_metadata["size"]
    current_time = datetime.now(timezone.utc)

    logical_file_uuid = generate_uuid()
    logical_file_uri = RESOURCE_BASE + logical_file_uuid

    file_format = file_metadata["format"]
    file_extension = file_metadata["extension"]
    logical_file_name = f"{logical_file_uuid}.{file_extension}"

    query = f"""
    {PREFIXES}
    INSERT DATA {{
      GRAPH {sparql_escape_uri(DEFAULT_GRAPH)} {{
          {sparql_escape_uri(physical_file_uri)} a nfo:FileDataObject;
                                           nie:dataSource {sparql_escape_uri(logical_file_uri)} ;
                                           mu:uuid {sparql_escape_string(physical_file_uuid)};
                                           nfo:fileName {sparql_escape_string(physical_file_name)} ;
                                           dct:creator {sparql_escape_uri(JOB_CREATOR_URI)};
                                           dct:created {sparql_escape_datetime(current_time)};
                                           dct:modified {sparql_escape_datetime(current_time)};
                                           dct:format {sparql_escape_string(file_format)};
                                           nfo:fileSize {sparql_escape_int(file_size)};
                                           dbpedia:fileExtension {sparql_escape_string(file_extension)}.
          {sparql_escape_uri(logical_file_uri)} a nfo:FileDataObject;
                                           mu:uuid {sparql_escape_string(logical_file_uuid)};
                                           nfo:fileName {sparql_escape_string(logical_file_name)} ;
                                           dct:creator {sparql_escape_uri(JOB_CREATOR_URI)};
                                           dct:created {sparql_escape_datetime(current_time)};
                                           dct:modified {sparql_escape_datetime(current_time)};
                                           dct:format {sparql_escape_string(file_format)};
                                           nfo:fileSize {sparql_escape_int(file_size)};
                                           dbpedia:fileExtension {sparql_escape_string(file_extension)} .
      }}
    }}
    """
    update_sudo(query)
    return logical_file_uri
