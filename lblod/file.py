import os
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_int, sparql_escape_datetime
from constants import FILE_STATUSES
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
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dbpedia: <http://dbpedia.org/ontology/>
PREFIX ndo: <http://oscaf.sourceforge.net/ndo.html#>
PREFIX    adms: <http://www.w3.org/ns/adms#>

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
