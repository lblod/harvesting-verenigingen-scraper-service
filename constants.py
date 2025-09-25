import os

RESOURCE_BASE = "http://data.lblod.info/id/"

JOB_TYPE = "http://vocab.deri.ie/cogs#Job"
TASK_TYPE = "http://redpencil.data.gift/vocabularies/tasks/Task"

DEFAULT_GRAPH = os.environ.get('DEFAULT_GRAPH', None) or 'http://mu.semte.ch/graphs/public'
MUTADIEDIENST_SYNC_INTERVAL = int(os.environ.get('MUTADIEDIENST_SYNC_INTERVAL', 1))

FILE_STATUSES =  {
    'READY': 'http://lblod.data.gift/file-download-statuses/ready-to-be-cached',
    'ONGOING': 'http://lblod.data.gift/file-download-statuses/ongoing',
    'COLLECTED': 'http://lblod.data.gift/file-download-statuses/collected',
    'FAILURE': 'http://lblod.data.gift/file-download-statuses/failure',
}
TASK_STATUSES = {
    'BUSY': 'http://redpencil.data.gift/id/concept/JobStatus/busy',
    'SCHEDULED': 'http://redpencil.data.gift/id/concept/JobStatus/scheduled',
    'SUCCESS': 'http://redpencil.data.gift/id/concept/JobStatus/success',
    'FAILED': 'http://redpencil.data.gift/id/concept/JobStatus/failed'
}

OPERATIONS = {
    'COLLECTING': 'http://lblod.data.gift/id/jobs/concept/TaskOperation/collecting',
     # Note: this URI has been abused to make the work
    'FULL_HARVEST_JOB': 'http://lblod.data.gift/id/jobs/concept/JobOperation/lblodHarvesting',
    'INCREMENTAL_COLLECTING': 'http://lblod.data.gift/id/jobs/concept/JobOperation/incrementalCollecting',
    'INCREMENTAL_COLLECTING_TASK_OPERATION': 'http://lblod.data.gift/id/jobs/concept/TaskOperation/mutatieDienstCollecting'
}

JOB_CREATOR_URI = "http://data.lblod.info/creator/verenigingen-scraper-service"

PREFIXES = """
   PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX adms: <http://www.w3.org/ns/adms#>
   PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
   PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
   PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
   PREFIX nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
   PREFIX dbpedia: <http://dbpedia.org/ontology/>
   PREFIX ndo: <http://oscaf.sourceforge.net/ndo.html#>
   PREFIX cogs: <http://vocab.deri.ie/cogs#>
"""

PUBLIC_API_BASE_VERENIGINGENREGISTER = os.environ.get("PUBLIC_API_BASE_VERENIGINGENREGISTER", None) or\
    "https://publiek.verenigingen.staging-vlaanderen.be"

MUTATIEDIENST_PATH = "/v1/verenigingen/mutaties"
MUTATIEDIENST_URL = f"{PUBLIC_API_BASE_VERENIGINGENREGISTER}{MUTATIEDIENST_PATH}"
