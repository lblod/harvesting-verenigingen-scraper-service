import os
RESOURCE_BASE = "http://data.lblod.info/id/"
SCRAPE_JOB_TYPE = "http://lblod.data.gift/ns/ScrapeJob"
SCRAPE_CAMPAIGN_JOB_TYPE = "http://lblod.data.gift/ns/ScrapeCampaignJob"
SCRAPE_GRAPH = "http://mu.semte.ch/graphs/jobs-graph"
DEFAULT_GRAPH = os.environ['DEFAULT_GRAPH'] or 'http://mu.semte.ch/graphs/public'
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
    'COLLECTING': 'http://lblod.data.gift/id/jobs/concept/TaskOperation/collecting'
}
