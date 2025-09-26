# Verenigingen Scraper Service

This service scrapes the verenigingen register.

## Overview

The service has two tasks:
1. Full sync from the register.
2. Incremental sync using the "mutatiedienst".

The way these tasks are triggered is different. The fetched data is also slightly different. In the future, we want to make this more uniform.

### Task: Full Sync

This task collects all active verenigingen in Flanders and Brussels and converts them to a JSON-LD file.
The JSON-LD is passed to the next task that imports the data into the database.

The trigger is done outside this service by the `scheduled-job-controller`. It creates a "HARVEST URL" job with URI: `http://lblod.data.gift/id/jobs/concept/JobOperation/lblodHarvesting`


### Task: Mutatiedienst Incremental Sync

This task queries the [mutatiedienst](https://vlaamseoverheid.atlassian.net/wiki/spaces/AGB/pages/6285361348/API+documentatie#Mutatiedienst) for changes since the last run.
It gives small batches with high update speed.

The trigger happens inside this service. We do it this way because we want frequent polling and we only create a job in the database when there is data to ingest. We also have better control over concurrency.

The data is unfiltered, so it may contain more than the full sync (which only fetches Flanders and Brussels).

### Interplay Between Tasks

- During the day: schedule mutatiedienst every few seconds.
- At night: stop incremental sync and do a full sync.

This makes sure no updates are missed.

### Models Used

- Jobs model: <https://github.com/lblod/job-controller-service>
- Feitelijke verenigingen: <https://data.vlaanderen.be/doc/applicatieprofiel/FeitelijkeVerenigingen/> (with some extensions)
- Custom model: keeps track of last sync state
  Example:
  ```
<http://data.lblod.info/id/283ea607-7d1d-4301-b4c7-cdd15f50f875>
  a <http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/MutatiedienstStateInfo>;
 <http://purl.org/dc/terms/modified> "2025-09-22T17:00:00.00"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
  <http://data.lblod.info/vocabularies/FeitelijkeVerenigingen/lastSequenceMutatiedienst> 0.
```

### Related Services

- <https://github.com/lblod/job-controller-service>
- <https://github.com/lblod/scheduled-job-controller-service>
- <https://github.com/lblod/harvesting-verenigingen-import-service>

## Setup
### docker-compose.yml
[TODO]
### delta-notifier
And add a delta rule in rules.js
```json
[
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status'
      },
      object: {
        type: 'uri',
        value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'
      }
    },
    callback: {
      method: 'POST',
      url: 'http://harvest_scraper/delta'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  }
]
```
## Reference

### Configuration

The service can be configured with the following environment variables:

#### `DEFAULT_GRAPH`
- **Default:** `http://mu.semte.ch/graphs/public`
- **Description:**
  RDF graph where the data is stored. Change this if you want the scraper to write to a different graph.

#### `MUTATIEDIENST_SYNC_INTERVAL_SECONDS`
- **Default:** `*/5`
- **Description:**
  Interval in seconds for how often the mutatiedienst incremental sync runs.
  The default means: every 5 seconds.

#### `MUTATIEDIENST_SYNC_INTERVAL_ACTIVITY_WINDOW`
- **Default:** `7-20`
- **Description:**
  Active hours (in 24h format) during which the mutatiedienst incremental sync runs.
  The default means: only run between 07:00 and 20:59.

#### `PUBLIC_API_BASE_VERENIGINGENREGISTER`
- **Default:** `https://publiek.verenigingen.staging-vlaanderen.be`
- **Description:**
  Base URL of the public verenigingen register API.
  Used by the scraper to fetch data. Can be changed to production or another environment.
