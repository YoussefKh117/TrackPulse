# TrackPulse --- Supply Chain & Intelligent Logistics

> A cloud-based data engineering project that simulates vehicle
> telemetry and transforms raw events into structured datasets for
> fleet, trip, route, and cold-chain analysis.

**Program:** Samsung Innovation Campus - BD \
**Team:** Team 8

## Table of Contents

-   [Project Overview](#project-overview)
-   [Business Problem and Objectives](#business-problem-and-objectives)
-   [Architecture](#architecture)
-   [Data Flow](#data-flow)
-   [Azure Storage Layout](#azure-storage-layout)
-   [Lakehouse Layers](#lakehouse-layers)
-   [Analytical Datasets](#analytical-datasets)
-   [Results and Insights](#results-and-insights)
-   [Orchestration and Serving](#orchestration-and-serving)
-   [Technologies Used](#technologies-used)
-   [Run the Streaming Producer](#run-the-streaming-producer)
-   [Implementation Notes](#implementation-notes)
-   [Team](#team)

## Project Overview

TrackPulse explores how logistics companies can use vehicle telemetry
and shipment data to improve operational visibility. The project
simulates a fleet of **5,000 delivery vehicles** that produce telemetry
events such as vehicle and event IDs, timestamps, GPS coordinates,
speed, odometer, engine temperature, RPM, fuel-rate measurements, and
cargo-temperature readings where available.

The platform is designed to support both **streaming ingestion** and
**historical/incremental processing**. Events are captured in cloud
storage, processed using PySpark, validated, and organized into Delta
Lake layers. Analytical datasets support trip, route, vehicle, and
cold-chain analysis.

## Business Problem and Objectives

Logistics data platforms face several challenges: - High volumes of
telemetry events from connected vehicles. - Limited visibility into
potential operational anomalies. - Duplicate events, missing
measurements, and inconsistent records that can distort KPIs. -
Efficient integration of new events with historical data. - Missing
cargo-temperature measurements that limit cold-chain assessment. - Raw
telemetry that must be transformed before it is useful for business
reporting.

TrackPulse aims to: - Generate synthetic telemetry and ingest events
into Azure. - Preserve raw captured data for traceability and historical
processing. - Process data using PySpark and Delta Lake. - Validate
records, deduplicate events, and quarantine invalid records. - Integrate
new records incrementally instead of reloading the full history every
run. - Build analytical datasets for trip, route, vehicle, and
cold-chain analysis.

## Architecture

### Architecture Diagram

<img width="1774" height="887" alt="Image" src="https://github.com/user-attachments/assets/cbcd7267-9161-4936-b481-4ff3034be4a9" />


### Main Components

  -----------------------------------------------------------------------
  Component                           Purpose
  ----------------------------------- -----------------------------------
  Python producer                     Generates synthetic vehicle
                                      telemetry and sends events to Event
                                      Hubs.

  Azure Event Hubs                    Managed streaming ingestion
                                      endpoint and buffer.

  Event Hubs Capture                  Archives incoming events as raw
                                      Avro files.

  Azure Data Lake Storage Gen2 (ADLS  Stores raw, historical, curated
  Gen2)                               data, and checkpoints.

  Azure Databricks                    Environment for running notebooks
                                      and distributed processing.

  PySpark                             Parses, validates, deduplicates,
                                      joins, and aggregates data.

  Delta Lake                          Structured lakehouse tables and
                                      operations such as `MERGE`.

  Azure Stream Analytics              Intended continuous streaming
                                      queries and low-latency rules.

  Azure Data Factory (ADF)            Orchestrates scheduled processing
                                      and dependencies where configured.

  Azure SQL Database / Power BI       Intended serving and visualization
                                      components, subject to end-to-end
                                      validation.
  -----------------------------------------------------------------------

The continuous streaming path and scheduled historical/incremental path
serve different purposes. The 12-hour batch schedule should not restart
a continuous Stream Analytics job.

## Data Flow

1.  **Data generation:** A Python generator creates synthetic historical
    data and streaming telemetry events.
2.  **Ingestion:** The producer sends new events to the
    `telemetry-events` Event Hub in `ns-trackpulse-dev`.
3.  **Raw capture:** Event Hubs Capture writes incoming events to ADLS
    Gen2 as Avro files in the `telemetry-raw` container.
4.  **Bronze ingestion:** Databricks reads newly captured files using
    Auto Loader and persistent schema/checkpoint locations where
    configured, then writes parsed records to a Bronze Delta table.
5.  **Silver processing:** The incremental workflow validates fields and
    types, deduplicates by `event_id`, merges valid records into
    Streaming Silver, and sends invalid records to quarantine with
    quality reasons.
6.  **Historical integration:** Streaming records are integrated with
    Historical Silver using key-based merge logic. Delta Change Data
    Feed (CDF) can be used when enabled at the required source version
    and when the relevant versions are available.
7.  **Gold analytics:** Transformations create business-oriented
    datasets for trips, routes, vehicle telemetry, and cold-chain
    monitoring.
8.  **Serving and visualization:** Curated outputs can be published to a
    serving layer and Power BI after the relevant connections and
    refresh steps have been validated.

## Azure Storage Layout

The Storage Account is **`trackpulsestorage`**. The project uses two
main containers.

### `telemetry-raw`

Purpose: raw streaming archive written by Event Hubs Capture.

``` text
telemetry-raw/
├── ns-trackpulse-dev/
│   └── telemetry-events/
│       └── ... raw capture files ...
├── fastpath-alerts/
└── fastpath-window-analytics/
```

Verify the actual files and active outputs in Azure Storage. A directory
existing by itself does not prove a job is currently writing to it.

### `trackpulse-data`

Purpose: historical data, processing layers, checkpoints, and supporting
datasets.

``` text
trackpulse-data/
├── audit/
├── bronze/
├── checkpoints/
├── fastpath-window-analytics/
├── gold/
├── historical/
├── master_data/
└── silver/
```

  -----------------------------------------------------------------------
  Directory                           Purpose
  ----------------------------------- -----------------------------------
  `master_data/`                      Reference datasets such as
                                      vehicles, drivers, routes,
                                      shipments, and trip plans.

  `historical/`                       Historical datasets and/or
                                      historical processing outputs.

  `bronze/`                           Raw-to-Delta ingestion layer.

  `silver/`                           Validated and deduplicated data,
                                      including configured quarantine
                                      paths.

  `gold/`                             Aggregated analytical datasets.

  `checkpoints/`                      Producer/processing state and
                                      schema metadata.

  `audit/`                            Supporting audit information where
                                      implemented.

  `fastpath-window-analytics/`        Separate window-analytics output if
                                      configured and active.
  -----------------------------------------------------------------------

**Checkpoint warning:** Checkpoints track processing progress. Do not
delete or reset them casually; understand the recovery behavior first
because resetting them may cause reprocessing or skipped data.

## Lakehouse Layers

### Bronze

-   Ingests newly captured data from the raw archive.
-   Stores records in Delta format.
-   Stays close to the source and may contain duplicate event IDs.

### Silver

-   Validates required fields and data types.
-   Deduplicates records using `event_id`.
-   Merges valid records into curated streaming data.
-   Sends invalid records to quarantine with data-quality reasons.

### Gold

-   Joins and aggregates validated data for business analysis.
-   Provides structured datasets for reporting and further consumption.

## Analytical Datasets

  ---------------------------------------------------------------------------------
  Dataset                           Purpose                 Example analysis
  --------------------------------- ----------------------- -----------------------
  `gold.trip_summary`               Trip-level summary      Compare planned and
                                    joined with planned     operational trip
                                    trips and reference     information.
                                    data.                   

  `gold.route_daily_performance`    Route and               Review daily trip
                                    planned-start-date      volume, status counts,
                                    aggregates.             and available
                                                            actual-distance data.

  `gold.vehicle_daily_scorecard`    Vehicle and event-date  Review event counts,
                                    aggregates.             average/max speed,
                                                            engine temperature,
                                                            RPM, and recorded fuel
                                                            rate.

  `gold.cold_chain_daily_summary`   Shipment-oriented       Review available
                                    temperature summary.    readings, limit
                                                            compliance, and
                                                            measurement gaps.
  ---------------------------------------------------------------------------------

Metric caveats: - `fuel_rate_lph` is a rate, not total fuel consumed. -
Missing distance must not be treated as measured zero distance. -
Missing cargo-temperature readings mean compliance cannot be evaluated
from those measurements; missing data is not proof of a violation. -
Reliable delay/on-time KPIs require reconciled trip-status and
actual/planned timestamp definitions.

## Results and Insights

The values below come from recorded validation and analysis snapshots.
They describe those tested snapshots, not guaranteed results for every
future run.

### Data Quality

  Check                                                 Recorded result
  --------------------------------------------------- -----------------
  Bronze rows                                                   167,522
  Distinct Bronze event IDs                                     166,022
  Duplicate-ID excess rows in Bronze                              1,500
  Streaming Silver rows                                         166,010
  Distinct Streaming Silver event IDs                           166,010
  Quarantined invalid records                                        12
  Null event IDs in validated Streaming Silver                        0
  Duplicate event IDs in validated Streaming Silver                   0

### Historical Integration

  Check                                                            Recorded result
  -------------------------------------------------------------- -----------------
  Historical Silver unique event IDs                                    10,166,010
  Streaming Silver IDs missing from Historical Silver                            0
  Rows added by a recorded repeat `MERGE` for the tested batch                   0

These results support continuity between the tested streaming and
historical datasets and idempotent behavior for the tested repeat merge.

### Trip Analysis

-   Trip Summary contained **5,000 rows** and **5,000 unique trip IDs**.
-   Reference matches were present for trip plans, vehicles, drivers,
    shipments, and routes in the recorded output.
-   The output showed discrepancies between operational and planned
    statuses and planned-end timing fields.
-   Reconcile these discrepancies before publishing reliable on-time or
    delay KPIs. They do not prove all trips failed.

## Orchestration and Serving

Azure Data Factory is intended to orchestrate scheduled processing. The
target cadence is every 12 hours or a manual run, where the relevant
trigger and activities are configured.

``` text
New captured data
      ↓
Bronze ingestion
      ↓
Silver validation and deduplication
      ↓
Data-quality checks
      ↓
Historical integration
      ↓
Gold updates
      ↓
Serving layer
      ↓
Power BI refresh
```

Verify the actual activity sequence, successful runs, database
publishing, and Power BI refresh in ADF Monitor and the target services.
The continuous Stream Analytics path operates independently of the
12-hour batch schedule.

## Technologies Used

  -----------------------------------------------------------------------
  Technology                          Purpose
  ----------------------------------- -----------------------------------
  Python                              Synthetic telemetry generation and
                                      event publishing.

  Azure Event Hubs                    Streaming event ingestion.

  Event Hubs Capture                  Archiving streaming events to cloud
                                      storage.

  Azure Blob Storage / ADLS Gen2      Raw, historical, and curated data
                                      storage.

  Azure Databricks                    Data-processing environment.

  Apache Spark / PySpark              Distributed transformations,
                                      validation, joins, and
                                      aggregations.

  Delta Lake                          Lakehouse tables and merge-based
                                      updates.

  Auto Loader                         Incremental discovery and ingestion
                                      of new files where configured.

  Delta Change Data Feed (CDF)        Change-based historical integration
                                      where configured and validated.

  Azure Stream Analytics              Continuous streaming queries and
                                      low-latency rules where configured.

  Azure Data Factory (ADF)            Scheduled orchestration and
                                      activity dependencies where
                                      configured.

  Azure SQL Database                  Intended structured serving layer,
                                      subject to validation.

  Power BI                            Intended dashboards and business
                                      visualization, subject to
                                      validation.

  Azure Developer CLI (AZD)           Include only if actually used for
                                      deployment; it is distinct from
                                      ADF.
  -----------------------------------------------------------------------

## Run the Streaming Producer

### Prerequisites

1.  Python installed and the project virtual environment activated.
2.  Project dependencies installed using the repository's dependency
    file, if provided.
3.  The Azure Event Hubs Namespace and Event Hub created.
4.  Producer connection settings configured using the project's
    supported environment/configuration mechanism.
5.  The referenced historical generator script available at the
    specified path.
6.  Network access and Event Hubs permissions configured.

**Security:** Never commit Event Hubs connection strings, keys,
passwords, or other secrets to Git. Use environment variables or an
approved secret-management mechanism. Rotate secrets if they were
exposed.

### Windows PowerShell

From the project root, activate the virtual environment if you use one:

``` powershell
.\.venv\Scripts\Activate.ps1
```

Run the producer using the command shown in the project terminal.
Confirm that the script and options match the current repository
version:

``` powershell
python .\generator\trackpulse_eventhub_streamer_cloud_only.py `
  --generator ".\generator\generate_historical_data_trackpulse.py" `
  --stream-interval-seconds 2 `
  --batch-size 500 `
  --batch-pause-seconds 0 `
  --checkpoint-every-intervals 1
```

### Command Arguments

  -----------------------------------------------------------------------
  Argument                            Meaning
  ----------------------------------- -----------------------------------
  `--generator`                       Path to the historical-data
                                      generator used by the streaming
                                      producer.

  `--stream-interval-seconds 2`       Sets the stream interval to 2
                                      seconds according to the script's
                                      implementation.

  `--batch-size 500`                  Configures a batch size of 500
                                      events.

  `--batch-pause-seconds 0`           Sets the pause between batches to
                                      zero seconds.

  `--checkpoint-every-intervals 1`    Requests checkpoint persistence
                                      every interval according to the
                                      script's implementation.
  -----------------------------------------------------------------------

The exact behavior of interval, batch, and checkpoint options depends on
the current producer code. Check the script's `--help` output or
argument definitions if behavior differs.

### Verify the Producer

1.  Confirm the script starts without configuration or authentication
    errors.
2.  Review console output for send/progress messages.
3.  Check Event Hubs metrics or Data Explorer for incoming events.
4.  Check `telemetry-raw` for captured files after the Capture
    interval/threshold is met.
5.  Verify Databricks ingestion separately; producer success alone does
    not prove the entire pipeline succeeded.

Stop the producer with `Ctrl+C` when the test is complete, following the
script's checkpoint and shutdown behavior.

## Implementation Notes

The project has recorded evidence for Bronze/Silver validation,
historical integration, and selected Gold transformations. Describe
downstream stages---such as full scheduled orchestration, incremental
Gold execution, Azure SQL publishing, and automatic Power BI
refresh---as fully operational only after successful end-to-end
validation.

Before describing the project as production-ready, validate: - Stream
Analytics input, query, output, and actual running status. - ADF
activity sequence and 12-hour trigger. - Incremental Gold initialization
and update logic; do not schedule a legacy overwrite notebook as an
incremental job. - Azure SQL publishing and Power BI
connectivity/refresh, if used. - Reconciliation of planned versus
operational trip status and timing. - Cargo-temperature coverage and
shipment-level risk logic.

## Team

**TrackPulse --- SIC BD** 
*Youssef Khaled* 
*Salsabel Osama*
*Abdelrahman Mohamed*

------------------------------------------------------------------------

*TrackPulse --- Supply Chain & Intelligent Logistics \| Samsung
Innovation Campus Graduation Project*
