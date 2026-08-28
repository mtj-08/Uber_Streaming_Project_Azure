# Uber Ride Streaming Data Platform — Azure Databricks + Event Hubs

An end-to-end streaming and batch data engineering project that simulates an Uber-style ride booking system, streams booking events through **Azure Event Hubs (Kafka surface)**, and processes them in **Azure Databricks** using **Lakeflow Declarative Pipelines (SDP)** with medallion architecture (Bronze → Silver → Gold), Auto CDC (SCD Type 1/2), and stream-stream joins with watermarking. Historical/dimension data is bulk-loaded via an **Azure Data Factory** metadata-driven pipeline.

## Table of Contents
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Flow](#project-flow)
- [1. Azure Event Hub Setup](#1-azure-event-hub-setup)
- [2. Booking API (Event Producer)](#2-booking-api-event-producer)
- [3. Historical Ingestion via Azure Data Factory](#3-historical-ingestion-via-azure-data-factory)
- [4. Databricks Lakeflow Declarative Pipeline](#4-databricks-lakeflow-declarative-pipeline)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
- [Pipeline DAG](#pipeline-dag)
- [Setup & Installation](#setup--installation)
- [Environment Variables](#environment-variables)
- [Scheduling](#scheduling)
- [Repository Structure](#repository-structure)

---

## Architecture

```
Uber Booking App (FastAPI)
        │  (ride event JSON)
        ▼
Azure Event Hub (Kafka-enabled namespace)
        │
        ├────────────────────────────┐
        ▼                            ▼
Databricks SDP: rides_raw     Azure Data Factory
(streaming ingest via Kafka)  (bulk/historical load
        │                      of dimension tables
        │                      from GitHub → ADLS raw)
        ▼                            │
   Bronze Layer  ◄───────────────────┘
   (uber.bronze.*)
        │
        ▼
   Silver Layer — stg_rides (append flow, union of
   bulk + streaming) → silver_obt (streaming join,
   watermarked, Jinja-templated metadata-driven SQL)
        │
        ▼
   Gold Layer — dimension tables (Auto CDC SCD1),
   dim_location (Auto CDC SCD2), fact table (Auto CDC SCD1)
```

<img width="988" height="378" alt="Untitled Diagram" src="https://github.com/user-attachments/assets/09719b15-1761-42ba-8d8a-d3b8215c2f42" />


## Tech Stack

| Layer | Technology |
|---|---|
| Event ingestion | Azure Event Hubs (Kafka-compatible endpoint, Standard tier) |
| API / event producer | Python, FastAPI, Jinja2|
| Orchestration (batch) | Azure Data Factory (metadata-driven, parameterized pipelines) |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Stream + batch processing | Azure Databricks, Lakeflow Declarative Pipelines (SDP), PySpark Structured Streaming |
| Storage format | Delta Lake |
| Data modeling | Medallion architecture (Bronze/Silver/Gold), Star schema, SCD1 & SCD2 via Auto CDC |
| Templating | Jinja2 (metadata-driven SQL generation) |

## Project Flow

1. A booking is simulated through a simple FastAPI app.
2. The event is published to Azure Event Hub via the Kafka-compatible producer client.
3. Databricks SDP consumes the stream (Bronze), while ADF handles historical/dimension bulk loads from GitHub into ADLS.
4. Bronze data is unified and cleaned into a Silver One Big Table (OBT) using streaming append flows and a watermarked stream join.
5. Gold-layer dimension and fact tables are built using Auto CDC flows, with SCD Type 2 applied to the location dimension.

---

## 1. Azure Event Hub Setup

1. Create a new **Resource Group** and add an **Event Hub** resource inside it.
2. Create an **Event Hub Namespace** with the **Standard** pricing tier — this enables the Kafka-compatible surface, which lets Spark Structured Streaming consume events using the standard `kafka` source.
3. Under **Shared Access Policies** on the Event Hub, create two policies:
   - `send_policy` — Send permission (used by the producer/API)
   - `listen_policy` — Listen permission (used by Databricks to consume events)

<img width="2556" height="1082" alt="image" src="https://github.com/user-attachments/assets/16c583e5-04a7-480c-9742-0e252c2b7932" />


## 2. Booking API (Event Producer)

A minimal FastAPI app simulates the Uber ride-booking experience:

- `GET /` — renders the booking home page
- `GET /book` — generates a pseudo-random ride confirmation (via Faker) and publishes it to Event Hub

**Files:**
- `app.py` — FastAPI routes and template rendering
- `connection.py` — Event Hub producer client; reads `CONNECTION_STRING` and `EVENT_HUBNAME` from `.env` (sourced from the `send_policy` connection string)
- `data.py` — `generate_uber_ride_confirmation()`, the synthetic ride data generator

**Run locally:**
```bash
python -m venv venv
source venv/bin/activate      # or venv\Scripts\activate on Windows
pip install -r requirements.txt
uvicorn app:app --reload
```
Visiting `/book` generates a ride event and sends it as a single-event batch to Event Hub using `EventHubProducerClient`.

<img width="940" height="243" alt="image" src="https://github.com/user-attachments/assets/498125ca-af75-41af-8f02-0bbc8fec0cac" />


## 3. Historical Ingestion via Azure Data Factory

Used for **lift-and-shift** / bulk loading of static dimension and historical ride data from a GitHub repo into ADLS, decoupled from the real-time event stream.

1. Create an **Azure Data Factory** resource.
2. Create an **HTTP Linked Service** pointing at the GitHub repo (source).
3. Create an **ADLS Linked Service** using the storage account (sink).
4. Build a **dynamic dataset** so a single pipeline can ingest any file from GitHub — the relative path and filename are passed as parameters instead of being hardcoded.
5. Create a **config JSON file** listing every file to ingest, and upload it to the `raw` folder in the ADLS container.
6. Add a **Lookup activity** that reads this config file from ADLS.
7. Add a **ForEach activity** driven by the Lookup output: `@activity('files_array').output.value`
8. Inside the ForEach, add a **Copy activity**:
   - **Source:** dynamic file name — `@{item().file}.json`
   - **Sink:** a new dataset (`ds_ingest`) writing to a new directory in the `raw` container, with the destination filename set dynamically via `@{dataset().p_file}.json`

This metadata-driven design means new files can be ingested just by adding an entry to the config JSON — no pipeline changes required.

<img width="940" height="337" alt="image" src="https://github.com/user-attachments/assets/14dfb359-a19f-4b4f-9582-774c8f0829b8" />


## 4. Databricks Lakeflow Declarative Pipeline

### Bronze Layer

**Streaming ingestion from Event Hub (Kafka surface):**

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col

EH_NAMESPACE = spark.conf.get("connection_string.eh.namespace")
EH_NAME = spark.conf.get("connection_string.eh.name")
EH_CONN_STR = spark.conf.get("connection_string")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config":
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "startingOffsets": "earliest",
    "failOnDataLoss": "true",
    "maxOffsetsPerTrigger": "10000"
}

@dp.table
def rides_raw():
    df = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()
    return df.withColumn("rides", col("value").cast("string"))
```

The connection string used here is the **listen policy** string, passed in as a pipeline configuration parameter.

**Static dimension tables (ADLS → Bronze) via SAS token:**

Rather than an Access Connector, this project reads static dimension files (`map_cities`, `map_cancellation_reasons`, `bulk_rides`, `map_payment_methods`, `map_ride_statuses`, `map_vehicle_makes`, `map_vehicle_types`) directly from ADLS with a **SAS token**, via Pandas, then converts to Spark and writes to Delta:

refer **Bronze_ADLS.pynb**

### Silver Layer

Building a **One Big Table (OBT)** by unioning historical (bulk) and real-time (event) ride data, using **SDP append flows** to avoid the cost of a traditional merge:

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table("stg_rides")

@dp.append_flow(target="stg_rides")
def rides_bulk():
    return spark.readStream.table("bulk_rides")

@dp.append_flow(target="stg_rides")
def stream_rides():
    df = spark.readStream.table("rides_raw")
    df_parsed = df.withColumn(
        "parsed_rides", from_json(col("rides"), rides_schema)
    ).select("parsed_rides.*")
    return df_parsed
```

The raw event stream is parsed against the same schema as the bulk data (`rides_schema`) so both flows land in a consistent structure.

**Metadata-driven join with Jinja2:**

To avoid hand-writing (and maintaining) a large join query across 7+ dimension tables, the SQL is generated dynamically from a config array using Jinja2 templating. Each dimension is defined once — table, selected columns, join condition — and the template renders the full `SELECT ... FROM ... LEFT JOIN ...` statement. Adding a new dimension is just one more entry in `jinja_config`, with no template changes.

The rendered SQL is then adapted into a **streaming SQL table** definition with a watermark (required because this is a stream-to-static/stream join in SDP):

```sql
CREATE OR REFRESH STREAMING TABLE silver_obt AS

    SELECT 
        
           stg_rides.ride_id, stg_rides.confirmation_number, stg_rides.passenger_id, stg_rides.driver_id, stg_rides.vehicle_id, stg_rides.pickup_location_id, stg_rides.dropoff_location_id, stg_rides.vehicle_type_id, stg_rides.vehicle_make_id, stg_rides.payment_method_id, stg_rides.ride_status_id, stg_rides.pickup_city_id, stg_rides.dropoff_city_id, stg_rides.cancellation_reason_id, stg_rides.passenger_name, stg_rides.passenger_email, stg_rides.passenger_phone, stg_rides.driver_name, stg_rides.driver_rating, stg_rides.driver_phone, stg_rides.driver_license, stg_rides.vehicle_model, stg_rides.vehicle_color, stg_rides.license_plate, stg_rides.pickup_address, stg_rides.pickup_latitude, stg_rides.pickup_longitude, stg_rides.dropoff_address, stg_rides.dropoff_latitude, stg_rides.dropoff_longitude, stg_rides.distance_miles, stg_rides.duration_minutes, stg_rides.booking_timestamp, stg_rides.pickup_timestamp, stg_rides.dropoff_timestamp, stg_rides.base_fare, stg_rides.distance_fare, stg_rides.time_fare, stg_rides.surge_multiplier, stg_rides.subtotal, stg_rides.tip_amount, stg_rides.total_fare, stg_rides.rating
              
              ,
               
        
           vehicle_makes.vehicle_make
              
              ,
               
        
           map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute
              
              ,
               
        
           map_ride_statuses.ride_status
              
              ,
               
        
           map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth
              
              ,
               
        
           map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at
              
              ,
               
        
           cancellation_reason.cancellation_reason 
               
        
    FROM
        
              
                STREAM (uber.bronze.stg_rides) 
                WATERMARK booking_timestamp DELAY OF INTERVAL 3 minutes
                stg_rides
                
        
              
                LEFT JOIN uber.bronze.map_vehicle_makes vehicle_makes ON stg_rides.vehicle_make_id=vehicle_makes.vehicle_make_id
                
        
              
                LEFT JOIN uber.bronze.map_vehicle_types map_vehicle_types ON stg_rides.vehicle_type_id=map_vehicle_types.vehicle_type_id
                
        
              
                LEFT JOIN uber.bronze.map_ride_statuses map_ride_statuses ON stg_rides.ride_status_id=map_ride_statuses.ride_status_id
                
        
              
                LEFT JOIN uber.bronze.map_payment_methods map_payment_methods ON stg_rides.payment_method_id=map_payment_methods.payment_method_id
                
        
              
                LEFT JOIN uber.bronze.map_cities map_cities ON stg_rides.pickup_city_id=map_cities.city_id
                
        
              
                LEFT JOIN uber.bronze.map_cancellation_reasons cancellation_reason ON stg_rides.cancellation_reason_id=cancellation_reason.cancellation_reason_id

         
```

The 3-minute watermark on `booking_timestamp` handles late-arriving events while bounding state size.

### Gold Layer

Dimensional model built with `dp.create_auto_cdc_flow` for slowly changing dimensions:

| Table | Type | SCD Type | Notes |
|---|---|---|---|
| `dim_passenger` | Dimension | SCD1 | Deduplicated on `passenger_id` |
| `dim_driver` | Dimension | SCD1 | Deduplicated on `driver_id` |
| `dim_vehicle` | Dimension | SCD1 | Deduplicated on `vehicle_id` |
| `dim_payment` | Dimension | SCD1 | Deduplicated on `payment_method_id` |
| `dim_booking` | Dimension | SCD1 | Deduplicated on `ride_id` |
| `dim_location` | Dimension | **SCD2** | Full history maintained; sequenced by `city_updated_at` |
| `fact` | Fact | SCD1 | Composite key: `ride_id`, `pickup_city_id`, `payment_method_id`, `driver_id`, `passenger_id`, `vehicle_id` |

Example (SCD2 location dimension):

```python
@dp.table
def dim_location_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("pickup_city_id", "pickup_city", "city_updated_at", "region", "state")
    return df.drop_duplicates(subset=["pickup_city_id", "city_updated_at"])

dp.create_streaming_table("dim_location")
dp.create_auto_cdc_flow(
    target="dim_location",
    source="dim_location_view",
    keys=["pickup_city_id"],
    sequence_by="city_updated_at",
    stored_as_scd_type="2"
)
```

All other dimensions and the fact table follow the same pattern with `stored_as_scd_type="1"`.

## Pipeline DAG

<img width="940" height="535" alt="image" src="https://github.com/user-attachments/assets/855352d8-e3c3-4fce-8359-929a3eb9a3fd" />


The full pipeline graph shows the flow from `rides_raw` / `bulk_rides` → `stg_rides` → `silver_obt` → the Gold dimension and fact tables, all managed declaratively by SDP.

## Setup & Installation

```bash
git link <>
cd <Uber_Streaming_Project_Azure>
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Configure your `.env` file (see below), then:

```bash
uvicorn app:app --reload
```

Deploy the Databricks notebooks/SQL files as a **Lakeflow Declarative Pipeline** in your workspace, pointing the source files at the Bronze/Silver/Gold definitions in `pipelines/`.

## Environment Variables

Create a `.env` file (never commit this — add it to `.gitignore`):

```
CONNECTION_STRING=<event_hub_send_policy_connection_string>
EVENT_HUBNAME=<event_hub_name>
```

In Databricks, set the **listen policy** connection string and namespace/name as pipeline configuration parameters (not hardcoded), referenced via `spark.conf.get(...)`.

## Scheduling

The Lakeflow pipeline can be scheduled to run continuously or triggered every 1–5 minutes to keep Bronze → Silver → Gold layers near-real-time in production.

## Repository Structure

```
.
├── app.py                     # FastAPI booking app
├── connection.py               # Event Hub producer (send policy)
├── data.py                     # Synthetic ride data generator
├── templates/
│   ├── home.html
│   └── confirmation.html
├── adf/
│   └── pipelines/               # ADF pipeline JSON (Lookup, ForEach, Copy)
├── pipelines/
│   ├── bronze/
│   │   ├── rides_raw.py         # Kafka stream ingestion
│   │   └── static_dims.py       # SAS-token ADLS bronze load
│   ├── silver/
│   │   ├── stg_rides.py         # Append flow (bulk + stream)
│   │   ├── jinja_config.py      # Metadata-driven join config
│   │   └── silver_obt.sql       # Watermarked streaming join
│   └── gold/
│       └── dimensions_facts.py  # Auto CDC dimension & fact tables
├── docs/
│   └── images/                  # Architecture & DAG screenshots
├── requirements.txt
├── .env.example
└── README.md
```

---

**Note:** This project was built as a hands-on exploration of Azure Event Hubs' Kafka-compatible surface, Azure Data Factory metadata-driven pipelines, and Databricks Lakeflow Declarative Pipelines (SDP) with Auto CDC for dimensional modeling. The API app used here is referred from Ansh Lamba's git repository. 
