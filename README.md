# RetailKart Fabric

Microsoft Fabric workspace artifacts for a retail analytics solution. The project
contains batch ingestion, data preparation, notebooks, lakehouses, warehouse
objects, real-time event processing, and operational demos.

## Architecture

The solution is organized around these flows:

- **Batch orders**: `pl_ingest_orders` copies `landing/orders_raw.csv` into
  the `raw_orders` table in `lh_retailkart`.
- **Data preparation**: `df_clean_orders` and the notebooks transform raw data
  into curated Delta tables such as `silver_orders`.
- **Analytics**: `pl_sales_daily`, `nb_sales_generate`, `nb_city_sales`, and
  `wh_retailkart` support daily sales and city-level reporting.
- **Real time**: `es_pos_live` ingests POS events, `es_pos_agg` aggregates
  events, and `eh_retailkart` provides Eventhouse/KQL exploration.
- **Automation and reliability**: `My activator.Reflex`, `CopyJob_1`, and the
  `pl_fail_demo` and `pl_rerun_demo` pipelines demonstrate event-driven
  actions, copy jobs, failure handling, and reruns.

## Repository layout

| Path | Fabric artifact |
| --- | --- |
| `lh_retailkart.Lakehouse` | Primary retail lakehouse |
| `lh_finance.Lakehouse` | Finance lakehouse and shortcuts |
| `wh_retailkart.Warehouse` | Retail warehouse schema and security objects |
| `pl_*.DataPipeline` | Ingestion, sales, failure, and rerun pipelines |
| `df_clean_orders.Dataflow` | Dataflow Gen2 transformation |
| `nb_*.Notebook` | PySpark generation, transformation, testing, and performance notebooks |
| `es_*.Eventstream` | POS streaming and aggregation eventstreams |
| `eh_retailkart.Eventhouse` | Real-time analytics and KQL database |
| `ApacheAirflowProject_1.ApacheAirflowJob` | Apache Airflow job definition |
| `env_retailkart.Environment` | Fabric environment configuration |
| `My activator.Reflex` | Reflex event automation definition |

## Getting started

1. Create or open a Microsoft Fabric workspace with Git integration enabled.
2. Connect the workspace to this GitHub repository and select the `main` branch.
3. Sync or update the workspace to import the Fabric artifacts.
4. Review the lakehouse and warehouse connections before running pipelines.
   Exported connection metadata may contain workspace-specific IDs that must
   be mapped to the target workspace.
5. Upload an `orders_raw.csv` file to the `landing` folder of `lh_retailkart`.
6. Run `pl_ingest_orders`, then execute the transformation and reporting
   notebooks or `pl_sales_daily`.
7. Configure the eventstream and Eventhouse connections before running the
   real-time workflows.

The notebook sources are stored as Fabric notebook exports in
`notebook-content.py`; they are intended to run in a Fabric Spark environment,
not as ordinary local Python scripts.

## Development

This repository stores Fabric metadata and source definitions. Changes should
normally be made through Fabric Git integration or by editing the corresponding
artifact export, then validating the artifact by syncing it to a development
workspace.

## License

This project is distributed under the [MIT License](LICENSE).