# RetailKart Data Lab

Microsoft Fabric item exports and learning exercises. The repository contains
lakehouses, pipelines, notebooks, a Dataflow Gen2 definition, Eventstreams,
an Eventhouse database, a warehouse project, a Fabric environment, a Reflex
definition, a Copy job, and an Apache Airflow job.

## Contents

### Retail order artifacts

- `lh_retailkart.Lakehouse` is the main lakehouse referenced by the order
  pipelines and notebooks.
- `lh_finance.Lakehouse` contains a second lakehouse definition and shortcut
  metadata.
- `pl_ingest_orders.DataPipeline` copies `landing/orders_raw.csv` from
  `lh_retailkart` to the `raw_orders` table.
- `df_clean_orders.Dataflow` reads `raw_orders`, replaces missing city values,
  derives an `amount_band` column, and writes `df_orders_clean`.
- `pl_sales_daily.DataPipeline` runs a notebook for Chennai, Bengaluru, and
  Hyderabad, captures its row-count result, and sets a pipeline status based
  on whether the count is greater than 300.
- `wh_retailkart.Warehouse` contains SQL project metadata, tables, views, and
  a city-filter function and security policy.

### Streaming and analytics artifacts

- `es_pos_live.Eventstream` sends the sample `Bicycles` source to the
  `pos_events` table in `lh_retailkart`.
- `es_pos_agg.Eventstream` calculates a 10-second average of `No_Bikes`
  partitioned by `Neighbourhood` and sends the result to the `bike_agg2`
  table in `eh_retailkart`.
- `eh_retailkart.Eventhouse` contains KQL definitions for `Stocks`,
  `StocksDaily`, and `bike_agg2`, along with a transformation function and a
  materialized view.

### Exercises and supporting definitions

- `nb_*.Notebook` contains PySpark notebook exports for batch work, city
  sales, load patterns, streaming windows, performance, and failure
  exercises.
- `pl_fail_demo.DataPipeline` and `pl_rerun_demo.DataPipeline` are pipeline
  exercises for failure and rerun behavior.
- `nb_fail.Notebook` and `nb_perf_lab.Notebook` are corresponding notebook
  exercises.
- `ApacheAirflowProject_1.ApacheAirflowJob` contains a manual Airflow DAG
  whose task prints `Hello, World!`.
- `CopyJob_1.CopyJob`, `My activator.Reflex`, and
  `env_retailkart.Environment` contain exported Fabric item definitions.

## Working with the exports

These files are source exports, not a standalone local application. Import or
sync the item folders in a Microsoft Fabric workspace, then review the
workspace and item references before running them. Several exported
connections use placeholder workspace IDs, so they must be mapped to the
target workspace.

Notebook code is stored in each item's `notebook-content.py` file and is
intended for a Fabric Spark notebook session. The SQL files under
`wh_retailkart.Warehouse` can be built with the Microsoft Build SQL project
tooling and the Fabric Data Warehouse DAC package.

## License

This project is distributed under the [MIT License](LICENSE).
