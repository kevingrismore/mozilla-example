# mozilla-example
A Prefect + dbt adaptation of a Mozilla data pipeline.

This example is intended to demonstrate a reduction in complexity in code and infrastructure management when interfacing with multiple external systems, covering concurrent data ingestion from an API, followed by transformation steps in a data warehouse (BigQuery).
Deployment steps involve running a GitHub Actions workflow which builds and pushes an image, then generates a Prefect deployment that can run that image on Google Cloud Run via its affiliated work pool.

