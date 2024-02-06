from datetime import datetime

from prefect import flow, task
from prefect_dbt import DbtCliProfile, DbtCoreOperation

from analytics.bigquery import BigqueryClient
from analytics.client import AnalyticsClient
from analytics.export import AnalyticsExport
from analytics.table_metadata import dimensions, metric_data
from config import APPS, EXPORT_DATASET_ID, PROJECT_ID


@flow
def app_store_analytics(start_date: datetime = datetime.today()):
    for app_id, app_name in APPS:
        app_export(app_id, app_name, start_date)

    run_dbt(start_date)


@flow(flow_run_name="{app_name}-export")
async def app_export(app_id: str, app_name: str, start_date: datetime):
    client = AnalyticsClient()
    analytics_export = AnalyticsExport(
        client=client,
        project=PROJECT_ID,
        dataset=EXPORT_DATASET_ID,
        app_id=app_id,
        app_name=app_name,
    )

    for dimension in dimensions:
        for metric, data in metric_data.items():
            await start_export.submit(
                analytics_export,
                start_date,
                app_name,
                metric,
                data,
                dimension,
            )


@task
async def start_export(
    analytics_export: AnalyticsExport,
    start_date: datetime,
    app_name: str,
    metric: str,
    data: dict,
    dimension: str,
):
    print(f"{metric} - {dimension}")

    # Generate a few rows of fake data
    rows = []
    for i in range(0, 3):
        rows.append(analytics_export._generate_fake_data(start_date, data, seed=i))

    bq_client = await BigqueryClient.create_client(PROJECT_ID, EXPORT_DATASET_ID)

    data_by_date = {start_date.strftime("%Y-%m-%d"): rows}

    await analytics_export.write_data(
        bq_client,
        app_name,
        metric,
        dimension,
        data_by_date,
        overwrite=False,
    )


@task
def run_dbt(start_date: datetime):
    profile = DbtCliProfile.load("mozilla-demo")

    DbtCoreOperation(
        commands=[
            f"dbt run --vars '{{submission_date: {start_date.strftime('%Y-%m-%d')}'}}"
        ],
        project_dir="transformations",
        overwrite_profiles=True,
        dbt_cli_profile=profile,
    ).run()


if __name__ == "__main__":
    app_store_analytics()
