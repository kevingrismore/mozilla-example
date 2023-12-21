from prefect import flow, task

from config import APPS


@flow
def app_store_analytics(date):
    """
    For each app
        Hit an api for some data

        structure the data real nice

        write it to a csv

        put it on bigquery in a date partition
        (all the app data goes to the same table)

    dbt run
    """
    for app_id, app_name in APPS:
        fetch_data(app_id, app_name)


@task
def fetch_data(app_id: str, app_name: str):
    # client = FakeAppleAPIClient()
    pass
