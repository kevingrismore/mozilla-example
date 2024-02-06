import asyncio
import random
from datetime import datetime

from faker import Faker
from faker.providers import company

from analytics.bigquery import BigqueryClient
from analytics.client import AnalyticsClient


class AnalyticsExport:
    def __init__(self, client: AnalyticsClient, project, dataset, app_id, app_name):
        self.client = client
        self.project = project
        self.dataset = dataset
        self.app_id = app_id
        self.app_name = app_name

        fake = Faker()
        fake.add_provider(company)
        self.fake = fake

    def _generate_fake_data(
        self, date: datetime, metric: dict, seed: int
    ) -> int | float:
        Faker.seed(seed)
        if metric["type"] == "INT64":
            return {
                "date": date.strftime("%Y-%m-%d"),
                "metric": random.randint(0, 10000),
                "dimension": self.fake.company(),
            }
        elif metric["type"] == "FLOAT64":
            return {
                "date": date.strftime("%Y-%m-%d"),
                "metric": round(random.uniform(0, 1), 2),
                "dimension": self.fake.company(),
            }

    @staticmethod
    async def write_data(
        bq_client: BigqueryClient, app_name, measure, dimension, data_by_date, overwrite
    ):
        await bq_client.create_table_if_not_exists(measure, dimension)

        write_promises = []

        for date, data in data_by_date.items():
            if not data:
                continue

        write_promises.append(
            bq_client.write_data(app_name, measure, dimension, date, data, overwrite)
        )

        await asyncio.gather(*write_promises)
