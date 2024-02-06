import asyncio
import os
from tempfile import NamedTemporaryFile

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .table_metadata import metric_data


class BigqueryClient:
    def __init__(self, dataset, project):
        self.dataset = dataset
        self.project = project
        # bigquery has a 100 concurrent request limit per method per user
        self.load_semaphore = asyncio.Semaphore(50)

    @staticmethod
    async def create_client(project_id, dataset_id):
        bq_client = bigquery.Client(project=project_id)

        dataset = bq_client.dataset(dataset_id)

        try:
            bq_client.get_dataset(dataset_id)
            print("Dataset {} already exists".format(dataset_id))
        except NotFound:
            print("Dataset {} is not found".format(dataset_id))
            bq_client.create_dataset(dataset)

        return BigqueryClient(dataset, project_id)

    async def create_table_if_not_exists(self, measure, dimension):
        table_name = BigqueryClient.get_table_name(measure, dimension)
        table_fqid = f"{self.dataset}.{table_name}"

        schema = BigqueryClient.get_schema(measure, dimension)
        description = metric_data[measure]["description"]

        table = self.dataset.table(table_name)

        bq_client = bigquery.Client(project=self.project)

        try:
            bq_client.get_table(table_fqid)  # Make an API request.
            print("Table {} already exists.".format(table_name))
            return table
        except NotFound:
            print("Table {} is not found.".format(table_name))
            table = bigquery.Table(table_fqid, schema=schema)
            table.description = description
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="date"
            )
            table = bq_client.create_table(table)
            print(f"Created table {table.table_id}")
            return table

    @staticmethod
    def get_table_name(measure, dimension):
        optin = "opt_in_" if metric_data[measure]["optin"] else ""
        return (
            f"{metric_data[measure]['name']}_by_{optin}" f"{dimension}"
            if dimension
            else f"{metric_data[measure]['name']}_total"
        )

    @staticmethod
    def get_schema(measure, dimension):
        schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("app_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField(
                metric_data[measure]["name"],
                metric_data[measure]["type"],
                mode="REQUIRED",
            ),
        ]

        if dimension:
            schema.append(bigquery.SchemaField(dimension, "STRING", mode="REQUIRED"))

        return schema

    async def write_data(self, app_name, measure, dimension, date, data, overwrite):
        bq_client = bigquery.Client(project=self.project)

        schema = BigqueryClient.get_schema(measure, dimension)
        table_name = BigqueryClient.get_table_name(measure, dimension)
        table_fqid = f"{self.dataset}.{table_name}"

        csv_data = [
            "\t".join(
                [entry["date"], app_name, str(entry["metric"]), entry["dimension"]]
            )
            for entry in data
        ]
        with NamedTemporaryFile(mode="w+", delete=False) as temp_file:
            temp_file.write("\n".join(csv_data))

        # Write to correct date partition
        # table = self.dataset.table(f"{table_fqid}${date.replace('-', '')}")

        async with self.load_semaphore:
            bq_client.query(
                (
                    f"DELETE FROM {table_fqid} "
                    f"WHERE date = '{date}' "
                    f"AND app_name = '{app_name}'"
                )
            )

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                if overwrite
                else bigquery.WriteDisposition.WRITE_APPEND,
                field_delimiter="\t",
                schema=schema,
            )

            with open(temp_file.name, "rb") as source_file:
                job = bq_client.load_table_from_file(
                    source_file, table_fqid, job_config=job_config
                )

            job.result()

        os.remove(temp_file.name)
        return table_name
