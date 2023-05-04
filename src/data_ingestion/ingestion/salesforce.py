from data_ingestion.common.iceberg import IcebergIngestionJob

from data_ingestion.common.s3_bucket_names_config import get_appflow_bucket, get_datalake_bucket
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime


class SalesforceIngestionJob(IcebergIngestionJob):
    def __init__(self, spark: SparkSession, env: str, datetime_string: str, source: str, table: str,
                 ingestion_type: str, insert: bool, update: bool, delete: bool, keep_deletes: bool, optimize: str):
        self.env = env
        self.datetime_string = datetime_string
        self.datetime = datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S%z')
        self.source = source
        self.table = table
        self.src_bucket = get_appflow_bucket(env=self.env)
        self.datalake_bucket = get_datalake_bucket(env=self.env)
        super().__init__(spark=spark,
                         ingestion_type=ingestion_type,
                         insert=insert,
                         update=update,
                         delete=delete,
                         keep_deletes=keep_deletes,
                         optimize=optimize,
                         ingestion_datetime=self.datetime)

    def raw_path(self) -> str:
        path = f"s3://{self.datalake_bucket}/raw/{self.source}/{self.table}/{self.datetime.strftime('%Y-%m-%d')}"
        return path

    def file_list_full_path(self) -> str:
        path = f"s3://{self.datalake_bucket}/clean/{self.source}/{self.glue_table()}/file_list_full/{self.datetime.strftime('%Y-%m-%d')}"
        return path

    def manifest_full_path(self) -> str:
        path = f"s3://{self.datalake_bucket}/clean/{self.source}/{self.glue_table()}/manifest_file/{self.datetime.strftime('%Y-%m-%d')}"
        return path

    def dataframe(self) -> DataFrame:
        return self.spark.read.parquet(self.raw_path())

    def glue_table(self) -> str:
        return self.table.replace('-', '_')

    def glue_database(self) -> str:
        return f"{self.env}_clean_{self.source}"

    def natural_keys(self) -> list[str]:
        return ["Id"]

    def updated_at_fields(self) -> list[str]:
        return ["SystemModstamp"]

    def partitioned_by(self) -> str:
        # Example on how to perform partitioning on the respective tables
        # if self.table == "task":
        #     return f"days({self.ingestion_datetime_field})"
        raise NotImplementedError

    def run(self):
        # Create or Update the iceberg table
        self.create_or_update_iceberg_table()

        # Perform default iceberg cleanup after each run
        self.perform_iceberg_cleanup()

        # Perform compaction once a month
        if self.datetime.strftime('%d') == "01":
            self.perform_iceberg_compaction()

        # Write the file list to S3, so it can be used for ingestion purposes
        file_list = self.retrieve_iceberg_file_list()
        file_list.write.mode("overwrite").json(self.file_list_full_path())

        # Write the manifest file to s3, so it can be used for ingestion purposes
        iceberg_manifest = self.retrieve_iceberg_manifest_file()
        iceberg_manifest.write.mode("overwrite").json(self.manifest_full_path())


    def test(self):
        diff_count = self.diff_count()

        assert diff_count == 0, f"The tables should be identical, but they are not. diff_count = {diff_count}"

