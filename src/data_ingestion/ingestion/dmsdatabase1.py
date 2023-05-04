from data_ingestion.common.iceberg import IcebergIngestionJob
from data_ingestion.common.s3_bucket_names_config import get_datalake_bucket
from pyspark.sql import SparkSession, DataFrame, functions as F
from datetime import datetime

class Dmsdatabase1IngestionJob(IcebergIngestionJob):
    def __init__(self, spark: SparkSession, env: str, datetime_string: str, source: str, table: str,
                 ingestion_type: str, insert: bool, update: bool, delete: bool, keep_deletes: bool, optimize: str):
        self.env = env
        self.datetime_string = datetime_string
        self.datetime = datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S%z')
        self.source = source
        self.table = table
        self.src_bucket = get_datalake_bucket(env=self.env)
        self.datalake_bucket = get_datalake_bucket(env=self.env)
        self.big_tables = []

        # Create UDF for bucket ordering See https://github.com/apache/iceberg/issues/5977#issuecomment-1413328965 for
        # more information
        self.number_of_partition_buckets = 8
        spark.sparkContext._jvm.org.apache.iceberg.spark.IcebergSpark.registerBucketUDF(
            spark._jsparkSession, f"iceberg_bucket_bigint",
            spark.sparkContext._jvm.org.apache.spark.sql.types.DataTypes.LongType,
            self.number_of_partition_buckets)

        super().__init__(spark=spark,
                         ingestion_type=ingestion_type,
                         insert=insert,
                         update=update,
                         delete=delete,
                         keep_deletes=keep_deletes,
                         optimize=optimize,
                         ingestion_datetime=self.datetime)

    def raw_path(self) -> str:
        if self.ingestion_type == "cdc":
            if self.table_exists(database=self.glue_database(), table=self.glue_table()):
                path = f"s3://{self.datalake_bucket}/raw/{self.source}/dms/cdc/schema1/{self.table.upper()}/{self.datetime.strftime('%Y/%m/%d')}"
            else:
                # For the initial ingestion you have to
                path = f"s3://{self.datalake_bucket}/raw/{self.source}/dms/cdc/schema1/{self.table.upper()}/"
                # TODO: add logic to test the cdc ingestion
        else:
            path = f"s3://{get_datalake_bucket(self.env)}/raw/{self.source}/{self.table}/{self.datetime.strftime('%Y-%m-%d')}"
        self.logger.warn(f"raw_path: {path}")
        return path

    def file_list_full_path(self) -> str:
        path = f"s3://{self.datalake_bucket}/clean/{self.source}/{self.glue_table()}/file_list_full/{self.datetime.strftime('%Y-%m-%d')}"
        return path

    def manifest_full_path(self) -> str:
        path = f"s3://{self.datalake_bucket}/clean/{self.source}/{self.glue_table()}/manifest_file/{self.datetime.strftime('%Y-%m-%d')}"
        return path

    # Return dataframe with lower case columns
    def dataframe(self) -> DataFrame:
        dataframe = self.spark.read.parquet(self.raw_path())

        # Put all columns in lowercase by default
        columns = dataframe.columns
        for col in columns:
            dataframe = dataframe.withColumnRenamed(col, col.lower())

        return dataframe

    def glue_table(self) -> str:
        return self.table.replace('-', '_')

    def glue_database(self) -> str:
        return f"{self.env}_clean_{self.source}"

    def natural_keys(self) -> list[str]:
        return ["recid"]

    def updated_at_fields(self) -> list[str]:
        return ["modifieddatetime"]

    def partitioned_by(self) -> str:
        # Not sure partitioning is useful here...
        raise NotImplementedError
        # if self.table in self.big_tables:
        #     return f"bucket({self.number_of_partition_buckets}, {self.natural_keys()[0]})"
        # else:
        #     raise NotImplementedError

    def partition_ordering(self, dataframe: DataFrame) -> DataFrame:
        raise NotImplementedError
        # if self.table in self.big_tables:
        #     # See https://iceberg.apache.org/docs/latest/spark-writes/#writing-to-partitioned-tables for more info
        #     return dataframe.sortWithinPartitions(F.expr(f"iceberg_bucket_bigint({self.natural_keys()[0]})"))
        # else:
        #     raise NotImplementedError

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
