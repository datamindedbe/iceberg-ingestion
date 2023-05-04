from pyspark.sql import SparkSession, DataFrame, Row, functions as F
from pyspark.sql.window import Window
from datetime import datetime
import boto3

from data_ingestion.common.spark import SparkLogger


def ingestion_type_choices() -> list[str]:
    return ["full", "incremental", "cdc", "replace"]


def storage_optim_choices() -> list[str]:
    return ["storage", "speed"]


class IcebergIngestionJob:

    def __init__(self,
                 spark: SparkSession,
                 ingestion_type: str,
                 insert: bool,
                 update: bool,
                 delete: bool,
                 keep_deletes: bool,
                 optimize: str,
                 ingestion_datetime: datetime):
        self.spark = spark
        self.ingestion_type = ingestion_type
        self.insert = insert
        self.update = update
        self.delete = delete
        self.keep_deletes = keep_deletes
        self.optimize = optimize
        self.ingestion_datetime = ingestion_datetime

        self.iceberg_retention_period = 90
        self.ingestion_datetime_field = "ingestion_datetime"
        self.hard_delete_field = "hard_deleted"

        self.logger = SparkLogger(spark)
        self.logger.info(f"""
Iceberg ingestion config
\tinsert:       {self.insert}
\tupdate:       {self.update}
\tdelete:       {self.delete}
\tkeep_deletes: {self.keep_deletes}
\toptimize:     {self.optimize}
\ttype:         {self.ingestion_type}
\tdatetime:     {self.ingestion_datetime}
        """)

    def default_iceberg_table_properties(self) -> dict:
        keep_days = self.iceberg_retention_period

        table_properties = {
            'history.expire.max-snapshot-age-ms': f'{keep_days * 86_400_000}',   # 90 days expiration
            'history.expire.min-snapshots-to-keep': f'{keep_days}',              # Keep at least 90 snapshots
            'write.target-file-size-bytes': f'{64 * 1025 * 1024}',               # 64 MB's preferred file size
        }

        # Speed means as little as possible files to consolidate the view. Good for smaller tables?
        if self.optimize == "speed":
            table_properties.update({
                'write.parquet.compression-codec': 'snappy',
                'format-version': 2,
                'write.merge.mode': 'copy-on-write',
                'write.update.mode': 'copy-on-write',
                'write.delete.mode': 'copy-on-write'
            })
        # storage means, avoid rewrites as much as possible. Good for larger tables to avoid storage costs?
        elif self.optimize == "storage":
            table_properties.update({
                'write.parquet.compression-codec': 'gzip',
                'format-version': 2,
                'write.merge.mode': 'merge-on-read',
                'write.update.mode': 'merge-on-read',
                'write.delete.mode': 'merge-on-read'
            })
        return table_properties

    def create_or_update_iceberg_table(self):
        database = self.glue_database()
        table = self.glue_table()
        updated_at_fields = self.updated_at_fields()
        natural_keys = self.natural_keys()
        keep_deletes = self.keep_deletes

        # Check whether table is partitioned
        try:
            partitioned_by = self.partitioned_by()
            partitioned = True
        except NotImplementedError:
            partitioned = False
            partitioned_by = ""

        # Craft the table properties that need to be set for this table
        table_properties = self.default_iceberg_table_properties()
        table_properties_string = ",".join([f"'{k}'='{v}'" for k, v in table_properties.items()])

        self.logger.info(f"Creating ur updating {database}.{table}")

        # Create or replace for small tables
        if self.ingestion_type == "replace":
            dataframe = self.dataframe().withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime))

            if partitioned:
                # Perform partition ordering
                dataframe = self.partition_ordering(dataframe)

            dataframe.createOrReplaceTempView("dataframe")

            self.spark.sql(f"""
                CREATE OR REPLACE TABLE {database}.{table}
                USING ICEBERG
                {f"PARTITIONED BY ({partitioned_by})" if partitioned else ""}
                TBLPROPERTIES({table_properties_string})
                AS SELECT * from dataframe
            """)

        # Create the table first
        elif not self.table_exists(database=database, table=table):
            # Read dataframe and put it in the right format
            dataframe = self.dataframe()
            # Add hard deleted field if necessary
            if keep_deletes:
                dataframe = dataframe.withColumn(self.hard_delete_field, F.lit(False).cast("Boolean"))
            dataframe = dataframe.withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime))

            # Remove the op column for cdc flows
            if self.ingestion_type == "cdc":
                dataframe = dataframe.drop("op")

            if partitioned:
                # Perform partition ordering
                dataframe = self.partition_ordering(dataframe)

            dataframe.createOrReplaceTempView("dataframe")

            # TODO: is partitioning necessary?
            sql_string = f"""
                CREATE TABLE IF NOT EXISTS {database}.{table}
                USING ICEBERG
                {f"PARTITIONED BY ({partitioned_by})" if partitioned else ""}
                TBLPROPERTIES({table_properties_string})
                AS SELECT * from dataframe
            """
            self.logger.info(sql_string)
            self.spark.sql(sql_string)

        # Update the existing table if it exists
        else:
            # Find all changes that need to happen first before performing a merge into + cache to avoid reading 3 times
            target = self.spark.read.table(f"{database}.{table}").cache()
            source = self.dataframe()
            if keep_deletes:
                source = source.withColumn(self.hard_delete_field, F.lit(False).cast("Boolean"))
            source = source.cache()
            empty = self.spark.createDataFrame([], source.schema)

            natural_key_join_condition = [F.col(f"source.{key}") == F.col(f"target.{key}") for key in natural_keys]
            update_join_condition = [F.col(f"source.{key}") > F.col(f"target.{key}") for key in updated_at_fields]

            # Situation is more or less similar for full and incremental updates
            if self.ingestion_type == "full" or self.ingestion_type == "incremental":
                # Find new entries by left anti joining on natural key on source column
                if self.insert:
                    new_entries = source.alias("source").join(target.alias("target"), natural_key_join_condition, "leftanti") \
                        .withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime))\
                        .withColumn("op", F.lit("I"))
                else:
                    new_entries = empty\
                        .withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime))\
                        .withColumn("op", F.lit("I"))

                # Find deleted entries by left anti joining on natural key on target column. No deletes with
                # incremental ingestion as it has no cdc column to detect hard deletes
                if self.delete and self.ingestion_type != "incremental":
                    deleted_entries = target.alias("target").join(source.alias("source"), natural_key_join_condition, "leftanti")\
                        .withColumn("op", F.lit("D"))
                    # Filter out the entries that already have been deleted
                    if keep_deletes:
                        deleted_entries = deleted_entries.filter(F.col(self.hard_delete_field) == F.lit(False).cast("Boolean"))
                else:
                    deleted_entries = empty\
                        .withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime))\
                        .withColumn("op", F.lit("D"))

                # Find updated entries by inner (left semi) joining the source and target + updated at field
                if self.update:
                    updated_entries = source.alias("source")\
                        .join(target.alias("target"), [*natural_key_join_condition, *update_join_condition], "leftsemi")\
                        .withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime))\
                        .withColumn("op", F.lit("U"))
                else:
                    updated_entries = empty\
                        .withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime)) \
                        .withColumn("op", F.lit("U"))

                # Put it all together and repartition to smaller entry as diffs are usually way smaller than total table
                changes = new_entries.union(deleted_entries.union(updated_entries))\
                    .withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime))
            # For the CDC use case, we need to be sure that we only take the latest entry from the list (in case
            # the same entry id is updated multiple times in the same processing window)
            elif self.ingestion_type == "cdc":
                changes = source\
                    .withColumn(self.ingestion_datetime_field, F.lit(self.ingestion_datetime))

                # Create window function that will partition by natural key and order them descending based on
                # updated_at fields
                window_spec_ordered = [F.col(key).desc() for key in updated_at_fields]
                window_spec = Window.partitionBy(natural_keys).orderBy(*window_spec_ordered)

                # Now apply the window function, rank it and only take the first one
                changes = changes.withColumn("row_number", F.row_number().over(window_spec))\
                    .filter(F.col("row_number") == 1).drop(F.col("row_number"))
            else:
                raise NotImplementedError

            # Calculated diffs on a daily basis will probably fit on a single partition
            changes = changes.repartition(1)

            delete_action = "DELETE"
            if keep_deletes:
                changes = changes.withColumn(self.hard_delete_field,
                                   F.when(changes.op == "D", F.lit(True).cast("Boolean")).otherwise(F.lit(False).cast("Boolean")))
                delete_action = f"UPDATE SET *"

            if partitioned:
                # Perform partition ordering
                changes = self.partition_ordering(changes)

            changes.createOrReplaceTempView("source")

            join_keys_string = ",".join(f"source.{key} = target.{key}" for key in natural_keys)

            self.spark.sql(f"""
                MERGE INTO {database}.{table} target
                USING (SELECT * FROM source) source
                ON {join_keys_string}
                WHEN MATCHED AND source.op = 'D' THEN {delete_action}
                WHEN MATCHED AND source.op = 'U' THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

    def table_exists(self, database: str, table: str) -> bool:
        # TODO: for some reason this fails with Iceberg, so find out why
        # return self.spark.catalog.tableExists(dbName=self.glue_database(), tableName=self.glue_table())
        tables_collection = self.spark.catalog.listTables(dbName=database)
        table_names_in_db = [table.name for table in tables_collection]
        return table in table_names_in_db

    def perform_iceberg_cleanup(self):
        database = self.glue_database()
        table = self.glue_table()

        self.spark.sql(f"""
            CALL iceberg.system.expire_snapshots('{database}.{table}')
        """)
        self.spark.sql(f"""
            CALL iceberg.system.remove_orphan_files('{database}.{table}')
        """)

    def perform_iceberg_compaction(self):
        database = self.glue_database()
        table = self.glue_table()

        self.spark.sql(f"""
            CALL iceberg.system.rewrite_data_files('{database}.{table}')
        """)

    def retrieve_iceberg_file_list(self) -> DataFrame:
        database = self.glue_database()
        table = self.glue_table()

        return self.spark.sql(f"""
            SELECT file_path from {database}.{table}.files
        """).repartition(1)

    def retrieve_iceberg_manifest_file(self) -> DataFrame:
        database = self.glue_database()
        table = self.glue_table()

        client = boto3.client('glue')
        response = client.get_table(DatabaseName=database, Name=table)
        row = Row("manifest")

        manifest = self.spark.sparkContext.parallelize([response["Table"]["Parameters"]["metadata_location"]]).map(row)
        return manifest.toDF().repartition(1)

    def diff_count(self) -> int:
        dataframe = self.dataframe()
        database = self.glue_database()
        table = self.glue_table()

        iceberg_table = self.spark.read.table(f"{database}.{table}").drop("ingestion_datetime")
        if self.keep_deletes:
            iceberg_table = iceberg_table.filter(F.col(self.hard_delete_field) == F.lit(False)).drop(self.hard_delete_field)

        # All rows should be identical comparing source and destination tables
        return iceberg_table.exceptAll(dataframe).union(dataframe.exceptAll(iceberg_table)).count()

    # List of "abstract" methods that should be implemented by the base class
    # The implementer of the base class should return the data frame that you want to save in the iceberg format. This
    # means that all cleaning and other operations should be performed before writing the data to the iceberg table format.
    def dataframe(self) -> DataFrame:
        raise NotImplementedError

    # Return the glue table of the glue database where you want to write the data to
    def glue_table(self) -> str:
        raise NotImplementedError

    # Returns the glue database of the table you want to write the data to
    def glue_database(self) -> str:
        raise NotImplementedError

    # Contains the list of columns containing the natural keys for that specific dataframe, this is necessary to
    # calculate updates to add
    def natural_keys(self) -> list[str]:
        raise NotImplementedError

    # Merge into statements only work when there is an updated_at timestamp field available. This function should
    # provide a list of columns containing a good updated at fields
    def updated_at_fields(self) -> list[str]:
        raise NotImplementedError

    # It is possible to enable partitioning while saving data to the data platform. This is to optimise queries on
    # larger datasets. Don't use this for small datasets as this will most likely decrease performance
    def partitioned_by(self) -> str:
        raise NotImplementedError

    # When saving partitioned dataframes iceberg expects some ordering withing partitions to be conducted. This function
    # will call the partioning ordering at the right moment in time. More information about this can be found at
    # https://iceberg.apache.org/docs/latest/spark-writes/#writing-to-partitioned-tables. To be able to perform bucket
    # partitioning, you can look at https://github.com/apache/iceberg/issues/5977#issuecomment-1413328965 on how to do
    # that with pyspark.
    def partition_ordering(self, dataframe: DataFrame) -> DataFrame:
        raise NotImplementedError
