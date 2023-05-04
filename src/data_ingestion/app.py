import argparse

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame

from data_ingestion.common.iceberg import ingestion_type_choices, storage_optim_choices
from data_ingestion.common.spark import ClosableSparkSession, transform, SparkLogger
from data_ingestion.ingestion.salesforce import SalesforceIngestionJob
from data_ingestion.ingestion.dmsdatabase1 import Dmsdatabase1IngestionJob

DataFrame.transform = transform


def main():
    type_choices = ingestion_type_choices()
    optim_choices = storage_optim_choices()
    parser = argparse.ArgumentParser(description="data_ingestion")
    parser.add_argument(
        "-d", "--datetime", dest="datetime", help="datetime in format YYYY-mm-ddTHH:MM:SS+ZZ:ZZ", required=True
    )
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-s", "--source", dest="source", help="source system where the table came from", required=True
    )
    parser.add_argument(
        "-t", "--table", dest="table", help="table name to do transformations on", required=True
    )
    parser.add_argument(
        "--test", dest="test", action='store_true', default=False, required=False,
        help="run comparison tests between source and destination"
    )
    parser.add_argument(
        "--type", dest="type", default=type_choices[0], required=False, choices=type_choices,
        help=f"defines what type of data we are receiving, possible answers are: {', '.join(type_choices)}"
    )
    parser.add_argument(
        "--no-insert", dest="noinsert", action='store_true', default=False, required=False,
        help="input data contains new rows"
    )
    parser.add_argument(
        "--no-update", dest="noupdate", action='store_true', default=False, required=False,
        help="input data contains updated rows"
    )
    parser.add_argument(
        "--no-delete", dest="nodelete", action='store_true', default=False, required=False,
        help="input data contains deleted rows"
    )
    parser.add_argument(
        "--keep-deletes", dest="keep_deletes", action='store_true', default=False, required=False,
        help="set this option if you want to do soft deletes iso hard deletes in the table"
    )
    parser.add_argument(
        "--optimize", dest="optimize", default=optim_choices[0], required=False, choices=optim_choices,
        help=f"optimize storage for the following options: {', '.join(optim_choices)}"
    )
    args = parser.parse_args()

    with ClosableSparkSession("data_ingestion", env=args.env) as session:
        run(spark=session,
            environment=args.env,
            datetime_string=args.datetime,
            source=args.source,
            table=args.table,
            ingestion_type=args.type,
            insert=(not args.noinsert),
            update=(not args.noupdate),
            delete=(not args.nodelete),
            keep_deletes=args.keep_deletes,
            optimize=args.optimize,
            test=args.test)


def run(spark: SparkSession,
        environment: str,
        datetime_string: str,
        source: str,
        table: str,
        ingestion_type: str,
        insert: bool,
        update: bool,
        delete: bool,
        keep_deletes: bool,
        optimize: str,
        test: bool):
    """Main ETL script definition.

    :return: None
    """
    if source == "salesforce":
        if not test:
            SalesforceIngestionJob(spark=spark,
                                   env=environment,
                                   datetime_string=datetime_string,
                                   source=source,
                                   table=table,
                                   ingestion_type=ingestion_type,
                                   insert=insert,
                                   update=update,
                                   delete=delete,
                                   keep_deletes=keep_deletes,
                                   optimize=optimize).run()
        else:
            SalesforceIngestionJob(spark=spark,
                                   env=environment,
                                   datetime_string=datetime_string,
                                   source=source,
                                   table=table,
                                   ingestion_type=ingestion_type,
                                   insert=insert,
                                   update=update,
                                   keep_deletes=keep_deletes,
                                   delete=delete,
                                   optimize=optimize).test()
    elif source == "dmsdatabase1":
        if not test:
            Dmsdatabase1IngestionJob(spark=spark,
                               env=environment,
                               datetime_string=datetime_string,
                               source=source,
                               table=table,
                               ingestion_type=ingestion_type,
                               insert=insert,
                               update=update,
                               delete=delete,
                               keep_deletes=keep_deletes,
                               optimize=optimize).run()
        else:
            Dmsdatabase1IngestionJob(spark=spark,
                               env=environment,
                               datetime_string=datetime_string,
                               source=source,
                               table=table,
                               ingestion_type=ingestion_type,
                               insert=insert,
                               update=update,
                               delete=delete,
                               keep_deletes=keep_deletes,
                               optimize=optimize).test()


if __name__ == "__main__":
    main()
