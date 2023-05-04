from airflow import DAG
from conveyor.operators import ConveyorSparkSubmitOperatorV2, ConveyorContainerOperatorV2
from datetime import timedelta
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator

import yaml
import os
from airflow.models import Variable
from airflow.datasets import Dataset

try:
    # Import the alerting feature
    from alerting import alert
except ImportError:
    # In environments where the alerting feature is not enabled and during dag validation, the alerting module will
    # not be available. The import statement above then raises a Python "ImportError". In environments without
    # alerting, we simply issue a warning instead of sending the alert.
    def alert(*_, **__):
        return lambda _: print("Alerting not deployed to this environment; skipping alerting")

project = "dataingestion"
conveyor_environment = Variable.get("environment")
env = conveyor_environment if conveyor_environment in ["acc", "pro"] else "dev"
date = "{{ ds }}"
date_path = date.replace("-", "/")

role = f"neo-iam-{project}-{env}"
region = "eu-west-3"

default_args = {
    "owner": "Conveyor",
    "depends_on_past": False,
    "start_date": dates.days_ago(1),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "aws_role": role,
    "on_failure_callback": alert(
        project=project,
        messager=lambda _: "Data ingestion project has failed!",
    ),
}

size_params = {
    "S": {
        "driver_size": "mx.small",
        "executor_size": "mx.small",
        "executors": 1,
        "mode": "local",
    },
    "M": {
        "driver_size": "mx.medium",
        "executor_size": "mx.medium",
        "executors": 3,
        "mode": "local",
    },
    "L": {
        "driver_size": "mx.large",
        "executor_size": "mx.large",
        "executors": 5,
        "mode": "cluster",
    },
    "XL": {
        "driver_size": "mx.large",
        "executor_size": "mx.xlarge",
        "executors": 8,
        "mode": "cluster",
    },
    "XXL": {
        "driver_size": "mx.large",
        "executor_size": "mx.2xlarge",
        "executors": 12,
        "mode": "cluster",
    },
}

# see importlib.resources to refer to other files or __FILE__ to get path of this file
dag = DAG(
    "data_ingestion_dms",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    max_active_runs=1,
    tags=["data_ingestion"],
)


def ingestion_operator(dag: DAG, source: str, table: str, schema: str, ingest_type: str) -> ConveyorContainerOperatorV2:
    return ConveyorContainerOperatorV2(
        dag=dag,
        task_id=f"{source}-{table}-ingestion",
        instance_type="mx.small",
        cmds=["./scripts/dms", region, source, table, env, date_path, schema, ingest_type],
    )


def cleaning_operator(dag: DAG, source: str, table: str, ingest_type: str, dataset:Dataset,
                      keep_deletes: bool, size: str) -> ConveyorSparkSubmitOperatorV2:
    application_args = [
        "--datetime", "{{ ts }}", "--env", env, "--source", source, "--table", table, "--type", ingest_type
    ]
    if keep_deletes:
        application_args.append("--keep-deletes")

    return ConveyorSparkSubmitOperatorV2(
        dag=dag,
        task_id=f"{source}-{table}-cleaning",
        num_executors=size_params[size]["executors"],
        driver_instance_type=size_params[size]["driver_size"],
        executor_instance_type=size_params[size]["executor_size"],
        mode=size_params[size]["mode"],
        spark_main_version=3,
        application="local:///opt/spark/work-dir/src/data_ingestion/app.py",
        application_args=application_args,
        outlets=[dataset]
    )


def test_operator(dag: DAG, source: str, table: str, ingest_type: str, keep_deletes: bool,
                  size: str) -> ConveyorSparkSubmitOperatorV2:
    application_args = [
        "--datetime", "{{ ts }}", "--env", env, "--source", source, "--table", table, "--type", ingest_type, "--test"
    ]
    if keep_deletes:
        application_args.append("--keep-deletes")

    return ConveyorSparkSubmitOperatorV2(
        dag=dag,
        task_id=f"{source}-{table}-test",
        num_executors=size_params[size]["executors"],
        driver_instance_type=size_params[size]["driver_size"],
        executor_instance_type=size_params[size]["executor_size"],
        mode=size_params[size]["mode"],
        spark_main_version=3,
        application="local:///opt/spark/work-dir/src/data_ingestion/app.py",
        application_args=application_args,
        retries=0,
    )


def get_end_operator(dag: DAG, source: str) -> DummyOperator:
    return DummyOperator(
        dag=dag,
        task_id=f"{source}_ingestion_end",
        outlets=[Dataset(f"clean_{source}/done")]
    )


config_path = os.path.join(os.path.dirname(dag.fileloc), "dms_flows.yaml")
with open(config_path, 'r') as dms_config:
    config = yaml.safe_load(dms_config)

# Generate the ingestion and cleaning jobs for all flows in the configuration
for source, tables in config.items():
    dag = DAG(
        f"data_ingestion_dms_{source}",
        default_args=default_args,
        schedule_interval="0 2 * * *",
        max_active_runs=1,
        tags=["data_ingestion"],
    )
    end = get_end_operator(dag=dag, source=source)
    for table, config in tables.items():
        ingest_type = config.get("type", "full")  # By default, do a full ingest.
        size = config.get("size", "M")  # By default, assume medium workload.
        schema = config.get("schema", "dbo")  # By default, assume dbo schema.
        keep_deletes = bool(config.get("keep_deletes", False))  # By default, perform hard deletes

        if ingest_type != "cdc":
            ingestion = ingestion_operator(dag=dag, source=source, table=table, schema=schema, ingest_type=ingest_type)

        dataset = Dataset(f"clean_{source}/{table.replace('-', '_')}")
        cleaning = cleaning_operator(dag=dag, source=source, table=table, ingest_type=ingest_type, dataset=dataset,
                                     keep_deletes=keep_deletes, size=size)
        cleaning >> end

        if ingest_type == "full" or ingest_type == "replace":
            test = test_operator(dag=dag, source=source, table=table, ingest_type=ingest_type,
                                 keep_deletes=keep_deletes, size=size)
            ingestion >> cleaning >> test
