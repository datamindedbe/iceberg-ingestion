import boto3

def get_appflow_bucket(env: str) -> str:
    client = boto3.client('ssm')
    _env = "dev"

    if (env == "dev") or (env == "acc") or (env == "pro"):
        _env = env

    parameter = client.get_parameter(Name=f"/platform/{_env}/common/appflow_bucket", WithDecryption=True)
    return parameter['Parameter']['Value']


def get_datalake_bucket(env: str) -> str:
    client = boto3.client('ssm')
    _env = "dev"

    if (env == "dev") or (env == "acc") or (env == "pro"):
        _env = env

    parameter = client.get_parameter(Name=f"/platform/{_env}/common/datalake_bucket", WithDecryption=True)
    return parameter['Parameter']['Value']
