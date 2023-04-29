import datetime
import json
import subprocess
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Optional

import requests
from azure.storage.blob import BlobClient
from prefect import flow, get_run_logger, task

import iceberg_loader


@task
def get_bnt_data(
    start_date: datetime.date,
    stop_date: Optional[datetime.date] = None,
    chunksize: int = 100000,
) -> List[Dict]:
    _logger = get_run_logger()
    url = "https://data.ny.gov/resource/qzve-kjga.json"
    start_date_str = start_date.strftime("%Y-%m-%dT00:00:00.00")
    where_clause = f'date>="{start_date_str}"'

    if stop_date:
        stop_date_str = stop_date.strftime("%Y-%m-%dT00:00:00.00")
        where_clause = f'{where_clause} and date<"{stop_date_str}"'

    params = {"$limit": chunksize, "$offset": 0, "$where": where_clause}

    data: List[Dict] = []
    _logger.info(
        "Querying data from %s with start_date=%s and stop_date=%s",
        url,
        start_date,
        stop_date,
    )
    while True:
        res = requests.get(url, params=params)
        res.raise_for_status()

        chunk = res.json()
        _logger.info("Got chunk of %d records", len(chunk))

        data.extend(chunk)

        if len(chunk) < chunksize:
            break
        else:
            params["$offset"] = params["$offset"] + chunksize  # type: ignore

    _logger.info("Got %d records total", len(data))
    return data


@task
def load_to_azure_blob(
    data: List[Dict],
    storage_account: str,
    access_key: str,
    container: str,
    blob_name: str,
):
    _logger = get_run_logger()
    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob = BlobClient(
        account_url=account_url,
        container_name=container,
        blob_name=blob_name,
        credential=access_key,
    )

    blob_data = json.dumps(data)
    _logger.info("Writing data to %s", f"{account_url}/{container}/{blob_name}")
    blob.upload_blob(blob_data, overwrite=True)
    _logger.info("Finished writing blob")


@task
def load_azure_blob_to_iceberg_table(
    storage_account: str,
    access_key: str,
    container: str,
    blob_name: str,
    spark_submit_executable: str,
):
    _logger = get_run_logger()
    jars = [
        "hadoop-azure-3.3.2.jar",
        "azure-storage-blob-12.22.0.jar",
        "iceberg-spark-runtime-3.3_2.12-1.2.1.jar",
    ]
    _logger.info(
        "Executing %s with spark_submit at %s",
        iceberg_loader.__file__,
        spark_submit_executable,
    )
    spark_submit(
        iceberg_loader,
        spark_submit_executable,
        jars,
        storage_account=storage_account,
        access_key=access_key,
        container=container,
        blob_name=blob_name,
    )


def spark_submit(
    spark_module, spark_submit_executable: str, jars: List[str] = [], **kwargs
):

    command = [spark_submit_executable]

    if jars:
        command.extend(["--jars", ",".join(jars)])

    # Uncomment for large local backfill runs
    # command.extend(["--driver-memory", "8g", "--master", "local[*]"])

    command.extend([spark_module.__file__])

    if kwargs:
        args = [(f"--{key.replace('_', '-')}", value) for key, value in kwargs.items()]
        command.extend([x for pair in args for x in pair])

    out = subprocess.run(command, capture_output=True, check=True)
    return out


@flow
def pipeline(
    start_date: datetime.date,
    stop_date: datetime.date,
    storage_account: str,
    access_key: str,
    container: str,
    spark_submit_executable: str,
):
    data = get_bnt_data(args.start_date, args.stop_date)
    blob_name = "stage/bnt_traffic_hourly.json"
    load_to_azure_blob(data, storage_account, access_key, container, blob_name)
    load_azure_blob_to_iceberg_table(
        storage_account, access_key, container, blob_name, args.spark_submit_executable
    )


def main(args: Namespace):
    print("Executing pipeline")
    pipeline(
        start_date=args.start_date,
        stop_date=args.stop_date,
        storage_account=args.storage_account,
        access_key=args.access_key,
        container=args.container,
        spark_submit_executable=args.spark_submit_executable,
    )
    print("Pipeline complete")


def get_args() -> Namespace:
    parser = ArgumentParser()

    parser.add_argument(
        "--start-date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
        help="Starting date to pull data for",
        required=True,
    )

    parser.add_argument(
        "--stop-date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
        help="Stop date to pull data for",
        required=False,
    )
    parser.add_argument("--storage-account", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--spark-submit-executable", required=True)

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()
    main(args)
