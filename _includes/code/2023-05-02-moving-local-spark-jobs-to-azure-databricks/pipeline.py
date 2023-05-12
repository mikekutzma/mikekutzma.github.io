import datetime
import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Optional

import pyspark.sql.functions as F
import requests
from azure.storage.blob import BlobClient
from prefect import flow, get_run_logger, task
from pyspark.sql import SparkSession


def get_spark(configs: Dict[str, str]) -> SparkSession:
    builder = SparkSession.builder
    for key, val in configs.items():
        builder = builder.config(key, val)
    spark = builder.getOrCreate()
    return spark


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
def create_bnt_iceberg_table(
    spark_configs: Dict[str, str],
    table_name: str,
):
    spark = get_spark(spark_configs)

    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        plaza_id string,
        date date,
        hour string,
        direction string,
        vehicles_e_zpass int,
        vehicles_vtoll int
    )
    USING iceberg
    PARTITIONED BY (years(date))
    """
    spark.sql(create_query).collect()


@task
def append_bnt_blob_to_iceberg(
    spark_configs: Dict[str, str],
    table_name: str,
    blob_uri: str,
):
    spark = get_spark(spark_configs)

    df = spark.read.json(blob_uri)

    df = (
        df.withColumn("date", F.to_date(df["date"]))
        .withColumn("vehicles_e_zpass", df["vehicles_e_zpass"].cast("int"))
        .withColumn("vehicles_vtoll", df["vehicles_vtoll"].cast("int"))
    )

    df.writeTo(table_name).append()


@flow
def pipeline(
    start_date: datetime.date,
    stop_date: datetime.date,
    storage_account: str,
    access_key: str,
    container: str,
    databricks_address: str,
    databricks_token: str,
    databricks_cluster_id: str,
):
    container_uri = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    blob_name = "stage/bnt_traffic_hourly.json"
    catalog_name = "mtademo"
    table_name = f"{catalog_name}.bronze.bnt_traffic_hourly"

    spark_configs = {
        "spark.databricks.service.address": databricks_address,
        "spark.databricks.service.token": databricks_token,
        "spark.databricks.service.clusterId": databricks_cluster_id,
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net": access_key,
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.type": "hadoop",
        f"spark.sql.catalog.{catalog_name}.warehouse": f"{container_uri}/warehouse",
    }

    data = get_bnt_data.submit(
        start_date,
        stop_date,
    )

    table_created = create_bnt_iceberg_table.submit(
        spark_configs,
        table_name,
    )

    blob_loaded = load_to_azure_blob.submit(
        data,
        storage_account,
        access_key,
        container,
        blob_name,
        wait_for=[data],
    )

    append_bnt_blob_to_iceberg.submit(
        spark_configs,
        table_name,
        f"{container_uri}/{blob_name}",
        wait_for=[table_created, blob_loaded],
    )


def main(args: Namespace):
    print("Executing pipeline")
    pipeline(
        start_date=args.start_date,
        stop_date=args.stop_date,
        storage_account=args.storage_account,
        access_key=args.access_key,
        container=args.container,
        databricks_address=args.databricks_address,
        databricks_token=args.databricks_token,
        databricks_cluster_id=args.databricks_cluster_id,
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
    parser.add_argument("--databricks-address", required=True)
    parser.add_argument("--databricks-token", required=True)
    parser.add_argument("--databricks-cluster-id", required=True)

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()
    main(args)
