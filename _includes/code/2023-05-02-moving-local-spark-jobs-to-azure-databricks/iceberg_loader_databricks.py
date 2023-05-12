import datetime
from argparse import ArgumentParser, Namespace

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main(args):
    container_uri = (
        f"abfss://{args.container}@{args.storage_account}.dfs.core.windows.net"
    )

    spark_configs = {
        "spark.databricks.service.address": args.databricks_address,
        "spark.databricks.service.token": args.databricks_token,
        "spark.databricks.service.clusterId": args.databricks_cluster_id,
        f"fs.azure.account.key.{args.storage_account}.dfs.core.windows.net": args.access_key,
        "spark.sql.catalog.mtademo": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.mtademo.type": "hadoop",
        "spark.sql.catalog.mtademo.warehouse": f"{container_uri}/warehouse",
    }

    builder = SparkSession.builder
    for key, val in spark_configs.items():
        builder = builder.config(key, val)
    spark = builder.getOrCreate()

    table_name = "mtademo.bronze.bnt_traffic_hourly"
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
    df = spark.read.json(f"{container_uri}/{args.blob_name}")

    df = (
        df.withColumn("date", F.to_date(df["date"]))
        .withColumn("vehicles_e_zpass", df["vehicles_e_zpass"].cast("int"))
        .withColumn("vehicles_vtoll", df["vehicles_vtoll"].cast("int"))
    )

    df.writeTo(table_name).append()


def get_args() -> Namespace:
    parser = ArgumentParser()

    parser.add_argument("--storage-account", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--blob-name", required=True)
    parser.add_argument("--databricks-address", required=True)
    parser.add_argument("--databricks-token", required=True)
    parser.add_argument("--databricks-cluster-id", required=True)

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()
    main(args)
