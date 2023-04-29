import datetime
from argparse import ArgumentParser, Namespace

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main(args):
    spark = SparkSession.builder.appName("mta-demo").getOrCreate()

    container_uri = (
        f"abfss://{args.container}@{args.storage_account}.dfs.core.windows.net"
    )

    spark.conf.set(
        f"fs.azure.account.key.{args.storage_account}.dfs.core.windows.net",
        args.access_key,
    )
    spark.conf.set("spark.sql.catalog.mtademo", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.mtademo.type", "hadoop")
    spark.conf.set("spark.sql.catalog.mtademo.warehouse", f"{container_uri}/warehouse")

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
    PARTITIONED BY (date)
    """
    spark.sql(create_query).show()
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

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()
    main(args)
