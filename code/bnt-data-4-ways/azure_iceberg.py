import datetime
from argparse import ArgumentParser, Namespace
from typing import Dict, List

import pyspark
import requests
from azure.storage.blob import BlobClient
from pyspark.sql import SparkSession


def tprint(msg: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f"[{timestamp}] {msg}")


def get_bnt_data(chunksize: int = 100000) -> List[Dict]:
    url = "https://data.ny.gov/resource/qzve-kjga.json"
    params = {"$limit": chunksize, "$offset": 0}

    data: List[Dict] = []
    while True:
        tprint(f"Requesting chunk of {chunksize} with offset {params['$offset']}")

        res = requests.get(url, params=params)
        chunk = res.json()

        tprint(f"Got chunk of size {len(chunk)}")

        data.extend(chunk)
        if len(chunk) < chunksize:
            break
        else:
            params["$offset"] = params["$offset"] + chunksize

    tprint(f"Got {len(data)} records total")
    return data


def main(args):
    data = get_bnt_data()
    spark = SparkSession.builder.appName("mta-demo-azure-iceberg").getOrCreate()

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

    df = spark.createDataFrame(data)

    df.writeTo("mtademo.bnt_traffic_hourly").create()


def get_args() -> Namespace:
    parser = ArgumentParser()

    parser.add_argument("--storage-account", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--container", required=True)

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()
    main(args)
