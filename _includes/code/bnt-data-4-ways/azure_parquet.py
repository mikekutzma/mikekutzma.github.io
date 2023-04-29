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
    spark = SparkSession.builder.appName("mta-demo-azure-parquet").getOrCreate()

    spark.conf.set(
        f"fs.azure.account.key.{args.storage_account}.dfs.core.windows.net",
        args.access_key,
    )

    df = spark.createDataFrame(data)

    container_uri = (
        f"abfss://{args.container}@{args.storage_account}.dfs.core.windows.net"
    )
    blob_name = "bnt_traffic_hourly.parquet"
    df.write.save(f"{container_uri}/{blob_name}", format="parquet", mode="overwrite")
    tprint(
        f"Wrote data to https://{args.storage_account}.blob.core.windows.net/{args.container}/{blob_name}"
    )


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
