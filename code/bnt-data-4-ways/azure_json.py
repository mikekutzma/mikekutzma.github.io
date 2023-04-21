import datetime
import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List

import requests
from azure.storage.blob import BlobClient


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


def load_data_to_blob(
    data: List[Dict],
    storage_account: str,
    access_key: str,
    container: str,
    blob_name: str = "bnt_traffic_hourly.json",
):
    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob = BlobClient(
        account_url=account_url,
        container_name=container,
        blob_name=blob_name,
        credential=access_key,
    )

    blob_data = json.dumps(data)
    tprint(f"Writing data to {account_url}/{container}/{blob_name}")
    blob.upload_blob(blob_data, overwrite=True)
    tprint("Finished writing blob")


def main(args):
    data = get_bnt_data()
    load_data_to_blob(data, args.storage_account, args.access_key, args.container)


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
