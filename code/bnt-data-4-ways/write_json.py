import datetime
import json
from typing import Dict, List

import requests


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


def write_local(data: List[Dict], fname: str = "bnt_traffic_hourly.json"):
    tprint(f"Writing data to {fname}")
    with open(fname, "w") as f:
        json.dump(data, f)
    tprint("Finished writing file")


def main():
    data = get_bnt_data()
    write_local(data)


if __name__ == "__main__":
    main()
