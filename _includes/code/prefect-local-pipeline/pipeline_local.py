import datetime
import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Optional

import requests
from azure.storage.blob import BlobClient
from prefect import flow, get_run_logger, task


@task
def get_bnt_data(
    start_date: datetime.date,
    stop_date: Optional[datetime.date] = None,
    chunksize: int = 10000,
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
def write_locally(data: List[Dict]):
    _logger = get_run_logger()
    outfile = "chunk.json"
    _logger.info("Writing file to %s", outfile)
    with open(outfile, "w") as f:
        json.dump(data, f)


@flow
def pipeline(start_date: datetime.date):
    data = get_bnt_data(args.start_date, args.stop_date)
    write_locally(data)


def main(args: Namespace):
    print("Executing pipeline")
    pipeline(start_date=args.start_date)
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

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()
    main(args)
