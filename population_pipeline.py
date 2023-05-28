import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List

import requests
from prefect import flow, get_run_logger, task
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule


@task
def get_population_data() -> List[Dict]:
    _logger = get_run_logger()
    url = "https://datausa.io/api/data"
    params = {"drilldowns": "State", "measures": "Population"}

    _logger.info(
        "Querying data from %s",
        url,
    )
    res = requests.get(url, params=params)
    res.raise_for_status()

    data = res.json()

    _logger.info("Got %d records total", len(data))
    return data


@task
def write_data(data):
    _logger = get_run_logger()
    fname = "population_data.json"
    _logger.info("Writing data to %s", fname)
    with open(fname, "w") as f:
        json.dump(data, f)


@flow
def population_pipeline():
    data = get_population_data.submit()
    write_data.submit(data, wait_for=[data])


def main(args):

    if args.deploy:
        deployment = Deployment.build_from_flow(
            flow=population_pipeline,
            name="population_pipeline",
            work_queue_name="default",
            schedule=IntervalSchedule(interval=30),
        )
        deployment.apply()
    else:
        population_pipeline()


def get_args() -> Namespace:
    parser = ArgumentParser()

    parser.add_argument("--deploy", action="store_true")

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()
    main(args)
