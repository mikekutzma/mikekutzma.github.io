---
layout: post
title: "Prefect-Azure-Demo"
date: 
categories: 
---
Getting Prefect Up and Running in Azure

This is the first(?) post in a series I planm to put together focusing on how
to *deploy* (i.e get running) a prefect setup in the cloud, namely Azure.

While there seems to be a lot of opinionated *best practice* tutorials out
there, making use of Kubernetes and Helm or Prefect Cloud, my goal here is
really targeted towards a smaller analytics group that maybe doesn't have the
tehcnical bench to maintain a k8s setup, or would like to understand what is
actually necessary and how the different pieces fit together.

Said in another way, "We have some pipelines on prem, but we want to move to
the cloud and would like to use a more modern orchestration framework. What
does that look like?"

## Scope
For this post, since we'll focus mostly on a bare-bones 'get things running'
setup, we'll assume a fairly simple pipeline that will allow us to do some ELT,
fully orchestrated from Azure. What we'll build here is far from a
productionalized setup, but it's a step towards it. That is:
* Grab daily ridership data from the [NYC MTA daily ridership
  API](https://data.ny.gov/Transportation/MTA-Daily-Ridership-Data-Beginning-2020/vxuj-8kew)
* Dump the data into a container in Azure Blob Storage

## TLDR
* Set up Azure storage account and create container for data
* Test pipeline from local machine without prefect server
* Spin up VM to host Prefect Api/Orchestration Engine/UI

## Storage Account
For setting up the storage accout, the [Azure
Docs](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal)
do a better job than I would, probably just follow those.

Once you storage account if set up, create a new container to dump the data,
we'll call it `daily-ridership`. After doing so, your empty contianer should
look like so
![Empty Azure
Container]({{site.url}}/assets/images/prefect-azure-pt1/azure-container-empty.png)

At this point our very simple cloud storage is ready and we can move on to
testing our pipeline using a local prefect server

## Local Testing with Cloud Storage
We'll be using the following pipeline defintion 

```python
import json
import os

import requests
from azure.storage.blob import BlobClient
from prefect import flow, task


@task
def get_daily_ridership_from_api(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


@task
def load_data_to_blob(data, conn_str, container, blob_name):
    blob = BlobClient.from_connection_string(
        conn_str=conn_str,
        container_name=container,
        blob_name=blob_name,
    )

    blob_data = json.dumps(data)
    blob.upload_blob(blob_data, overwrite=True)


@flow
def daily_ridership_flow(url, conn_str, container):
    data = get_daily_ridership_from_api(url)
    load_data_to_blob(data, conn_str, container, "all_days.json")


if __name__ == "__main__":
    url = "https://data.ny.gov/resource/vxuj-8kew.json"
    conn_str = os.getenv("AZURE_CONN_STR")
    container = "daily-ridership"
    daily_ridership_flow(url, conn_str, container)
```


### Set environment variables
In order to authenticate with the BlobClient, we'll need the storage accoutn
connectino string. We can do that using the azure cli tool (along with jq to
parse the response)

```bash
$ az login
$ export AZURE_CONN_STR=`az storage account show-connection-string -g <resource-group> -n <storage-account-name> | jq -r .connectionString`
```

### Run the pipeline
With the environment variables set, we can do a local run of our pipeline

```bash
$ python daily_ridership.py
```

We can see the two tasks ran successfully ![Local Pipeline
Success]({{site.url}}/assets/images/prefect-azure-pt1/local-pipeline-success.png)

And checking up on our container, we can now see the blob ![Azure Container
with
Blob]({{site.url}}/assets/images/prefect-azure-pt1/azure-container-all_days-blob.json)