---
title: Prefect Local Pipeline
date: 2023-04-27 08:48 -0400
tags: Tech MTA Azure Prefect Iceberg Spark
---

* Orchestration: Local
* Compute: Local
* Storage: Cloud

In this demo, we'll build a pipeline using Prefect that will extract and
process data from the [MTA Bridge and Tunnel Hourly Traffic
Dataset](https://data.ny.gov/Transportation/Hourly-Traffic-on-Metropolitan-Transportation-Auth/qzve-kjga),
and do some downstream analysis.  
Our pipeline will have the following steps:
* Pull new data from the Bridge and Tunnel API
* Ingest into Iceberg table

## Prerequisites
I won't spend too much time on the *how* around installing these (there are
better tutorials floating around), but more on *what* exactly the prerequisites
I'll assume are.

### Prefect
[Prefect](https://docs.prefect.io/latest/getting-started/installation/) is the
main tool we'll be using to orchestrate our pipelines. In this post, we'll use
prefect locally, without even running the prefect server, so the install is
pretty basic. We'll also install some other dependencies we'll use later in our
virtualenv.

```bash
mkdir prefect-azure-demo && cd prefect-azure-demo
python -m venv venv
venv/bin/python -m pip install prefect azure-storage-blob
venv/bin/pip freeze > requirements.txt
```

### Spark
We'll make use of Spark for some of our transformation tasks, so we'll want
Spark running locally as well. Installing a local spark cluster is outside of
the scope of this post, we'll assume Spark3.3.2. Some resources:
* [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html) 

We'll also need a few jars that will allow us to load data into Azure and on
Azure, into Iceberg tables. For now, we'll just grab them locally

```bash
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.2/hadoop-azure-3.3.2.jar
wget https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.22.0/azure-storage-blob-12.22.0.jar
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.2.1/iceberg-spark-runtime-3.3_2.12-1.2.1.jar
```

### Azure Storage

For setting up the storage accout, the [Azure
Docs](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal)
do a better job than I would, probably just follow those.

Once you storage account if set up, create a new container to dump the data,
we'll call it `mta-demo`.

## Pipeline Construction

First, we'll set up a basic pipeline consisting of a single flow, and two
tasks:
* Pull data from API
* Load data to local json

This will allow us to check the initial circuits, and understand what it looks
like to run prefect locally.

{% highlight python %}
{% include code/prefect-local-pipeline/pipeline_local.py %}
{% endhighlight %}

Running this pipeline, we can see data being successfully extracted and loaded
into a local json file.
![local success](/assets/images/2023-04-27-prefect-local-pipeline/local-success.png)
With this working, we can swap out our local storage with our azure storage.

To do this, we'll replace our `write_locally` task with a new
`write_to_azure_blob` task

```python
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
```

Next we'll update our flow to call this new task in place of our old one

```python
@flow
def pipeline(
    start_date: datetime.date,
    stop_date: datetime.date,
    storage_account: str,
    access_key: str,
    container: str,
):
    data = get_bnt_data(args.start_date, args.stop_date)
    blob_name = "stage/bnt_traffic_hourly.json"
    load_to_azure_blob(data, storage_account, access_key, container, blob_name)
```

And finally we just need to update our `get_args` and `main` methods to allow
for passing these new azure arguments

```python
def main(args: Namespace):
    print("Executing pipeline")
    pipeline(
        start_date=args.start_date,
        stop_date=args.stop_date,
        storage_account=args.storage_account,
        access_key=args.access_key,
        container=args.container,
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

    args = parser.parse_args()

    return args
```

We can then run this with 

```bash
venv/bin/python pipeline.py \
    --start-date 2023-04-10 \
    --stop-date 2023-04-27 \
    --storage-account <your-storage-account> \
    --container mta-demo \
    --access-key '<your-access-key>'
```

After which we should see successful logs
![load to azure logs]({{site.url}}/assets/images/2023-04-27-prefect-local-pipeline/load-to-azure-logs.png)
And upon checking our container, we can now see the blob in our new staging zone
![azure json]({{site.url}}/assets/images/2023-04-27-prefect-local-pipeline/azure-json.png)
We're ready to now add a second step in our pipeline, namely ingesting our
staged blob into an Iceberg table.

To do so, we'll write a pyspark script to read the blob, perform some
transformations, and append to the table.

Our script looks like this

{% highlight python %}
{% include code/prefect-local-pipeline/iceberg_loader.py %}
{% endhighlight %}

To test, we can even spark-submit our script directly from outside of the
pipeline with

```bash
spark-submit \
    --jars hadoop-azure-3.3.2.jar,azure-storage-blob-12.22.0.jar,iceberg-spark-runtime-3.3_2.12-1.2.1.jar \
    bridgeNtunnel_traffic/azure_iceberg.py \
    --storage-account <your-storage-account> \
    --container mta-demo \
    --access-key <your-access-key> \
    --blob-name stage/bnt_traffic_hourly.json
```

In order to incorporate this into our pipeline, since we are just calling spark
locally, we can use a subprocess call. Our new task (and help function) will
look like

```python
@task
def load_azure_blob_to_iceberg_table(
    storage_account: str,
    access_key: str,
    container: str,
    blob_name: str,
    spark_submit_executable: str,
):
    _logger = get_run_logger()
    jars = [
        "hadoop-azure-3.3.2.jar",
        "azure-storage-blob-12.22.0.jar",
        "iceberg-spark-runtime-3.3_2.12-1.2.1.jar",
    ]
    _logger.info(
        "Executing %s with spark_submit at %s",
        iceberg_loader.__file__,
        spark_submit_executable,
    )
    spark_submit(
        iceberg_loader,
        spark_submit_executable,
        jars,
        storage_account=storage_account,
        access_key=access_key,
        container=container,
        blob_name=blob_name,
    )


def spark_submit(
    spark_module, spark_submit_executable: str, jars: List[str] = [], **kwargs
):

    command = [spark_submit_executable]

    if jars:
        command.extend(["--jars", ",".join(jars)])

    command.extend([spark_module.__file__])

    if kwargs:
        args = [(f"--{key.replace('_', '-')}", value) for key, value in kwargs.items()]
        command.extend([x for pair in args for x in pair])

    out = subprocess.run(command, capture_output=True)
    return out
```

And our updated pipeline will look like

```python
@flow
def pipeline(
    start_date: datetime.date,
    stop_date: datetime.date,
    storage_account: str,
    access_key: str,
    container: str,
    spark_submit_executable: str,
):
    data = get_bnt_data(args.start_date, args.stop_date)
    blob_name = "stage/bnt_traffic_hourly.json"
    load_to_azure_blob(data, storage_account, access_key, container, blob_name)
    load_azure_blob_to_iceberg_table(
        storage_account, access_key, container, blob_name, args.spark_submit_executable
    )
```
where `spark_submit_executable` is the path to our local spark-submit command.

Upon running our new pipeline, we'll now see our spark job being submitted
after the api task

![iceberg pipeline logs]({{site.url}}/assets/images/2023-04-27-prefect-local-pipeline/iceberg-pipeline-logs.png)
And checking out our iceberg table, we can follow the directories to the data
directory and see our paritioned data
![azure-iceberg-bronze]({{site.url}}/assets/images/2023-04-27-prefect-local-pipeline/azure-iceberg-bronze.png)
Opening up a pyspark repl and initializing our connection parameters as in the
`iceberg_loader.py` script, we can now query our table using sql syntax
![iceberg-query]({{site.url}}/assets/images/2023-04-27-prefect-local-pipeline/iceberg-query.png)

This completes our demo, with our final pipeline script in full looking like

{% highlight python %}
{% include code/prefect-local-pipeline/pipeline.py %}
{% endhighlight %}
