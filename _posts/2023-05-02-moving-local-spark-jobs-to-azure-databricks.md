---
title: Remote Spark Execution Tasks in Prefect Flows using Databricks Connect
date: 2023-05-12 17:00 -0400
tags: Tech MTA Azure Prefect Iceberg Spark Databricks
---

Continuing on our pipeline from [Prefect Local Pipeline]({% post_url
2023-04-27-prefect-local-pipeline %}), one of the more stick pieces of our pipeline is the fact that we use
`spark_submit` on a local machine to execute our spark transformations. This is
at the very least annoying for a number of reasons:
1. Getting a spark cluster running is enough of a pain locally, let alone doing
   so manually in a production environment.
1. We need to make use of a subprocess to exeute the spark process, which
   forces us to communicate through stdin/stdout rather than using native
   python objects.
1. Because we call this job from a subprocess, we need to separate our pipeline
   code from our 'Spark Code' which causes maintenance issues.

With all of this in mind, it is in fact very possible to run production
pipelines using this scheme (ask me how I know :wink:). However, with the advent
of [Databricks
Connect](https://docs.databricks.com/dev-tools/databricks-connect.html), and
more generally [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html), we can simplify our
codebase and reduce the heavy lifting required to integrate Spark in our
pipelines.


## Spinning up a Databricks Spark Cluster

In order to make use of a Databricks Spark cluster, we'll need to create one.
Once we create our cluster, we can terminate it so that it incurs no cost while
we're not using it. Conveniently, the Databricks Connect library will start up
our cluster if it's not running when we batch a job to it from our pipeline, so
we don't need to handle that explicitly in code.

In a production scenario, we would try to schedule most of our jobs utilizing
the same cluster to run in the same time window and we configure the cluster to
terminate automatically after some amount of idle time. Since having to restart the
cluster for each run will increase time and cost, this setup can result in more
streamlined pipelines for regularly scheduled runs.

For this demo, we can create a fairly slim single node cluster, since our
pipeline code will be the same regardless of if we swap out a cluster with more
resources (which is pretty awesome).

A few things we do want to make sure of when creating our cluster:
* Our Spark version should be 3.3.0, which at time of writing is the 11.3 LTS
  runtime. This is due to a compatibility with the Databricks Connect library,
  as well as iceberg, which at the time of writing does not yet support Spark
  3.4.0
* We want to make sure to set the `Terminate After` value to be something
  short, in our case 10 minutes, to ensure we aren't incurring additional cost
  as the cluster sits idle.
* Finally, we want to make sure that we set the following spark config
  `spark.databricks.service.server.enabled true`. This is required for
  Databricks Connect to be enabled on the cluster for remote execution.

![databricks-cluster](/assets/images/2023-05-02-moving-local-spark-jobs-to-azure-databricks/databricks-cluster.png)

You can create the cluster either using the UI, or through the API ([docs
here](https://docs.databricks.com/api-explorer/workspace/clusters/create)). A
sample payload for the API might look like

```json
{
    "num_workers": 0,
    "cluster_name": "MTADemo_SingleNode",
    "spark_version": "11.3.x-scala2.12",
    "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode",
        "spark.databricks.service.server.enabled": "true"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_F4",
    "driver_node_type_id": "Standard_F4",
    "ssh_public_keys": [],
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "spark_env_vars": {},
    "autotermination_minutes": 10,
    "enable_elastic_disk": true,
    "cluster_source": "UI",
    "init_scripts": [],
    "single_user_name": <your-databricks-username>,
    "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
    "runtime_engine": "STANDARD"
}
```

After creation, you'll want to also add the following Maven libraries (which
can be done in the `Libraries` tab on the Cluster UI, or through the API ([docs
here](https://docs.databricks.com/api-explorer/workspace/libraries/install))
* `org.apache.hadoop:hadoop-azure:3.3.0`
* `org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.1`

![databricks-cluster-libraries](/assets/images/2023-05-02-moving-local-spark-jobs-to-azure-databricks/databricks-cluster-libraries.png)

To be clear, a cluster only needs to be created once, whether through the API
or through the UI, and can then be started/terminated/used as many times as
needed.


## Testing Our New Cluster


With our cluster created, let's test that we can submit a job to it before
incorporating the transformation into the pipeline. We'll assume that we have a
Bridge & Tunnel Traffic blob sitting in Azure to test with, as shown in [Bridge
& Tunnel Data 4 Ways]({% post_url 2023-04-21-bridgeNtunnel-data-4-ways %}). As a starting point, we can
use our `iceberg_loader.py` script from [Adding Spark Task]({% post_url
2023-04-27-prefect-local-pipeline %}#adding-spark-task).

Really the only thing we need to change code-wise is to add a few more spark
configs, and add the new arguments to be passed. Our new test script should
look like

{% highlight python %}
{% include
code/2023-05-02-moving-local-spark-jobs-to-azure-databricks/iceberg_loader_databricks.py
%}
{% endhighlight %}

In order to run this, we'll need to ensure the databricks connect library is
installed. The library version is strongly tied to the databricks runtime of
the cluster being used, so in this case we want version `11.3.*`. On my machine
at least (a mac) I also need to drop an empty json object in the
`~/.databricks-connect` file, or else the code will prompt me to interactively
input some configs there, which we don't want.

```bash
venv/bin/python -m pipe uninstall pyspark
venv/bin/python -m pip install "databricks-connect==11.3.*"
echo '{}' > ~/.databricks-connect
```

With our environment set up, we can now kick off our test script and confirm
the connections between our local env, our databricks cluster, and our azure
storage.

```bash
venv/bin/python iceberg_loader_databricks.py \
    --storage-account <your-storage-account> \
    --container mta-demo \
    --access-key <your-access-key> \
    --blob-name stage/bnt_traffic_hourly.json
    --databricks-address <your-databricks-address> \
    --databricks-token <your-databricks-token> \
    --databricks-cluster-id <your-cluster-id>
```

One thing to notice is that we no longer need to run this script with
`spark-submit`, but rather can execute it natively from our python interpreter.
This will make things easier later when we want to embed this code in our
pipeline.


## Integrating with Our Pipeline
With all the circuits checked, we can now update our pipeline tasks to use
databricks connect rather than spark-submit subprocesses.

Essentially, the changes are just to replace our
`load_azure_blob_to_iceberg_table` task with two new spark-powered tasks,
`create_bnt_iceberg_table` and `append_bnt_blob_to_iceberg`.

```python
def get_spark(configs: Dict[str, str]) -> SparkSession:
    builder = SparkSession.builder
    for key, val in configs.items():
        builder = builder.config(key, val)
    spark = builder.getOrCreate()
    return spark


@task
def create_bnt_iceberg_table(
    spark_configs: Dict[str, str],
    table_name: str,
):
    spark = get_spark(spark_configs)

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


@task
def append_bnt_blob_to_iceberg(
    spark_configs: Dict[str, str],
    table_name: str,
    blob_uri: str,
):
    spark = get_spark(spark_configs)

    df = spark.read.json(blob_uri)

    df = (
        df.withColumn("date", F.to_date(df["date"]))
        .withColumn("vehicles_e_zpass", df["vehicles_e_zpass"].cast("int"))
        .withColumn("vehicles_vtoll", df["vehicles_vtoll"].cast("int"))
    )

    df.writeTo(table_name).append()
```

By breaking these two processes up, we can schedule them more appropriately in
our pipeline.

```python
@flow
def pipeline(
    start_date: datetime.date,
    stop_date: datetime.date,
    storage_account: str,
    access_key: str,
    container: str,
    databricks_address: str,
    databricks_token: str,
    databricks_cluster_id: str,
):
    container_uri = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    blob_name = "stage/bnt_traffic_hourly.json"
    catalog_name = "mtademo"
    table_name = f"{catalog_name}.bronze.bnt_traffic_hourly"

    spark_configs = {
        "spark.databricks.service.address": databricks_address,
        "spark.databricks.service.token": databricks_token,
        "spark.databricks.service.clusterId": databricks_cluster_id,
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net": access_key,
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.type": "hadoop",
        f"spark.sql.catalog.{catalog_name}.warehouse": f"{container_uri}/warehouse",
    }

    data = get_bnt_data.submit(
        start_date,
        stop_date,
    )

    table_created = create_bnt_iceberg_table.submit(
        spark_configs,
        table_name,
    )

    blob_loaded = load_to_azure_blob.submit(
        data,
        storage_account,
        access_key,
        container,
        blob_name,
        wait_for=[data],
    )

    append_bnt_blob_to_iceberg.submit(
        spark_configs,
        table_name,
        f"{container_uri}/{blob_name}",
        wait_for=[table_created, blob_loaded],
    )
```

By making use of `.submit` to execute our tasks, Prefect will make use of the
`ConcurrentTaskRunner`, and execute our various tasks concurrently. However, we
do have dependencies between our tasks, for example, we can't append our blob
to our iceberg table until we've madde sure that our table has been created. To
handle dependencies, we pass the `wait_for` argument in our submit call, where
the argument should be the list of *task futures* (basically the object
returned by calling `.submit` on a task) that this task is dependent on.

The above pipeline gives us a dependency graph that looks like

<script type="module">
  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
</script>
{% mermaid %}
graph TD;
    get_bnt_data-->load_to_azure_blob;
    create_bnt_iceberg_table-->append_bnt_blob_to_iceberg;
    load_to_azure_blob-->append_bnt_blob_to_iceberg;
{% endmermaid %}

Finally, we can execute our pipeline and confirm it runs successfully.

![pipeline-success](/assets/images/2023-05-02-moving-local-spark-jobs-to-azure-databricks/pipeline-success.png)

In the next post, we'll improve our pipeline orchestration by making use of the
Prefect Server, Blocks, and Deployments.

*Final pipeline*
{% highlight python %}
{% include
code/2023-05-02-moving-local-spark-jobs-to-azure-databricks/pipeline.py
%}
{% endhighlight %}

