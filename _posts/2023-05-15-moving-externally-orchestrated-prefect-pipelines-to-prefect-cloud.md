---
title: Moving Externally Orchestrated Prefect Pipelines to Prefect Cloud
date: 2023-05-15 15:20 -0400
tags: Tech MTA Prefect Cloud
---

One of the things that makes Prefect such a great data pipeline framework (in
my opinion) is the ability to write a pipeline as a standalone piece of code,
and execute it independently of any built in scheduler or larger framework.

This is great for two reasons:
1. Local testing becomes immensely easier.
2. Integration with existing systems becomes possible without having to
   interface with some Prefect scheudler.
   
For those who might come from the
[Luigi](https://luigi.readthedocs.io/en/stable/) world, these might seem
obvious, but since many folks these days jumped right into Airflow, I find the
second point above is not always appreciated as much as it might be.

A nice part of building pipelines in Luigi was that we were always able to
separate our internal task dependency logic (i.e inside the pipeline) from our
pipeline scheduling or execution logic. This has the added benefit of making
the transition from some other pipeline framework more painless, since we can
first rewrite our pipeline as Luigi (or Prefect) pipeline (or Flow), and swap
it in using the existing scheduler. Once we've sufficiently moved our pipeline
code to the new framework, we can handle swapping out the orchestration (if we
so desire.)

Similar to Luigi, Prefect allows one to
handle these two aspects -- intra-flow task dependencies and flow orchestration
-- totally separately at the framework level. Even better than Luigi though,
Prefect fills in the gaps that many of these other frameworks lack.

While this is a subtle point to some, I hope other engineers that have had to
perform large scale framework transitions of sprawling data estates appreciate
the power that this kind of decoupling brings.

## Prefect Cloud
In our last [posts]({%post_url
2023-05-02-moving-local-spark-jobs-to-azure-databricks %}), we built a pipeline using Prefect that simulated an ELT
process for [Bridge & Tunnel Traffic Data](). Conveniently, we didn't need to
spend any time thinking about how we would orchestrate the execution of our
pipeline while building it, and that's a good thing.

With our pipeline built and tested, it's now an appropriate question to ask how
it is we plan to schedule and orchestate this pipeline, and others like it. For
this, we have essentially two options:
1. [Prefect Server]()
2. [Prefect Cloud]()

Essentially, these are the same, however Prefect Server is the open source
scheduling server, and Prefect Cloud is the managed offering by PrefectHQ.
While I would normally suggest starting first with the open source offering and
then upgrading to the managed service if/when needed, Prefect Cloud's free tier
is actually pretty fully featured. For any enterprise, you'll certainly need to
upgrade to a paid option if you choose not to self-host the Prefect Server, but
for a data engineer validating the framework or building a POC, Prefect Cloud
can save a lot of time during the early stages of onboarding.

Having created our free Prefect Cloud instance, we'll want to create a new
workspace (we can call this `mtademo`), grab an API key, and log in from our
development machine with

```bash
venv/bin/prefect cloud login
```

From here, we can run our existing pipeline exactly as before

```bash
venv/bin/python pipeline.py \
    --storage-account <your-storage-account> \
    --container mta-demo \
    --access-key <your-access-key> \
    --databricks-address <your-databricks-address> \
    --databricks-token <your-databricks-token> \
    --databricks-cluster-id <your-cluster-id> \
    --start-date 2019-01-01 \
    --stop-date 2023-01-01
```
 which should execute exactly as previously, however now the run should be
 viewable on the UI (under 'Flow Runs').

![local-pipeline-UI](/assets/images/2023-05-15-moving-externally-orchestrated-prefect-pipelines-to-prefect-cloud/local-pipeline-UI.png)

While the prefect server didn't have any control over the run, you can think of
this as the server simply "knowing about" the run. Because of this, from the UI
you won't have any control over executing the run again or scheduling it for a
future run.

In order to hand over the orchestration of the pipeline to the server, we'll
need to 'deploy' the flow. First though, we'll take a small detour to set up
some Blocks, or stored configs, that will simplify our execution.

### Moving configs to Blocks
Blocks are simply structured configs, stored in the Prefect Server database,
that are available to flows at runtime. 

We'll use a number of predefined Blocks (mostly to store base level credentials
that might be used in multiple other blocks), and compose these with some
custom Blocks.

We'll create a new file in our module `mtademo/blocks.py`, and add the
following Block defintions

{% highlight python %}
{% include
code/2023-05-15-moving-externally-orchestrated-prefect-pipelines-to-prefect-cloud/blocks.py
%}
{% endhighlight %}

We can then register these blocks with the UI with the command
```bash
venv/bin/prefect block register --file mtaprefect/blocks.py
```

With these registered, we should now see them on the API, and we can begin
configuring the Blocks.

![azure-credentials-block](/assets/images/2023-05-15-moving-externally-orchestrated-prefect-pipelines-to-prefect-cloud/azure-credentials-block.png)

![azure-container-block](/assets/images/2023-05-15-moving-externally-orchestrated-prefect-pipelines-to-prefect-cloud/azure-container-block.png)

![iceberg-block](/assets/images/2023-05-15-moving-externally-orchestrated-prefect-pipelines-to-prefect-cloud/iceberg-block.png)

![databricks-credentials-block](/assets/images/2023-05-15-moving-externally-orchestrated-prefect-pipelines-to-prefect-cloud/databricks-credentials-block.png)

![databricks-cluster-block](/assets/images/2023-05-15-moving-externally-orchestrated-prefect-pipelines-to-prefect-cloud/databricks-cluster-block.png)

