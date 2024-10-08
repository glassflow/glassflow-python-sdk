# Pipeline and Space Management

In order to manage your pipelines and spaces with this SDK, you need to provide the `PERSONAL_ACCESS_TOKEN` 
to the GlassFlow client.

```python
from glassflow import GlassFlowClient

client = GlassFlowClient(personal_access_token="<your personal access token>")
```

Here is a list of operations you can do with the `GlassFlowClient`:

* [List Pipelines](#list-pipelines) - Returns a list with all your pipelines
* [Get Pipeline](#get-pipeline) - Returns a pipeline object from a given pipeline ID
* [Create Pipeline](#create-pipeline) - Create a new pipeline
* [List Spaces](#list-spaces) - Returns a list with all your spaces
* [Create Space](#create-space) - Create a new space

You can also interact directly with the `Pipeline` or `Space` objects. They
allow for some extra functionalities like delete or update.

* [Update Pipeline](#update-pipeline) - Update an existing pipeline
* [Delete Pipeline](#delete-pipeline) - Delete an existing pipeline
* [Delete Space](#delete-space) - Delete an existing pipeline

## List Pipelines

Returns information about the available pipelines. It can be restricted to a
specific space by passing the `space_id`.

### Example Usage

```python
from glassflow import GlassFlowClient

client = GlassFlowClient(personal_access_token="<your access token>")
res = client.list_pipelines()
```

## Get Pipeline

Gets information about a pipeline from a given pipeline ID. It returns a Pipeline object
which can be used manage the Pipeline. 

### Example Usage

```python
from glassflow import GlassFlowClient

client = GlassFlowClient(personal_access_token="<your access token>")
pipeline = client.get_pipeline(pipeline_id="<your pipeline id>")

print("Name:", pipeline.name)
```

## Create Pipeline

Creates a new pipeline and returns a `Pipeline` object.

### Example Usage

```python
from glassflow import GlassFlowClient

client = GlassFlowClient(personal_access_token="<your access token>")
pipeline = client.create_pipeline(
    name="MyFirstPipeline",
    space_id="<your space id>",
    transformation_file="path/to/transformation.py"
)

print("Pipeline ID:", pipeline.id)
```

In the next example we create a pipeline with Google PubSub source 
and a webhook sink:

```python
from glassflow import GlassFlowClient

client = GlassFlowClient(personal_access_token="<your access token>")

pipeline = client.create_pipeline(
    name="MyFirstPipeline",
    space_id="<your space id>",
    transformation_file="path/to/transformation.py",
    source_kind="google_pubsub",
    source_config={
        "project_id": "<your gcp project id>",
        "subscription_id": "<your subscription id>",
        "credentials_json": "<your credentials json>"
    },
    sink_kind="webhook",
    sink_config={
        "url": "www.my-webhook-url.com",
        "method": "POST",
        "headers": [{"header1": "header1_value"}]
    }
)
```

## Update Pipeline

The Pipeline object has an update method.

### Example Usage

```python
from glassflow import Pipeline

pipeline = Pipeline(
    id="<your pipeline id>",
    personal_access_token="<your access token>",
)

pipeline.update(
    transformation_file="path/to/new/transformation.py",
    name="NewPipelineName",
)
```

## Delete Pipeline

The Pipeline object has a delete method to delete a pipeline

### Example Usage

```python
from glassflow import Pipeline

pipeline = Pipeline(
    id="<your pipeline id>",
    personal_access_token="<your access token>"
)
pipeline.delete()
```

## List Spaces

Returns information about the available spaces.

### Example Usage

```python
from glassflow import GlassFlowClient

client = GlassFlowClient(personal_access_token="<your access token>")
res = client.list_spaces()
```


## Create Space

Creates a new space and returns a `Space` object.

```python
from glassflow import GlassFlowClient

client = GlassFlowClient(personal_access_token="<your access token>")
space = client.create_space(name="MyFirstSpace")
```


## Delete Space

The Space object has a delete method to delete a space

### Example Usage

```python
from glassflow import Space

space = Space(
    id="<your space id>", 
    personal_access_token="<your access token>"
)
space.delete()
```