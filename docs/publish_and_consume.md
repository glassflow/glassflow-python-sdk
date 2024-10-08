# Publish and Consume Events


* [Publish](#publish) - Publish a new event into the pipeline from a data source
* [Consume](#consume) - Consume the transformed event from the pipeline in a data sink
* [Consume Failed](#consume-failed) - Consume the events that failed from the pipeline in a 
* [Validate Credentials](#validate-credentials) - Validate pipeline credentials


## Publish

Publish a new event into the pipeline

### Example Usage

```python
from glassflow import PipelineDataSource

source = PipelineDataSource(pipeline_id="<str value", pipeline_access_token="<str token>")
data = {} # your json event
res = source.publish(request_body=data)

if res.status_code == 200:
    print("Published successfully")
```


## Consume

Consume the transformed event from the pipeline

### Example Usage

```python
from glassflow import PipelineDataSink

sink = PipelineDataSink(pipeline_id="<str value", pipeline_access_token="<str value>")
res = sink.consume()

if res.status_code == 200:
    print(res.json())
```


## Consume Failed

If the transformation failed for any event, they are available in a failed queue. You can consume those events from the pipeline

### Example Usage

```python
from glassflow import PipelineDataSink

sink = PipelineDataSink(pipeline_id="<str value", pipeline_access_token="<str value>")
res = sink.consume_failed()

if res.status_code == 200:
    print(res.json())
```


## Validate Credentials

Validate pipeline credentials (`pipeline_id` and `pipeline_access_token`) from source or sink

### Example Usage

```python
from glassflow import PipelineDataSource, errors

try:
    source = PipelineDataSource(pipeline_id="<str value", pipeline_access_token="<str value>")
    source.validate_credentials()
except errors.PipelineNotFoundError as e:
    print("Pipeline ID does not exist!")
    raise e
except errors.PipelineAccessTokenInvalidError as e:
    print("Pipeline Access Token is invalid!")
    raise e
```
