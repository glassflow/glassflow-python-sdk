# Welcome to GlassFlow Python SDK Docs

The [GlassFlow](https://www.glassflow.dev/) Python SDK provides a convenient way to interact with the GlassFlow API in your Python applications. The SDK is used to publish and consume events to your [GlassFlow pipelines](https://learn.glassflow.dev/docs/concepts/pipeline-configuration).


## Installation

You can install the GlassFlow Python SDK using pip:

```shell
pip install glassflow
```

## Available Operations

* [publish](#publish) - Publish a new event into the pipeline
* [consume](#consume) - Consume the transformed event from the pipeline
* [consume failed](#consume-failed) - Consume the events that failed from the pipeline
* [validate credentials](#validate-credentials) - Validate pipeline credentials


## publish

Publish a new event into the pipeline

### Example Usage

```python
from glassflow import PipelineDataSource

source = PipelineDataSource(pipeline_id="<str value", pipeline_access_token="<str token>")
data = {} # your json event
res = source.publish(request_body=data)

if res.status_code == 200:
    print("Published sucessfully")
```



## consume

Consume the transformed event from the pipeline

### Example Usage

```python
from glassflow import PipelineDataSink

sink = PipelineDataSink(pipeline_id="<str value", pipeline_access_token="<str value>")
res = sink.consume()

if res.status_code == 200:
    print(res.json())
```


## consume failed

If the transformation failed for any event, they are available in a failed queue. You can consume those events from the pipeline

### Example Usage

```python
from glassflow import PipelineDataSink

sink = PipelineDataSink(pipeline_id="<str value", pipeline_access_token="<str value>")
res = sink.consume_failed()

if res.status_code == 200:
    print(res.json())
```

## validate credentials

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

## SDK Maturity

Please note that the GlassFlow Python SDK is currently in beta and is subject to potential breaking changes. We recommend keeping an eye on the official documentation and updating your code accordingly to ensure compatibility with future versions of the SDK.


## User Guides

For more detailed information on how to use the GlassFlow Python SDK, please refer to the [GlassFlow Documentation](https://learn.glassflow.dev). The documentation provides comprehensive guides, tutorials, and examples to help you get started with GlassFlow and make the most out of the SDK.

## Contributing

Anyone who wishes to contribute to this project, whether documentation, features, bug fixes, code cleanup, testing, or code reviews, is very much encouraged to do so.

Just raise your hand on the GitHub [discussion](https://github.com/glassflow/glassflow-python-sdk/discussions) board.

If you are unfamiliar with how to contribute to GitHub projects, here is a [Get Started Guide](https://docs.github.com/en/get-started/quickstart/contributing-to-projects). A full set of contribution guidelines, along with templates, are in progress.

## Troubleshooting

For any questions, comments, or additional help, please reach out to us via email at [help@glassflow.dev](mailto:help@glassflow.dev).
Please check out our [Q&A](https://github.com/glassflow/glassflow-python-sdk/discussions/categories/q-a) to get solutions for common installation problems and other issues.

### Raise an issue

To provide feedback or report a bug, please [raise an issue on our issue tracker](https://github.com/glassflow/glassflow-python-sdk/issues).
