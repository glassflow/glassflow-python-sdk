# GlassFlow Python SDK

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/your-username/glassflow-py-sdk/blob/main/LICENSE)

The GlassFlow Python SDK provides a convenient way to interact with the GlassFlow API in your Python applications. It can be used to publish and consume events to your GlassFlow pipelines

## Installation

You can install the GlassFlow Python SDK using pip:

```shell
pip install glassflow
```

## Available Operations

* [publish](#publish) - Publish a new event into the pipeline
* [consume](#consume) - Consume the transformed event from the pipeline


## publish

Publish a new event into the pipeline

### Example Usage

```python
import glassflow

client = glassflow.GlassFlowClient()
pipeline_client = client.pipeline_client(space_id="<str value>", pipeline_id="<str value")
data = {} # your json event
req = pipeline_client.publish(request_body=data, pipeline_access_token="<str token>")

if res.status_code == 200:
    print("Published sucessfully")
```


## consume

Consume the transformed event from the pipeline

### Example Usage

```python
import glassflow

client = glassflow.GlassFlowClient()
pipeline_client = client.pipeline_client(space_id="<str value>", pipeline_id="<str value")
data = {} # your json event
res = pipeline_client.consume(pipeline_access_token="<str value>")

if res.status_code == 200:
    print(res.body.event)
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
