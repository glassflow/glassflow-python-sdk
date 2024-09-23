<div align="center">
  <img src="https://docs.glassflow.dev/~gitbook/image?url=https%3A%2F%2F1082326815-files.gitbook.io%2F%7E%2Ffiles%2Fv0%2Fb%2Fgitbook-x-prod.appspot.com%2Fo%2Forganizations%252FaR82XtsD8fLEkzPmMtb7%252Fsites%252Fsite_8vNM9%252Flogo%252Fft4nLD8mKhRmqTJjDp3i%252Flogo-color.png%3Falt%3Dmedia%26token%3Deb19e3bf-195b-413f-9965-4c76112953a3&width=128&dpr=3&quality=100&sign=10efaa8d&sv=1" /><br /><br />
</div>
<p align="center">
        <a href="https://github.com/glassflow/glassflow-python-sdk/blob/main/LICENSE.md">
        <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"/></a>
<a href="https://join.slack.com/t/glassflowhub/shared_invite/zt-2g3s6nhci-bb8cXP9g9jAQ942gHP5tqg">
        <img src="https://img.shields.io/badge/slack-join-community?logo=slack&amp;logoColor=white&amp;style=flat"
            alt="Chat on Slack"></a>
<a href="https://github.com/astral-sh/ruff"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json" alt="Ruff" style="max-width:100%;"></a>
<!-- Pytest Coverage Comment:Begin -->
<!-- Pytest Coverage Comment:End -->


# GlassFlow Python SDK

The [GlassFlow](https://www.glassflow.dev/) Python SDK provides a convenient way to interact with the GlassFlow API in your Python applications. The SDK is used to publish and consume events to your [GlassFlow pipelines](https://docs.glassflow.dev/concepts/pipeline).

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
import glassflow

pipeline_source = glassflow.PipelineDataSource(pipeline_id="<str value", pipeline_access_token="<str token>")
data = {} # your json event
res = pipeline_source.publish(request_body=data)

if res.status_code == 200:
    print("Published sucessfully")
```



## consume

Consume the transformed event from the pipeline

### Example Usage

```python
import glassflow

pipeline_sink = glassflow.PipelineDataSink(pipeline_id="<str value", pipeline_access_token="<str value>")
res = pipeline_sink.consume()

if res.status_code == 200:
    print(res.json())
```


## consume failed

If the transformation failed for any event, they are available in a failed queue. You can consume those events from the pipeline

### Example Usage

```python
import glassflow

pipeline_sink = glassflow.PipelineDataSink(pipeline_id="<str value", pipeline_access_token="<str value>")
res = pipeline_sink.consume_failed()

if res.status_code == 200:
    print(res.json())
```

## validate credentials

Validate pipeline credentials (`pipeline_id` and `pipeline_access_token`) from source or sink

### Example Usage

```python
import glassflow

try:
    pipeline_source = glassflow.PipelineDataSource(pipeline_id="<str value", pipeline_access_token="<str value>")
    pipeline_source.validate_credentials()
except glassflow.errors.PipelineNotFoundError as e:
    print("Pipeline ID does not exist!")
    raise e
except glassflow.errors.PipelineAccessTokenInvalidError as e:
    print("Pipeline Access Token is invalid!")
    raise e
```


## Quickstart

Follow the quickstart guide [here](https://docs.glassflow.dev/get-started/quickstart)

## Code Samples

[GlassFlow Examples](https://github.com/glassflow/glassflow-examples)

## SDK Maturity

Please note that the GlassFlow Python SDK is currently in beta and is subject to potential breaking changes. We recommend keeping an eye on the official documentation and updating your code accordingly to ensure compatibility with future versions of the SDK.


## User Guides

For more detailed information on how to use the GlassFlow Python SDK, please refer to the [GlassFlow Documentation](https://docs.glassflow.dev). The documentation provides comprehensive guides, tutorials, and examples to help you get started with GlassFlow and make the most out of the SDK.

## Contributing

Anyone who wishes to contribute to this project, whether documentation, features, bug fixes, code cleanup, testing, or code reviews, is very much encouraged to do so.

1. Join the [Slack channel](https://join.slack.com/t/glassflowhub/shared_invite/zt-2g3s6nhci-bb8cXP9g9jAQ942gHP5tqg).

2. Just raise your hand on the GitHub [discussion](https://github.com/glassflow/glassflow-python-sdk/discussions) board.

If you are unfamiliar with how to contribute to GitHub projects, here is a [Get Started Guide](https://docs.github.com/en/get-started/quickstart/contributing-to-projects). A full set of contribution guidelines, along with templates, are in progress.

## Troubleshooting

For any questions, comments, or additional help, please reach out to us via email at [help@glassflow.dev](mailto:help@glassflow.dev).
Please check out our [Q&A](https://github.com/glassflow/glassflow-python-sdk/discussions/categories/q-a) to get solutions for common installation problems and other issues.

### Raise an issue

To provide feedback or report a bug, please [raise an issue on our issue tracker](https://github.com/glassflow/glassflow-python-sdk/issues).
