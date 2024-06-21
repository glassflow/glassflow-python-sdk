<div align="center">
  <img src="https://learn.glassflow.dev/~gitbook/image?url=https:%2F%2F3630921082-files.gitbook.io%2F%7E%2Ffiles%2Fv0%2Fb%2Fgitbook-x-prod.appspot.com%2Fo%2Fspaces%252FpRyi93X0Jn9wrh2Z4Ffm%252Flogo%252Fj4ZLY66JC4CCI0kp4Tcl%252FBlue.png%3Falt=media%26token=824ab2c7-e9a7-4b53-bd9a-375650951fc1&width=128&dpr=2&quality=100&sign=312af88abf1a93b897726483f4d86c2733192ab70b94b68ba438f6c85caf7e1a" /><br /><br />
</div>
<p align="center">
        <a href="https://github.com/glassflow/glassflow-python-sdk/blob/main/LICENSE.md">
        <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"/></a>
<a href="https://join.slack.com/t/glassflowhub/shared_invite/zt-2g3s6nhci-bb8cXP9g9jAQ942gHP5tqg">
        <img src="https://img.shields.io/badge/slack-join-community?logo=slack&amp;logoColor=white&amp;style=flat"
            alt="Chat on Slack"></a>

# GlassFlow Python SDK

The [GlassFlow](https://www.glassflow.dev/) Python SDK provides a convenient way to interact with the GlassFlow API in your Python applications. The SDK is used to publish and consume events to your [GlassFlow pipelines](https://learn.glassflow.dev/docs/concepts/pipeline-configuration).

See how it is easy to setup a new data pipeline and do data transformation with GlassFlow:

![GlassFlow data pipeline creation](/assets/GlassFlow%20quickstart.gif)

## Installation

You can install the GlassFlow Python SDK using pip:

```shell
pip install glassflow
```

## Available Operations

* [publish](#publish) - Publish a new event into the pipeline
* [consume](#consume) - Consume the transformed event from the pipeline
* [consume failed](#consume-failed) - Consume the events that failed from the pipeline


## publish

Publish a new event into the pipeline

### Example Usage

```python
import glassflow

client = glassflow.GlassFlowClient()
pipeline_client = client.pipeline_client(pipeline_id="<str value", pipeline_access_token="<str value>")
data = {} # your json event
res = pipeline_client.publish(request_body=data)

if res.status_code == 200:
    print("Published sucessfully")
```


## consume

Consume the transformed event from the pipeline

### Example Usage

```python
import glassflow

client = glassflow.GlassFlowClient()
pipeline_client = client.pipeline_client(pipeline_id="<str value", pipeline_access_token="<str value>")
res = pipeline_client.consume()

if res.status_code == 200:
    print(res.body.event)
```

## consume failed

If the transformation failed for any event, they are available in a failed queue. You can consume those events from the pipeline

### Example Usage

```python
import glassflow

client = glassflow.GlassFlowClient()
pipeline_client = client.pipeline_client(pipeline_id="<str value", pipeline_access_token="<str value>")
res = pipeline_client.consume_failed()

if res.status_code == 200:
    print(res.body.event)
```


## Quickstart

Follow the quickstart guide [here](https://learn.glassflow.dev/docs/get-started/quickstart)

## Code Samples

[GlassFlow Examples](https://github.com/glassflow/glassflow-examples)

## SDK Maturity

Please note that the GlassFlow Python SDK is currently in beta and is subject to potential breaking changes. We recommend keeping an eye on the official documentation and updating your code accordingly to ensure compatibility with future versions of the SDK.


## User Guides

For more detailed information on how to use the GlassFlow Python SDK, please refer to the [GlassFlow Documentation]([https://learn.glassflow.dev](https://learn.glassflow.dev/docs)). The documentation provides comprehensive guides, tutorials, and examples to help you get started with GlassFlow and make the most out of the SDK.

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
