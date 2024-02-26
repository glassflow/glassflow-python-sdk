# GlassFlow Python SDK

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/your-username/glassflow-py-sdk/blob/main/LICENSE)

The GlassFlow Python SDK provides a convenient way to interact with the GlassFlow API in your Python applications. It can be used to publish and consume events to your GlassFlow pipelines

## Installation

You can install the GlassFlow Python SDK using pip:

```shell
pip install glassflow
```

## SDK Example Usage

```python
import glassflow

client = glassflow.GlassFlowClient()
pipeline = client.pipeline_client(space_id="<space_id>", pipeline_id="<pipeline_id>")
pipeline_access_token="<pipeline_token>"
data = {
    "name": "Hello World",
    "id": 1
}
res = pipeline.publish(request_body=data,pipeline_access_token=pipeline_access_token)
```

## SDK Maturity

Please note that the GlassFlow Python SDK is currently in beta and is subject to potential breaking changes. We recommend keeping an eye on the official documentation and updating your code accordingly to ensure compatibility with future versions of the SDK.


## User Guides

For more detailed information on how to use the GlassFlow Python SDK, please refer to the [GlassFlow Documentation](https://learn.glassflow.dev). The documentation provides comprehensive guides, tutorials, and examples to help you get started with GlassFlow and make the most out of the SDK.

For any questions, comments or additional help, please reach out to us via email at [help@glassflow.dev](mailto:help@glassflow.dev).
