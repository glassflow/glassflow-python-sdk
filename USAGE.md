```python
import glassflow

client = glassflow.GlassFlowClient()
pipeline = client.pipeline_client(pipeline_id="<pipeline_id>", pipeline_access_token="<pipeline_token>")
data = {
    "name": "Hello World",
    "id": 1
}
res = pipeline.publish(request_body=data)
```