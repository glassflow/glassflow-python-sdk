```python
import glassflow

source = glassflow.PipelineDataSource(pipeline_id="<pipeline_id>", pipeline_access_token="<pipeline_token>")
data = {
    "name": "Hello World",
    "id": 1
}
source_res = source.publish(request_body=data)

sink = glassflow.PipelineDataSink(pipeline_id="<pipeline_id>", pipeline_access_token="<pipeline_token>")
sink_res = sink.consume()
```