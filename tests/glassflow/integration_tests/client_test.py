
def test_get_pipeline(client):
    pipeline_id = "bdbbd7c4-6f13-4241-b0b6-da142893988d"

    pipeline = client.get_pipeline(pipeline_id="bdbbd7c4-6f13-4241-b0b6-da142893988d")

    assert pipeline.id == pipeline_id
