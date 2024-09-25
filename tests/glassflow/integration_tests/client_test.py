def test_get_pipeline_ok(client):
    pipeline_id = "bdbbd7c4-6f13-4241-b0b6-da142893988d"

    p = client.get_pipeline(pipeline_id="bdbbd7c4-6f13-4241-b0b6-da142893988d")
    assert p.id == pipeline_id
