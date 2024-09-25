def test_get_pipeline_ok(client):
    pipeline_id = "bdbbd7c4-6f13-4241-b0b6-da142893988d"

    p = client.get_pipeline(pipeline_id="bdbbd7c4-6f13-4241-b0b6-da142893988d")
    assert p.id == pipeline_id


def test_list_pipelines_ok(client, creating_pipeline):
    res = client.list_pipelines()

    assert res.status_code == 200
    assert res.content_type == "application/json"
    assert res.total_amount >= 1
    assert res.pipelines[-1]["id"] == creating_pipeline.id
    assert res.pipelines[-1]["name"] == creating_pipeline.name
