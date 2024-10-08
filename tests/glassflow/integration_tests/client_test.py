def test_get_pipeline_ok(client, creating_pipeline):
    p = client.get_pipeline(pipeline_id=creating_pipeline.id)

    assert p.id == creating_pipeline.id
    assert p.name == creating_pipeline.name


def test_list_pipelines_ok(client, creating_pipeline):
    res = client.list_pipelines()

    assert res.status_code == 200
    assert res.content_type == "application/json"
    assert res.total_amount >= 1
    assert res.pipelines[-1]["id"] == creating_pipeline.id
    assert res.pipelines[-1]["name"] == creating_pipeline.name
