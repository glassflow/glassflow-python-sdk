def test_get_pipeline_ok(client, creating_pipeline):
    p = client.get_pipeline(pipeline_id=creating_pipeline.id)

    assert p.id == creating_pipeline.id
    assert p.name == creating_pipeline.name


def test_list_pipelines_ok(client, creating_pipeline):
    res = client.list_pipelines()

    assert res.total_amount >= 1
    assert res.pipelines[-1].id == creating_pipeline.id
    assert res.pipelines[-1].name == creating_pipeline.name


def test_list_spaces_ok(client, creating_space):
    res = client.list_spaces()

    assert res.total_amount >= 1
    assert res.spaces[-1].id == creating_space.id
    assert res.spaces[-1].name == creating_space.name


def test_list_secrets_ok(client, creating_secret):
    res = client.list_secrets()

    assert res.total_amount >= 1
    assert res.secrets[-1].key == creating_secret.key
