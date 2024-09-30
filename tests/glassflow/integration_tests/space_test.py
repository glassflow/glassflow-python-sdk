
def test_create_space_ok(creating_space):
    assert creating_space.name == "integration-tests"
    assert creating_space.id is not None
