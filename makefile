API_DATA_MODELS = ./src/glassflow/models/api/api.py

generate-api-data-models:
	datamodel-codegen  \
		--url https://api.glassflow.dev/v1/openapi.yaml \
		--output $(API_DATA_MODELS)

add-noqa: generate-api-data-models
	echo "Add noqa comment ..."
	sed -i '' -e '1s/^/# ruff: noqa\n/' $(API_DATA_MODELS)


add-dataclass-json-decorators: add-noqa
	echo "Import dataclass_json ..."
	sed -i '' -e '/^from __future__ import annotations/a\'$$'\n''from dataclasses_json import dataclass_json' $(API_DATA_MODELS)


	echo "Add dataclass_json decorators ..."
	sed  -i '' -e '/@dataclass/ i\'$$'\n''@dataclass_json\''' $(API_DATA_MODELS)

generate: add-dataclass-json-decorators

include .env
export
checks: lint formatter test

test:
	pytest tests

lint:
	ruff check .

formatter:
	ruff format --check .
