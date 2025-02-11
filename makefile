API_DATA_MODELS = ./src/glassflow/models/api/api.py

generate-api-data-models:
	datamodel-codegen  \
		--url https://api.glassflow.dev/v1/openapi.yaml \
		--output $(API_DATA_MODELS)

add-noqa: generate-api-data-models
	echo "Add noqa comment ..."
	sed -i '' -e '1s/^/# ruff: noqa\n/' $(API_DATA_MODELS)

generate: add-noqa

include .env
export
checks: lint formatter test

test:
	pytest tests

lint:
	ruff check .

formatter:
	ruff format --check .

fix-format:
	ruff format .

fix-lint:
	ruff check --fix .

fix: fix-format fix-lint