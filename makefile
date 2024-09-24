
generate-api-data-models:
	datamodel-codegen  \
		--url https://api.glassflow.dev/v1/openapi.yaml \
		--output ./src/glassflow/models/api/api.py
	sed -i '' -e '1s/^/# ruff: noqa\n/' ./src/glassflow/models/api/api.py
