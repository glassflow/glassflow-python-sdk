
generate-api-data-models:
	datamodel-codegen  \
		--url https://api.glassflow.dev/v1/openapi.yaml \
		--output ./src/glassflow/models/api/api.py

add-noqa: generate-api-data-models
	echo "Add noqa comment ..."
	sed -i '' -e '1s/^/# ruff: noqa\n/' ./src/glassflow/models/api/api.py


add-dataclass-json-decorators: add-noqa
	echo "Import dataclass_json ..."
	sed -i '' -e '/^from __future__ import annotations/a\'$$'\n''from dataclasses_json import dataclass_json' ./src/glassflow/models/api/api.py


	echo "Add dataclass_json decorators ..."
	sed  -i'' -e '/@dataclass/ i\'$$'\n''@dataclass_json\'$$'\n''' ./src/glassflow/models/api/api.py

generate: add-dataclass-json-decorators