#!/bin/sh
lazydocs \
    --output-path="./docs/api-docs" \
    --overview-file="README.md" \
    --src-base-url="https://github.com/glassflow/glassflow-python-sdk/blob/main/" \
    glassflow
