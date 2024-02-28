from lazydocs import generate_docs

generate_docs(
    ["glassflow"],
    output_path="./docs/api-docs",
    src_base_url="https://github.com/glassflow/glassflow-python-sdk/blob/main",
    overview_file="README.md")
