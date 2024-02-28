from lazydocs import MarkdownGenerator
"""
try:
    generate_docs(
        ["glassflow.config"],
        output_path="./docs/api-docs",
        src_base_url=
        "https://github.com/glassflow/glassflow-python-sdk/blob/main",
        overview_file="README.md")
except Exception as e:
    print(e)
"""
from glassflow.config import GlassFlowConfig
import glassflow.config

generator = MarkdownGenerator()

modules = [GlassFlowConfig]

for module in modules:
    markdown_docs = generator.class2md(module)
    print(markdown_docs)
    with open(f"docs/api-docs/{module}.md", "w") as f:
        f.write(markdown_docs)
