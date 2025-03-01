import setuptools

try:
    with open("README.md") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = ""

setuptools.setup(
    name="glassflow",
    version="2.1.0",
    author="glassflow",
    description="GlassFlow Python Client SDK",
    url="https://www.glassflow.dev/docs",
    project_urls={
        "Source": "https://github.com/glassflow/glassflow-python-sdk",
    },
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(where="src"),
    install_requires=[
        "urllib3==1.26.15",
        "certifi>=2023.7.22",
        "charset-normalizer>=3.2.0",
        "pydantic>=2.10.6",
        "idna>=3.4",
        "jsonpath-python>=1.0.6 ",
        "marshmallow>=3.19.0",
        "mypy-extensions>=1.0.0",
        "packaging>=23.1",
        "python-dateutil>=2.8.2",
        "requests>=2.31.0",
        "six>=1.16.0",
        "typing-inspect>=0.9.0",
        "typing_extensions>=4.7.1",
        "python-dotenv==1.0.1",
        "eval_type_backport>=0.2.0",
    ],
    extras_require={
        "dev": [
            "pylint>=2.16.2",
            "pytest>=8.3.2",
            "pytest-cov>=5.0.0",
            "datamodel-code-generator[http]>=0.27.0",
            "requests-mock>=1.12.1",
            "isort>=5.13.2",
            "ruff>=0.9.0",
        ]
    },
    package_dir={"": "src"},
    python_requires=">=3.8",
    package_data={"glassflow": ["py.typed"]},
    entry_points={
        "console_scripts": [
            "glassflow = cli.cli:glassflow",
        ],
    },
)
