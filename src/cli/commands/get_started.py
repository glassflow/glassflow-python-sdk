import os

import click
from dotenv import load_dotenv


@click.command()
@click.option(
    "--personal-access-token", "-pat", default=None, help="Personal access token."
)
@click.option(
    "--env-file",
    "-e",
    default=".env",
    help="Path to the .env file (default: .env in current directory).",
)
def get_started(personal_access_token, env_file):
    """Displays a welcome message and setup instructions."""

    # Load token from .env if not provided in CLI
    if personal_access_token is None:
        if os.path.exists(env_file):
            load_dotenv(env_file)  # Load environment variables
            personal_access_token = os.getenv("PERSONAL_ACCESS_TOKEN")
        else:
            click.echo("⚠️ No token provided and .env file not found!", err=True)
            return

    if not personal_access_token:
        click.echo("❌ Error: Personal access token is required.", err=True)
        return

    click.echo("🚀 Welcome to Glassflow! \n")
    click.echo(
        f"🔑 Using Personal Access Token: {personal_access_token[:4]}... "
        f"(hidden for security)"
    )
    click.echo("\n📝 In this getting started guide, we will do the following:")
    click.echo("1. Define a data transformation function in Python.\n")
    click.echo("2. Create a pipeline with the function.\n")
    click.echo("3. Send events to the pipeline.\n")
    click.echo("4. Consume transformed events in real-time from the pipeline\n")
    click.echo("5. Monitor the pipeline and view logs.\n")

    filename = create_transformation_function()
    pipeline = create_space_pipeline(personal_access_token, filename)
    send_consume_events(pipeline)

    click.echo(
        "\n🎉 Congratulations! You have successfully created a pipeline and sent"
        " events to it.\n"
    )
    click.echo(
        "💻 View the logs and monitor the Pipeline in the "
        f"Glassflow Web App at https://app.glassflow.dev/pipelines/{pipeline.id}"
    )


def create_transformation_function(filename="transform_getting_started.py"):
    file_content = """import json
import logging

def handler(data: dict, log: logging.Logger):
    log.info("Echo: " + json.dumps(data))
    data['transformed_by'] = "glassflow"

    return data
"""
    with open(filename, "w") as f:
        f.write(file_content)
    click.echo(f"✅ Transformation function created in {filename}")
    click.echo("The transformation function is:\n")
    click.echo(file_content)
    click.echo("📝 You can modify the transformation function in the file.")
    return filename


def create_space_pipeline(personal_access_token, transform_filename):
    import glassflow

    # create glassflow client to interact with GlassFlow
    client = glassflow.GlassFlowClient(personal_access_token=personal_access_token)
    example_space = client.create_space(name="getting-started")
    pipeline = client.create_pipeline(
        name="getting-started-pipeline",
        transformation_file=transform_filename,
        space_id=example_space.id,
    )
    click.echo(f"✅ Created a pipeline with pipeline_id {pipeline.id}")
    return pipeline


def send_consume_events(pipeline):
    click.echo("🔄 Sending some generated events to pipeline .....")
    data_source = pipeline.get_source()
    for i in range(10):
        event = {"data": f"hello GF {i}"}
        res = data_source.publish(event)
        if res.status_code == 200:
            click.echo(f"Sent event: {event}")

    click.echo("📡 Consuming transformed events from the pipeline")
    data_sink = pipeline.get_sink()
    for _ in range(10):
        resp = data_sink.consume()
        if resp.status_code == 200:
            click.echo(f"Consumed event: {resp.event()} ")
