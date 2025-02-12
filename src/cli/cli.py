import click

from .commands import get_started


@click.group()
def glassflow():
    """Glassflow CLI - Manage and control Glassflow SDK"""
    pass



@click.command()
@click.argument("command", required=False)
def help(command):
    """Displays help information about Glassflow CLI and its commands."""

    commands = {
        "get-started": "Initialize Glassflow with an access token.\nUsage: "
        "glassflow get-started --token YOUR_TOKEN",
        "help": "Shows help information.\nUsage: glassflow help [command]",
    }

    if command:
        if command in commands:
            click.echo(f"ℹ️  Help for `{command}`:\n{commands[command]}")
        else:
            click.echo(
                f"❌ Unknown command: `{command}`. Run `glassflow help` for a "
                f"list of commands."
            )
    else:
        click.echo("📖 Glassflow CLI Help:")
        for cmd, desc in commands.items():
            click.echo(f"  ➜ {cmd}: {desc.splitlines()[0]}")
        click.echo("\nRun `glassflow help <command>` for more details.")


# Add commands to CLI group
glassflow.add_command(get_started)
glassflow.add_command(help)

if __name__ == "__main__":
    glassflow()
