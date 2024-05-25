import click
from commands import concat
from commands import schema
from commands import cfilter
from commands import query


@click.group(help="CLI tool to perform operation with arrow and duckdb")
def cli():
    pass


cli.add_command(concat.concat)
cli.add_command(schema.schema)
cli.add_command(cfilter.cfilter)
cli.add_command(query.query)
if __name__ == "__main__":
    cli()
