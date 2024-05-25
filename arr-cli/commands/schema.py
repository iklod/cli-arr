import pyarrow.csv as csv
import pyarrow.parquet as pq
import pathlib
import click


@click.command()
@click.argument(
    "input",
    nargs=1,
    type=click.Path(exists=True, dir_okay=False),
)
@click.option("-d", "--delimiter", default=",", help="set the delimiter")
def schema(input, delimiter):
    """Print the schema of INPUT"""
    ext = pathlib.Path(input).suffix
    if ext == ".parquet":
        schema = pq.read_schema(input)
        click.echo(schema)
    elif ext == ".csv":
        csv_reader = csv.open_csv(
            input,
            parse_options=csv.ParseOptions(delimiter=delimiter),
        )
        schema = csv_reader.schema
        click.echo(schema)
    else:
        click.echo("Please use .parquet or .csv files")
