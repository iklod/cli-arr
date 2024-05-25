import pyarrow.csv as csv
import pyarrow.parquet as pq
import duckdb
import click
from tabulate import tabulate
import pathlib
import sys


@click.command()
@click.argument(
    "file",
    nargs=1,
    type=click.Path(exists=True, dir_okay=False),
)
@click.argument(
    "statement",
    nargs=1,
    type=str,
)
@click.option("-d", "--delimiter", default=",", help="delimiter for input csv file")
def query(file, statement, delimiter):
    """
    Execute sql STATEMENT on table FILE (csv or parquet file)

    In the sql statement, refer to the table as "arr_table" by default
    """
    ext = pathlib.Path(file).suffix
    if ext == ".parquet":
        arr_table = pq.read_table(file)
    elif ext == ".csv":
        parse_options = csv.ParseOptions(delimiter=delimiter)
        arr_table = csv.read_csv(
            file,
            parse_options=parse_options,
        )
    else:
        click.echo("wrong FILE input, use csv or parquet")
        sys.exit()
    duck = duckdb.connect()
    output = duck.execute(query=statement).arrow()
    print(tabulate(output.to_pydict(), headers="keys"))

    pass
