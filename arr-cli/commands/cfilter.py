import pathlib

import click
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import csv as csv
from tabulate import tabulate
import sys


@click.command()
@click.argument("input", nargs=1, type=click.Path(exists=True, dir_okay=False))
@click.argument("filter", nargs=1, type=click.Path(exists=True, dir_okay=False))
@click.option("-d", "--delimiter", default=",", help="set the delimiter for csv inputs")
@click.option(
    "-f", "--file", type=click.Path(dir_okay=False), help="write result in a csv file"
)
@click.option(
    "-c1",
    "--column-input",
    help="select column to filter on, if no column is set, input needs to be one column only",
)
@click.option(
    "-c2",
    "--column-filter",
    default=None,
    help="select column to create the array filter",
)
@click.option("-d", "--delimiter", default=",", help="set the delimiter")
@click.option("--invert/--no-invert", default=False, help="invert the filter")
def cfilter(input, filter, delimiter, column_input, column_filter, invert, file):
    """Filter an array/table read from INPUT by an array read from FILTER

    If INPUT is a table, a column name on which to filter needs to be set via -c1

    If FILTER has more than one column you need to use -c2 to select the column for the array
    """
    extf = pathlib.Path(filter).suffix
    col_sel = None
    if column_filter is not None:
        col_sel = [column_filter]

    if extf == ".parquet":
        filter_table = pq.read_table(filter, columns=col_sel)
    elif extf == ".csv":
        parse_options = csv.ParseOptions(delimiter=delimiter)
        convert_options = csv.ConvertOptions(include_columns=col_sel)
        filter_table = csv.read_csv(
            filter, parse_options=parse_options, convert_options=convert_options
        )
    else:
        click.echo("FILTER must be csv or parquet")
        sys.exit()
    filter_array = filter_table.column(0)
    ext = pathlib.Path(input).suffix
    if ext == ".parquet":
        table = pq.read_table(input)
    elif ext == ".csv":
        parse_options = csv.ParseOptions(delimiter=delimiter)
        table = csv.read_csv(
            input,
            parse_options=parse_options,
        )
    else:
        click.echo("INPUT must be csv or parquet")
        sys.exit()
    filter_column = table.column(column_input)
    filter_condition = pc.is_in(filter_column, value_set=filter_array)
    if invert:
        result_data = table.filter(pc.invert(filter_condition))
        result(result_data, file, delimiter)
    else:
        result_data = table.filter(filter_condition)
        result(result_data, file, delimiter)


def result(object, file, delimiter):
    if file is not None:
        # result_table = pa.Table.from_arrays([array], names=["result"])
        csv.write_csv(object, file, write_options=csv.WriteOptions(delimiter=delimiter))
    else:
        click.echo(tabulate(object.to_pydict(), headers="keys"))
