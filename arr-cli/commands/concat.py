import click
import glob
from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq


@click.command()
@click.argument("src", nargs=-1, type=click.Path())
@click.argument("out", type=click.Path())
@click.option("-d", "--delimiter", default=",", help="set the delimiter")
@click.option(
    "-c",
    "--column-name",
    multiple=True,
    default=[],
    help="set columns names if csv has no header",
)
@click.option(
    "-i",
    "--include-columns",
    multiple=True,
    default=[],
    help="set columns to include while reading files",
)
@click.option("-f", "--format", default="parquet", help="set the format of the output")
@click.option(
    "--header/--no-header", default=True, help="set weather the csv file has headers"
)
def concat(src, out, delimiter, format, column_name, include_columns, header):
    """Concatenate multiple csv files (SRC) into one parquet or csv file (OUT).

    SRC can be multiple paths and also accepts wildcards

    OUT is the path to the output parquet/csv file

    Example:
    'concat path/test*.csv path/unique.csv path/output.parquet'
    will concat all csv respecting test*.csv and unique.csv into output.parquet
    """
    csv_path = []
    for fn in src:
        csv_path += glob.glob(fn, recursive=False)
    if not csv_path:
        click.echo("No file(s) found")

    csv_readers = []
    read_options = csv.ReadOptions(autogenerate_column_names=False)
    if not header and len(column_name) == 0:
        read_options = csv.ReadOptions(autogenerate_column_names=True)
    parse_options = csv.ParseOptions(delimiter=delimiter)
    convert_options = csv.ConvertOptions(include_columns=list(column_name))
    for file_path in csv_path:
        try:
            csv_reader = csv.read_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            )
            csv_readers.append(csv_reader)
        except Exception:
            click.echo(f"cannot read_csv {file_path}")

        if len(csv_readers) > 0:
            result_table = pa.concat_tables(csv_readers, unify_schemas=True)
            if format == "parquet":
                pq.write_table(result_table, out)
            elif format == "csv":
                csv.write_csv(
                    result_table,
                    out,
                    write_options=csv.WriteOptions(delimiter=delimiter),
                )
        else:
            click.echo("Wrong format: use csv")
