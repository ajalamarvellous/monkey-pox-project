"""
url = "https://meta.m.wikimedia.org/wiki/List_of_countries_by_regional_classification"  # noqas
"""

from pathlib import Path

import click
import pandas as pd

LOCATION = Path("data", "raw")


@click.command()
@click.argument("table_url")
@click.argument("output_filepath", type=click.Path())
def get_countries(table_url: str, output_filepath: str):
    """
    This function downloads data from a table in a wikipedia page online and
    saves it to a dataframe
    """
    data = pd.read_html(table_url)
    df = pd.DataFrame(data[0])
    filename = Path(output_filepath, "countries_meta.parquet")
    df.to_parquet(filename)
    return df
