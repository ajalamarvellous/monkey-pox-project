"""
url = "https://meta.m.wikimedia.org/wiki/List_of_countries_by_regional_classification"  # noqas
"""

from pathlib import Path

import pandas as pd

LOCATION = Path("data", "raw")


def get_countries(url: str):
    """
    This function downloads data from a table in a wikipedia page online and
    saves it to a dataframe
    """
    data = pd.read_html(url)
    df = pd.DataFrame(data[0])
    filename = Path(LOCATION, "countries_meta.parquet")
    df.to_parquet(filename)
    return df
