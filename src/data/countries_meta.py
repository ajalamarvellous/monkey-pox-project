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
    return df
