from pathlib import Path

import pandas as pd

# Path(__file__).parents[1]
location = Path(
    "data", "raw", "2022-08-04-timeseries-country-confirmed.parquet"
)  # noqa

df = pd.read_parquet(location)

df.head()
df.info()
df.describe(include="all")

df["Date"] = pd.to_datetime(df["Date"])

df.info()
