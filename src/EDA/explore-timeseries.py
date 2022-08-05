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

df["Country"].unique()
df["Country"].nunique()

c_count = df["Country"].value_counts().sort_values(ascending=False)
c_count[:20]
c_count[-20:]

df[df["Date"] == df["Date"].min()]
latest_date = df[df["Date"] == df["Date"].max()]
latest_date = latest_date.sort_values(by="Cumulative_cases", ascending=False)
latest_date[:20]
