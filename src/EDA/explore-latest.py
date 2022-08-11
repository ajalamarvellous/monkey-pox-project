from pathlib import Path

import pandas as pd

# location = Path(Path(__file__).parents[1], "data", "raw", "latest.parquet")
location = Path("data", "raw", "2022-08-04-latest.parquet")

df = pd.read_parquet(location)

df.head()
df.info()
df.describe(include="all")
df.iloc[0]

df.columns


df = df.drop(["Source_V", "Source_VI", "Source_VII"], axis=1)

df["Date_onset"] = pd.to_datetime(df["Date_onset"])
df["Date_confirmation"] = pd.to_datetime(df["Date_confirmation"])
df["Date_hospitalisation"] = pd.to_datetime(df["Date_hospitalisation"])
df["Date_onset"].min()
df["Date_confirmation"].min()
df[df["Date_confirmation"] == df["Date_confirmation"].min()]


df.ID.nunique()

df.Location.nunique()

df.Location.value_counts()
df.City.value_counts()
df.sample(10)

df[
    df[
        [
            "Date_hospitalisation",
            "Isolated (Y/N/NA)",
            "Date_isolation",
            "Outcome",
            "Contact_comment",
            "Contact_ID",
            "Contact_location",
            "Travel_history (Y/N/NA)",
            "Travel_history_entry",
            "Travel_history_start",
            "Travel_history_location",
            "Travel_history_country",
            "Genomics_Metadata",
            "Confirmation_method",
        ]
    ].notnull()
]
df[
    df["Genomics_Metadata"].notnull() & (df["Travel_history_country"].notnull())  # noqa
].sample(  # noqa
    10
)
