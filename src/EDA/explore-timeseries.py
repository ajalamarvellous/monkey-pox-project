from datetime import timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Path(__file__).parents[1]
location = Path(
    "data", "raw", "2022-08-04-timeseries-country-confirmed.parquet"
)  # noqa
plot_locations = Path("reports", "figures").__str__()

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


fig, ax = plt.subplots(1, 1, figsize=(10, 8))
sns.barplot(
    x=latest_date["Country"][:20], y=latest_date["Cumulative_cases"][:20]
)  # noqa
plt.xticks(rotation=90)
plt.savefig(f"{plot_locations}/top20_countries.png", dpi=100)


def smoothen_curve(df, country=None):
    """Get the smoothened cumulative curve for selected country"""
    df_list = list()
    if country is None:
        country = df["Country"].unique()
    elif isinstance(country, str):
        country = [country]
    else:
        country = country
    for country_ in country:
        indices = []
        df_ = df[df["Country"] == country_]
        cum_cases = 0
        for row in df_.iterrows():
            if indices == []:
                cum_cases = row[1]["Cumulative_cases"]
                indices.append(row[0])
            else:
                if row[1]["Cumulative_cases"] > cum_cases:
                    indices.append(row[0])
                    cum_cases = row[1]["Cumulative_cases"]
        df_list.append(df.iloc[indices])
    return pd.concat(df_list)


naija_prog = smoothen_curve(df, "Nigeria")
naija_prog
naija_prog.Date.max() - naija_prog.Date.min()


def plot_country_progression(df, country):
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    sns.lineplot(x=df["Date"], y=df["Cumulative_cases"])
    plt.title(f"Plot of Monkeypox cases in the {country} against time")
    plt.savefig(Path(plot_locations, f"{country}.png"), dpi=100)


plot_country_progression(naija_prog, "Nigeria")


df[df["Date"] == df.Date.min() + timedelta(days=1)]


cameroon_prog = smoothen_curve(df, "Cameroon")
cameroon_prog


UK_prog = smoothen_curve(df, "United Kingdom")
UK_prog
UK_prog.Date.max() - UK_prog.Date.min()

plot_country_progression(UK_prog, "UK")


US_prog = smoothen_curve(df, "United States")
US_prog
US_prog.Date.max() - US_prog.Date.min()

plot_country_progression(US_prog, "US")
