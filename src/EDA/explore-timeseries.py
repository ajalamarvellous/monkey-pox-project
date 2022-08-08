from datetime import timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Location to the parquet file
# Path(__file__).parents[1]
location = Path(
    "data", "raw", "2022-08-04-timeseries-country-confirmed.parquet"
)  # noqa
plot_locations = Path("reports", "figures").__str__()

# Read the data
df = pd.read_parquet(location)

# See first five items of the file
df.head()

# View basic info about the datatype, count and how many not null values in
# the file
df.info()

# Get the description (count, nunique, frequency, mean, median etc) of the
# data including the non integer or float values
df.describe(include="all")

# convert column "Date" to datetime format
df["Date"] = pd.to_datetime(df["Date"])

df.info()

# Get all the unique values for column country
df["Country"].unique()

# Count the no of unique countries in the data
df["Country"].nunique()

# Count the freqeuncy of each country in the dataset and arrange in the
# descending order
c_count = df["Country"].value_counts().sort_values(ascending=False)
c_count[:20]
c_count[-20:]

# get the earliest date in the dataset
df[df["Date"] == df["Date"].min()]

# slice the dataset to return only values of the last date
latest_date = df[df["Date"] == df["Date"].max()]
latest_date = latest_date.sort_values(by="Cumulative_cases", ascending=False)
latest_date[:20]


# Visualize the number of cases of top 20 countries and save it
fig, ax = plt.subplots(1, 1, figsize=(10, 8))
sns.barplot(
    x=latest_date["Country"][:20], y=latest_date["Cumulative_cases"][:20]
)  # noqa
# rotate the x ticks to stand vertically, so as to better format the plot
plt.xticks(rotation=90)
plt.savefig(f"{plot_locations}/top20_countries.png", dpi=100)


def smoothen_curve(df, country=None):
    """Get the smoothened cumulative curve for selected country"""
    df_list = list()
    # checks if the country passed in a single country or a list of countries
    if country is None:
        # if no country is provided, use all the unique country values as list
        # of countries to use
        country = df["Country"].unique()
    elif isinstance(country, str):
        # if only a country is provided, convert it to a list
        # this is because we want to iterate over the values in countries and
        # if a str is provided, python with iterate over each character instead
        country = [country]
    else:
        assert isinstance(country, list)
        country = country
    for country_ in country:
        indices = []
        # get slice of dataframe matching the country
        df_ = df[df["Country"] == country_]
        cum_cases = 0
        # iterate over it row by row
        for row in df_.iterrows():
            # for the first row
            if indices == []:
                # get the cumulative cases
                cum_cases = row[1]["Cumulative_cases"]
                # add the index to the indices list
                indices.append(row[0])
            else:
                # for other values, check ig the cumulative cases is greater
                # than the last Cumulative_cases
                if row[1]["Cumulative_cases"] > cum_cases:
                    # if true, add the index to list of indices
                    indices.append(row[0])
                    # update value for the last Cumulative_cases
                    cum_cases = row[1]["Cumulative_cases"]
        # slice the dataframe with values of all the indices in the indices
        # list
        df_list.append(df.iloc[indices])
    return pd.concat(df_list)


naija_prog = smoothen_curve(df, "Nigeria")
naija_prog
naija_prog.Date.max() - naija_prog.Date.min()


def plot_country_progression(df, country, hue=None):
    if hue is not None:
        hue = df[hue]
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    sns.lineplot(x=df["Date"], y=df["Cumulative_cases"], hue=hue)
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


top20_count = latest_date["Country"].values[:20]
top20_count

df_top20 = smoothen_curve(df, top20_count)
df_top20


plot_country_progression(df_top20, "Top 20 countries", hue="Country")
