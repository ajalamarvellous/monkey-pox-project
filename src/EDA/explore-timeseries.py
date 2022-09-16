from datetime import timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Location to the parquet file and where to save visualizations
location = Path(
    "data", "raw", "2022-09-12-timeseries-country-confirmed.parquet"
)  # noqa
plot_locations = Path("reports", "figures").__str__()

# Read the data
df = pd.read_parquet(location)

# BASIC DATA INFORMATION
df.head()

df.info()

df.info()

df.describe(include="all")

# convert column "Date" to datetime format
df["Date"] = pd.to_datetime(df["Date"])

df.info()

# Count the no of unique countries in the data
df["Country"].nunique()

# Get all the unique values for column country
df["Country"].unique()

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
    # plt.savefig(Path(plot_locations, f"{country}.png"), dpi=100)


plot_country_progression(naija_prog, "Nigeria")
naija = df[df["Country"] == "Nigeria"]
sns.lineplot(x=naija["Date"], y=naija["Cumulative_cases"], hue=None)
sns.lineplot(x=naija["Date"], y=naija["Cases"], hue=None)

df[df["Date"] == df.Date.min() + timedelta(days=1)]

df[(df["Date"] == df.Date.max()) & (df["Country"] == "Nigeria")]

cameroon_prog = smoothen_curve(df, "Cameroon")
cameroon_prog


UK_prog = smoothen_curve(df, "United Kingdom")
UK_prog
UK_prog.Date.max() - UK_prog.Date.min()

plot_country_progression(UK_prog, "UK")
uk = df[df["Country"] == "United Kingdom"]

fig, ax = plt.subplots(1, 1, figsize=(10, 8))
sns.lineplot(x=uk["Date"], y=uk["Cumulative_cases"], hue=None)
fig, ax = plt.subplots(1, 1, figsize=(10, 8))
sns.lineplot(x=uk["Date"], y=uk["Cases"], hue=None)

uk_no_zero = uk[uk["Cases"] != 0]
fig, ax = plt.subplots(1, 1, figsize=(10, 8))
sns.lineplot(x=uk_no_zero["Date"], y=uk_no_zero["Cases"], hue=None)

US_prog = smoothen_curve(df, "United States")
US_prog
US_prog.Date.max() - US_prog.Date.min()

plot_country_progression(US_prog, "US")


top20_count = latest_date["Country"].values[:20]
top20_count

df_top20 = smoothen_curve(df, top20_count)
df_top20


plot_country_progression(df_top20, "Top 20 countries", hue="Country")


# Next,let's view the rate of change to get insight into countries with fastest
# rapidly increasing rates
def rate_of_change(df: object, duration: int):
    """
    This function computes the rate of change/increase of monkeypox cases for
    different countries on the duration started
    Parameter(s)
    ---------------
    df      : object
              pandas dataframe from which we will get the info we need
    duration: int
              integer representing time duration we want to see

    Return(s):
    ----------

    """
    # Getting all the unique values for countries
    countries = df["Country"].unique()
    # initializing dictionary where we will save al our values
    rate_dict = dict()
    for country in countries:
        rate_dict[country] = dict()

        # slicing df to give just country of focus
        df_ = df[df["Country"] == country]

        # initializing date of first and latest appearance of the virus in the
        # country
        start_date = df_["Date"].min()
        final_date = df_["Date"].max()
        while start_date < final_date:
            # Interval which we want to compute first
            interval_end = start_date + timedelta(days=duration)
            # If end of interval is beyond the latest date in our dataset, set
            # interval end to latest date in our dataset
            if interval_end > final_date:
                interval_end = final_date

            # getting the interval in days
            interval = interval_end - start_date

            # getting difference between the end of interval and start date
            case_diff = (
                df_[df_["Date"] == interval_end]["Cumulative_cases"].values[0]
                - df_[df_["Date"] == start_date]["Cumulative_cases"].values[0]
            )

            # getting the rate of cahnge of cases over time and appending to
            # our dictionary
            rate_dict[country][interval_end.__str__()] = (
                case_diff / interval.days
            )  # noqa
            # update start date to start from end of previous interval
            start_date = interval_end
    return rate_dict


# get dictionary containing rate of change in countries
rate_change_dict = rate_of_change(df=df, duration=30)

# getting the countries
countries = df.Country.unique()

# values for rate_change_dict (rcd)
rcd_dict_values = list(rate_change_dict.values())
# keys of the values of timestamp
timestamps = [list(x.keys()) for x in rcd_dict_values]
# linearize timestamps to give a single list as opposed to list in list
all_timestamps = []
for x in timestamps:
    all_timestamps.extend(x)

# Unique timestamp values
timestamp = set(all_timestamps)

# rate_of_change(roc) dataframe
roc_df = pd.DataFrame(rate_change_dict, columns=countries, index=timestamp)

# convert roc_df index to datetime
roc_df.index = pd.to_datetime(roc_df.index)

roc_df.index

roc_df

# downsample roc_df to bin months together
roc_df = roc_df.resample("M").sum()

roc_df

top20_roc = roc_df.max(axis=0).sort_values(ascending=False)[:20]
list(top20_roc.index)

fig, ax = plt.subplots(1, 1, figsize=(10, 8))
# sns.lineplot(x=roc_df.index, y=list(top20_roc.index), data=roc_df)
plt.plot(roc_df[list(top20_roc.index)])
plt.title(
    "Rate of spread of Monkeypox cases in the top2o fastest spreading \
    months against time(month)"
)
plt.xlabel("Time(months)")
plt.ylabel("Rate of spread per day")
plt.legend(list(top20_roc.index))
plt.savefig(Path(plot_locations, "top20_countries_roc.png"), dpi=100)

xyz = roc_df[list(top20_roc.index)].unstack()

plt.bar(xyz, height=xyz.values)

fig, ax = plt.subplots(1, 1, figsize=(10, 8))
# sns.lineplot(x=roc_df.index, y=list(top20_roc.index), data=roc_df)
plt.hist(roc_df[list(top20_roc.index)])
plt.title(
    "Rate of spread of Monkeypox cases in the top2o fastest spreading \
    months against time(month)"
)
plt.xlabel("Time(months)")
plt.ylabel("Rate of spread per day")
plt.legend(list(top20_roc.index))
# plt.savefig(Path(plot_locations, "top20_countries_roc.png"), dpi=100)
