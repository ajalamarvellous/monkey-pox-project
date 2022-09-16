from pathlib import Path

import pandas as pd
from sklearn.preprocessing import LabelEncoder

# Location to the files
RAW_FOLDER = Path("data", "raw")


def read_files(files: list) -> list:
    """Read a list of files and produce the dataframes in a list"""
    df_list = list()
    for file in files:
        file_path = Path(RAW_FOLDER, file)
        df = pd.read_parquet(file_path)
        df_list.append(df)
    return df_list


files = [
    "countries_region.parquet",
    "2022-08-04-timeseries-country-confirmed.parquet",
]  # noqa
df_regions, df_timeseries = read_files(files)


def basic_info(df: pd.DataFrame):
    """Returns a basic summary of the dataframe"""
    print("First 5 rows of the data", df.head(), sep="\n", end="\n\n")
    print("Shape of the dataset", df.shape, end="\n\n")
    print("Info about the dataset", df.info(), sep="\n", end="\n\n")
    print(
        "Description of the dataset",
        df.describe(include="all"),
        sep="\n",
        end="\n\n",  # noqa
    )


basic_info(df_regions)

basic_info(df_timeseries)


def get_intersect(dfs: list, columns: list) -> list:
    """
    Get the intersect of values present in two dataframes

    Argument(s)
    ---------------
    df   :  list
            of dataframes we want to check the intersections from (ideally two)
    columns:  list
              of the columns to use in the same order as their dataframes
    Return(s)
    -------------
    intersects : list
                 of values found the columns specified of dataframes provided
    """
    unique_values = list()
    for dataframe, column in zip(dfs, columns):
        unique_values.append(set(dataframe[column]))
    intersect_values = unique_values[0].intersection(unique_values[1])
    return intersect_values


dfs = [df_regions, df_timeseries]
intersect_columns = ["Country"] * 2
countries_interset = get_intersect(dfs, intersect_columns)

len(countries_interset)


df_timeseries[
    ~df_timeseries["Country"].isin(list(countries_interset))
].Country.unique()  # noqa


# df_regions[df_regions["Country"] == "Bosnia and Herzegovina"]

# The "A" in the "and" in the name starts with small letter [WRONG]
# df_timeseries[df_timeseries["Country"] == "Bosnia and Herzegovina"]

# df_timeseries[df_timeseries["Country"] == "Bosnia And Herzegovina"]

# DEMOCRATIC REPUBLIC OF CONGO
# This gives No result
# df_regions[df_regions["Country"] == "Democratic Republic Of The Congo"]
# df_timeseries[df_timeseries["Country"] == "Democratic Republic Of The Congo"]


def find_index(word: str, df: pd.DataFrame) -> list:
    """Looks for the index of the country that most likely contain the word"""
    indices = list()
    for value, index in zip(
        df_regions["Country"].values, df_regions.index.values
    ):  # noqa
        if word.lower() in value.lower():
            indices.append(index)
    return indices


# congo_indices = find_index("congo", df_regions)
# congo_indices
# df_regions.loc[congo_indices]

# korea_index = find_index("korea", df_regions)
# korea_index
# df_regions.loc[korea_index]
# df_timeseries["Country"].unique()


df_regions["Country"].unique()

countries = {
    "Bosnia and Herzegovina": "Bosnia And Herzegovina",
    "Congo, The Democratic Republic of the": "Democratic Republic Of The Congo",  # noqa
    "Congo": "Republic of Congo",
    "Russian Federation": "Russia",
    "Korea, Republic of": "South Korea",
    "Korea, Democratic People's Republic of": "North Korea",
    "Netherlands Antilles": "Netherlands",
    "Micronesia, Federated States of": "Micronesia",
    "Iran, Islamic Republic of": "Iran",
    "Holy See (Vatican City State)": "Vatican City",
}

df_regions["Country"] = df_regions["Country"].replace(countries)

df_regions

# Combine the two dataframes
df_combined = pd.merge(df_regions, df_timeseries, on="Country")
df_combined


df_combined["Date"] = pd.to_datetime(df_combined["Date"])

df_combined.info()


def time_values(df: pd.DataFrame) -> pd.DataFrame:
    """Generates extra feature based on the time"""
    df["Day"] = df["Date"].dt.weekday
    df["Week"] = df["Date"].dt.isocalendar().week
    df["Month"] = df["Date"].dt.month
    return df


df_combined = time_values(df_combined)
df_combined

encoder = LabelEncoder()

df_combined["Country"] = encoder.fit_transform(df_combined.Country)

df_combined
encoder.classes_


reg_encoder = LabelEncoder()
df_combined["Region"] = reg_encoder.fit_transform(df_combined["Region"])
df_combined

df_combined.to_parquet("./data/processed/processed_file.parquet")
