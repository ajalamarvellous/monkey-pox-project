from pathlib import Path

import pandas as pd

# location = Path(Path(__file__).parents[1], "data", "raw", "latest.parquet")
location = Path("data", "raw", "2022-08-04-latest.parquet")

df = pd.read_parquet(location)

df.head()
df.info()
df.describe(include="all")
df.iloc[0]
