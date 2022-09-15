import logging
import os
from datetime import datetime

import pandas as pd
from prefect import Flow

# from prefect.schedules import IntervalSchedule


log_fmt = "%(name)s %(asctime)s - [%(funcName)s] - %(levelname)s : %(message)s"
logging.basicConfig(
    level=logging.INFO, format=log_fmt, filename="docs/download.log"
)  # noqa
logger = logging.getLogger(__name__)


def main(url):
    """Saves the data to into the file"""
    with Flow(name="Data-download-etl") as flow:

        file_name = url.split("/")[-1]
        file_name = f"{file_name.split('.')[0]}.parquet"

        date = datetime.strftime(datetime.now(), "%Y-%m-%d")
        address = os.path.join("data", "raw", date + "-" + file_name)

        logger.info(f"Dowloading file from {url}....")
        df = pd.read_csv(url)
        df.to_parquet(address)
        logger.info("File written and saved successfully")
        return flow


# TO FIX:
# Schedule for how often to run the tasks
# scheduler = IntervalSchedule(interval=timedelta(days=1))


if __name__ == "__main__":
    url_list = [
        "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/timeseries-country-confirmed.csv",  # noqa
        "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv",  # noqa
    ]
    for url in url_list:
        flow = main(url)
        flow.run()
