import logging
import os

import pandas as pd

# from prefect import Flow, task
# from prefect.schedules import IntervalSchedule

log_fmt = "%(name)s %(asctime)s - [%(funcName)s] - %(levelname)s : %(message)s"
logging.basicConfig(
    level=logging.INFO, format=log_fmt, filename="docs/download.log"
)  # noqa
logger = logging.getLogger(__name__)


# @Flow
def main(url):
    """Saves the data to into the file"""
    file_name = url.split("/")[-1]
    file_name = f"{file_name.split('.')[0]}.parquet"
    # address = os.path.join(
    #     "data", "raw", datetime.now().ctime() + " " + file_name
    # )
    address = os.path.join("data", "raw", file_name)
    df = pd.read_csv(url)
    df.to_parquet(address)
    # logger.info("File written and saved successfully")


# TO FIX:
# Schedule for how often to run the tasks
# scheduler = IntervalSchedule(interval=timedelta(days=1))


if __name__ == "__main__":
    url_list = [
        "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/timeseries-country-confirmed.csv",  # noqa
        "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv",  # noqa
    ]
    for url in url_list:
        main(url)
        # prefect_flow =
        # prefect_flow.run()
