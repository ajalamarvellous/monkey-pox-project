import logging
import os
from datetime import datetime

import pandas as pd
from prefect import flow, task
from prefect.schedules import CronSchedule

log_fmt = "%(name)s %(asctime)s - [%(funcName)s] - %(levelname)s : %(message)s"
logging.basicConfig(
    level=logging.INFO, format=log_fmt, filename="docs/download.log"
)  # noqa
logger = logging.getLogger(__name__)


# Schedule for how often to run the tasks
scheduler = CronSchedule(cron="0 0 * * 0")


def get_filename(address):
    """Returns the name the new ile will ne called"""


@task(retries=10, retry_delay_seconds=30)
def get_file(url, address):
    """Get csv file and save locally as .parquet"""
    logger.info(f"Dowloading file from {url}....")
    df = pd.read_csv(url)
    df.to_parquet(address)
    logger.info("File written and saved successfully")


@flow(name="Data-download-etl", schedule=scheduler)
def main(url):
    """Saves the data to into the file"""
    file_name = url.split("/")[-1]
    file_name = f"{file_name.split('.')[0]}.parquet"

    date = datetime.strftime(datetime.now(), "%Y-%m-%d")
    address = os.path.join("data", "raw", date + "-" + file_name)

    get_file(url, address)


if __name__ == "__main__":
    url_list = [
        "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/timeseries-country-confirmed.csv",  # noqa
        "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv",  # noqa
    ]
    for url in url_list:
        flow = main(url)
        flow.run()

# TO DO: Add the urls to a local env file and load with dotenv
