import csv
import logging
import os
from datetime import datetime, timedelta

import requests
from prefect import Flow, task

# from prefect.schedules import IntervalSchedule

log_fmt = "%(name)s %(asctime)s - [%(funcName)s] - %(levelname)s : %(message)s"
logging.basicConfig(
    level=logging.INFO, format=log_fmt, filename="docs/download.log"
)  # noqa
logger = logging.getLogger(__name__)


# Add task to encapsulate our function and retry 10 if it fails
@task(max_retries=10, retry_delay=timedelta(seconds=10))
def get_data(url):
    """Retrieves the data from the internet"""
    data = requests.get(url)
    # Confirms that correct data is returned
    try:
        data.raise_for_status()
        logger.info("File downloaded safely")
        return data.text.split("/n")
    except Exception as error:
        logger.exception(f"File download was not successful due to {error}")


@task
def get_name(url):
    """Retrieves the name of the file from the url address"""
    return url.split("/")[-1]


@task
def save_file(data, file_name):
    """Saves the data to into the file"""
    address = os.path.join(
        "data", "raw", datetime.now().ctime() + " " + file_name
    )  # noqa
    file = open(address, "w+")
    csv_writer = csv.writer(file)
    for line in data:
        csv_writer.writerow(line.split(","))
    file.close()
    logger.info("File written and saved successfully")


# TO FIX:
# Schedule for how often to run the tasks
# scheduler = IntervalSchedule(interval=timedelta(days=1))


def main():
    with Flow(name="Data download pipeline") as flow:
        url_list = [
            "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/timeseries-country-confirmed.csv",  # noqa
            "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv",  # noqa
        ]
        for url in url_list:
            data = get_data(url)
            filename = get_name(url)
            save_file(data=data, file_name=filename)
        logger.info("Task completed...")
    return flow


if __name__ == "__main__":
    prefect_flow = main()
    prefect_flow.run()
