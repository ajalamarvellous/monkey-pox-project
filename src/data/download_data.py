import requests
import csv
import logging
import os


log_fmt = "%(name)s %(asctime)s - [%(funcName)s] - %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt, filename="docs/download.log") # noqa
logger = logging.getLogger(__name__)


def get_data(url):
    """Retrieves the data from the internet"""
    data = requests.get(url)
    # Confirms that correct data is returned
    try:
        data.raise_for_status()
        logger.info("File downloaded safely")
        return data
    except Exception as exec:
        logger.exception(f"File download was not successful due to {exec}")


def get_name(url):
    """Retrieves the name of the file from the url address"""
    return url.split("/")[-1]


def save_file(data, file_name):
    """Saves the data to into the file"""
    address = os.path.join("data", "raw", file_name)
    file = open(address, "w+")
    csv_writer = csv.writer(file)
    for line in data:
        csv_writer.writerow(line)
    file.close()
    logger.info("File written and saved successfully")


def main():
    url_list = [
        "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/timeseries-country-confirmed.csv", # noqa
        "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv", # noqa
    ]
    for url in url_list:
        data = get_data(url)
        filename = get_name(url)
        save_file(data=data, file_name=filename)
    logger.info("Task completed...")


if __name__ == "__main__":
    main()
