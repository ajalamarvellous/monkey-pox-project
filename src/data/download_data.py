import requests
import csv
import logging
import os

log_fmt = "%(asctime)s - [%(name)s] - %(levelname)s : %(message)s"
logging.BasicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger()


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


def save_file(data, filename):
    """Saves the data to into the file"""
    address = os.path.join(os.getcwd().split("/")[:-1],
                            "data", "raw", filename)
    file = open(address, "w+")
    csv_writer = csv.Writer(file)
    for line in data:
        csv_writer.writerow(line))
    file.close()
    logger.info("File written and saved successfully")
