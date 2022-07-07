import requests
import csv
import logging

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
        logger.error(f"File download was not successful due to {exec}")

def get_name(url):
    """Retrieves the name of the file from the url address"""
    return url.split("/")[-1]
