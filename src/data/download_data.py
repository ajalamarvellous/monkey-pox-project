import requests
import csv
import logging

log_fmt = "%(asctime)s - [%(name)s] - %(levelname)s : %(message)s"
logging.BasicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger()
