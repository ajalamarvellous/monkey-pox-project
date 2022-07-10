# -*- coding: utf-8 -*-
import csv
import logging
import os
from pathlib import Path

import click

# from dotenv import find_dotenv, load_dotenv


def file_address(dir, tail):
    """This function gets the file location of the file we want to modify"""
    files = os.listdir(dir)
    latest = [x for x in files if x.endswith(tail)][0]
    address = "/".join([dir, latest])
    return address


def get_file(address):
    """This file reads and return the content of the document"""
    file = open(address, "r")
    csv_file = list(csv.reader(file))
    file.close()
    return csv_file


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    # load_dotenv(find_dotenv())

    main()
