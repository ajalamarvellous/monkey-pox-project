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
    """This function reads and return the content of the document"""
    file = open(address, "r")
    csv_file = list(csv.reader(file))
    file.close()
    return csv_file


def len_(header):
    """Returns the length of the row"""
    return len(header)


def MODIFY_LINE(line, header_len):
    """
    This function checks whether to modify line or not, if the line is longer
    than the header, then something is wrong with the line
    """
    if len_(line) > header_len:
        return True
    else:
        return False


def get_values(line, pos):
    """This function get the values that are splitted across multiple values"""
    if pos == "start":
        values = [x for x in line if x.startswith('"')]
    elif pos == "end":
        values = [x for x in line if x.endswith('"')]
    return values


def get_index(line, values):
    """This function returns the index of a word in a row"""
    index = [line.index(x) for x in values]
    index.sort()
    return index


def concat_words(line, start_index, end_index):
    """
    This function makes new words by joing words from starting index to ending
    index
    """
    new_word_list = []
    # Zip allows us to pass a list as both start index and end_index and allow
    # us to pick the values inside them one after the other.
    for x, y in zip(start_index, end_index):
        # "" instantiates a new string with no value
        new_word = ""
        # y+1 because range in not upper {higher) value inclusive, so as to
        # the end index value also
        for index in range(x, y + 1):
            new_word += line[index]
        new_word_list.append(new_word.strip('"'))
    return new_word_list


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
