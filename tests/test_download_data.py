import csv
import os
import sys

import pytest

# Adding path to the source codes
sys.path.insert(0, os.getcwd() + "/src/data")
import download_data as dd  # noqa


@pytest.fixture
def url():
    return "https://www.google.com"


@pytest.fixture
def data():
    return ["1, Asaba born, 2020, this", "2, warri warrior, 2022, that"]


def test_get_data(url):
    """Tests get_data function"""
    # Using .run because it is a prefect task and it is running outside prefect
    # flow manager, thus need .run method to function
    data = dd.get_data.run(url)
    assert isinstance(data, list)
    assert isinstance(data[0], str)


def test_get_name(url):
    name = dd.get_name.run(url)
    assert name == "www.google.com"


def test_save_file(data):
    name = "test_data.csv"
    # Saving our test file
    dd.save_file.run(data, name)
    dir = os.path.join("data", "raw")
    # listing the files in our file destination
    files = os.listdir(dir)
    # obtaining the file that march our test file and attaching dir name
    # before it
    file = "/".join([dir, [x for x in files if x.endswith(name)][0]])
    # reading the file
    with open(file, "r") as new_data:
        read_data = list(csv.reader(new_data))
        assert len(read_data) == 2
        assert read_data[1] == data[1].split(",")
    os.remove(file)


def test_main():
    pass
