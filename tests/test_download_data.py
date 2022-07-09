import os
import sys

import pytest

# Adding path to the source codes
sys.path.insert(0, os.getcwd() + "/src/data")
import download_data as dd  # noqa


@pytest.fixture
def url():
    return "https://www.google.com/"


def test_get_data(url):
    """Tests get_data function"""
    # Using .run because it is a prefect task and it is running outside prefect
    # flow manager, thus need .run method to function
    data = dd.get_data.run(url)
    assert isinstance(data, list)
    assert isinstance(data[0], str)


def test_get_name():
    pass


def test_save_file():
    pass


def test_main():
    pass
