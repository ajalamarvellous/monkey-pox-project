import os
import sys

import pytest

# Adding path to the source codes
sys.path.insert(0, os.getcwd() + "/src/data")
import download_data as dd  # noqa


@pytest.fixture
def url():
    return "https://www.google.com/"


def test_get_data():
    pass


def test_get_name():
    pass


def test_save_file():
    pass


def test_main():
    pass
