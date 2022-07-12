import os
import sys

import pytest

# Importing the file we want to test
sys.path.insert(0, os.getcwd() + "/src/data")
import make_dataset as md  # noqa


@pytest.fixture
def dir_name():
    location = os.path.dirname(__file__)
    return os.path.join(location, "..", "data", "raw")


def test_file_address(dir_name):
    assert os.path.exists(dir_name)
    address = md.file_address(dir_name, "latest.csv")
    assert os.path.exists(address)
    none_yet = md.file_address(dir_name, "none_yet.csv")
    assert none_yet == os.path.join(dir_name, "none_yet.csv")


def test_get_file():
    pass


def test_len_():
    pass


def test_MODIFY_LINE():
    pass


def test_get_values():
    pass


def test_get_index():
    pass
