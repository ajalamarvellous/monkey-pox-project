import os

import pytest


@pytest.fixtures
def dir_name():
    location = os.path.abspath(__file__)
    return os.path.join([location.parents[0], "data", "raw"])


def test_file_address():
    pass


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
