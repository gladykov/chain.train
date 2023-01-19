import pytest


def pytest_addoption(parser):
    parser.addoption("--schema_name")
    parser.addoption("--env")
