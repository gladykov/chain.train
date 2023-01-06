def pytest_addoption(parser):
    parser.addoption("--schema_name", action="store")
    parser.addoption("--env", action="store")