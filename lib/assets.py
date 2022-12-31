from pathlib import Path
import tomli

EXPECTED_FORMATS = ["date","date_time","float","float","guid","hash","int","timestamp","year"]


def config():
    config_file = Path(__file__).parent.parent / "config.toml"
    return tomli.loads(config_file.read_text())
