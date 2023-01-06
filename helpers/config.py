from pathlib import Path
import tomli

def config():
    config_file = Path(__file__).parent.parent / "config.toml"
    return tomli.loads(config_file.read_text())