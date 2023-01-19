import sys
from types import SimpleNamespace


def parser():
    # argparse was supposed to be nice. It is not, when used with Pytest.
    # parse_known_args() is useless

    args = ["--env", "--schema_name"]
    recognized_args = []
    for arg in sys.argv:
        if arg.split("=")[0] in args:
            recognized_args.append(arg)

    assert len(args) == len(recognized_args), "Provide args"

    parsed_args = SimpleNamespace()

    for recognized_arg in recognized_args:
        name, value = recognized_arg.split("=")
        name = name.lstrip("-")  # Remove --
        setattr(parsed_args, name, value)

    return parsed_args
