import argparse


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="Environment", required=True)
    parser.add_argument("--schema_name", help="Schema to test", required=True)
    parser.add_argument("--log_level", help="Log level", required=False, default="info")

    # Add pytest specific arguments below this line
    parser.add_argument("-s", help="Log level", required=False, default="info")

    return parser.parse_args()
