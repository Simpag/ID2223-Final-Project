import argparse
import os

from src.data_ingestion import run


def check_api_key(key_path):
    if os.getenv("HOPSWORKS_API_KEY", None) is None and key_path is not None:
        with open(key_path, "r") as f:
            os.environ["HOPSWORKS_API_KEY"] = f.read()


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", type=str, required=True)
    args.add_argument("--api_key_path", type=str, default=None)
    parser = args.parse_args()

    check_api_key(parser.api_key_path)
    print(os.environ["HOPSWORKS_API_KEY"])
    run(parser.config)
