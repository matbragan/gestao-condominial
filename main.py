import importlib

from etl import TABLES
from utils.bigquery import load_tables


def run_module(module_path):
    module = importlib.import_module(module_path)
    module.run()


def main():
    modules = [
        "etl.extraction.data_extraction"
    ]

    for table in TABLES:
        modules.append(f"etl.operational.{table}")

    for module_path in modules:
        run_module(module_path)

    load_tables("operational")


if __name__ == "__main__":
    main()