import importlib
import logging

from etl import TABLES
from utils.bigquery import load_tables

log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)


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

    try:
        load_tables("operational")
        logging.info("Operational Tables load in BigQuery")
    except Exception as e:
        logging.warning(f"Fatal Error loading tables in BigQuery: {e}")


if __name__ == "__main__":
    main()