import importlib

from etl import TABLES, DW_TABLES
from utils.bigquery import load_tables, run_sql_file_query


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

    for dw_table in DW_TABLES:
        run_sql_file_query(f"etl/data_warehouse/{dw_table}.sql")

    run_sql_file_query("etl/data_warehouse/data_viz.sql")


if __name__ == "__main__":
    main()
