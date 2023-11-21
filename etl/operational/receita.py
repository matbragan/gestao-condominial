from etl.operational.source import operational_writer, financial_generic_treatment


def run() -> None:
    operational_writer("receita", financial_generic_treatment)


if __name__ == "__main__":
    run()