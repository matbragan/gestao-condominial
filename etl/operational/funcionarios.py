from etl.operational.source import operational_writer, generic_treatment


def run() -> None:
    operational_writer("funcionarios", generic_treatment)


if __name__ == "__main__":
    run()
