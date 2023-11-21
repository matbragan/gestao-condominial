from etl.operational.source import operational_writer, moradores_treatment


def run() -> None:
    operational_writer("moradores", moradores_treatment)


if __name__ == "__main__":
    run()
