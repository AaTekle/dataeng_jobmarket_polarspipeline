from ingest import main as ingest_main
from transform_polars import main as transform_main


def main() -> None:
    print("Starting ingestion...")
    ingest_main()

    print("Starting Polars transformation...")
    transform_main()

    print("Pipeline complete.")


if __name__ == "__main__":
    main()