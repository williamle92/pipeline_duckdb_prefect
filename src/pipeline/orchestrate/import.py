from datetime import datetime
from logging import Logger
from pathlib import Path

import duckdb
from polars import DataFrame
from prefect import flow, get_run_logger, task
from prefect.futures import wait
from prefect.states import Completed
from sqlalchemy.dialects.postgresql import insert

from pipeline.db import AsyncSessionLocal
from pipeline.models.articles import Article
from pipeline.models.publication import Publication

CHUNK_SIZE: int = 500


@task
def get_file_path(file_path: str) -> Path:
    logger: Logger = get_run_logger()
    try:
        csv_file: Path = Path(file_path)
    except Exception as e:
        logger.error(f"Error converting file path: {e}")
        raise FileNotFoundError(
            f"The file {file_path} does not exist or is not a file."
        )

    if not csv_file.exists() and not csv_file.is_file():
        logger.error("Path is not an existing file")
        raise FileNotFoundError(
            f"The file {file_path} does not exist or is not a file."
        )

    return csv_file


@task
def load_csv_into_duckdb(file_path: Path):
    logger: Logger = get_run_logger()
    conn: duckdb.DuckDBPyConnection = duckdb.connect(database=":memory:")
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE articles AS
        FROM '{file_path}';
    """
    )
    row_count: int = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    logger.info(f"Number of rows ingested: {row_count}")
    return conn


@task
def create_publications_table(connection: duckdb.DuckDBPyConnection) -> None:
    logger: Logger = get_run_logger()
    if connection is None:
        logger.error("DuckDB connection is not established.")
        raise ValueError("The DuckDB connection is not established.")

    connection.execute(
        """
        CREATE OR REPLACE TABLE publications AS
        SELECT DISTINCT publication AS name
        FROM articles;
    """
    )
    row_count = connection.execute("SELECT COUNT(*) FROM publications").fetchone()[0]

    logger.info(f"Number of unique publications imported: {row_count}")


@task
async def import_publications_to_postgres(connection: duckdb.DuckDBPyConnection):
    if connection is None:
        raise ValueError("The DuckDB connection is not established.")

    publications: DataFrame = connection.sql("SELECT name FROM publications").pl()
    values: list[dict] = publications.to_dicts()

    publication_mapping: dict = {}
    async with AsyncSessionLocal() as session:
        for i in range(0, len(values), CHUNK_SIZE):
            chunk: list[str] = values[i : i + CHUNK_SIZE]
            # Create insert statement with RETURNING clause
            insert_stmt = insert(Publication).values(chunk)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["name"],
                set_={"updated_on": datetime.utcnow()},
            ).returning(Publication.name, Publication.id)

            async with session.begin():
                result = await session.execute(upsert_stmt)
                # Fetch all returned rows and build the mapping
                returned_rows = result.fetchall()
                for row in returned_rows:
                    publication_mapping[row.name] = row.id

        await session.commit()

    return publication_mapping


@task
async def import_articles_to_postgres(
    connection: duckdb.DuckDBPyConnection, publication_mapping: dict
):
    if connection is None:
        raise ValueError("The DuckDB connection is not established.")

    if not publication_mapping:
        raise ValueError("Publication mapping is empty. Insert publications first.")

    logger: Logger = get_run_logger()
    # Query articles with publication names from DuckDB
    articles_df: DataFrame = connection.sql(
        """
        SELECT
            url,
            title,
            subtitle,
            publication as publication_name,
            date as date_published
        FROM articles
    """
    ).pl()

    articles_data = articles_df.to_dicts()
    logger.info(f"Processing {len(articles_data)} articles")

    # Transform data and resolve foreign keys
    articles_for_insert = []
    missing_publications = set()

    for article_data in articles_data:
        publication_name = article_data["publication_name"]

        # Check if publication exists in mapping
        if publication_name not in publication_mapping:
            missing_publications.add(publication_name)
            continue

        # Create article dict with resolved foreign key
        article_dict = {
            "url": article_data["url"],
            "title": article_data["title"],
            "subtitle": article_data.get("subtitle"),
            "publication_id": publication_mapping[publication_name],
            "date_published": article_data.get("date_published"),
        }
        articles_for_insert.append(article_dict)

    if missing_publications:
        logger.info(f"missing publications have been found: {missing_publications}")

    # Bulk insert articles in chunks
    total_inserted = 0
    async with AsyncSessionLocal() as session:
        for i in range(0, len(articles_for_insert), CHUNK_SIZE):
            chunk = articles_for_insert[i : i + CHUNK_SIZE]

            async with session.begin():
                insert_stmt = insert(Article).values(chunk)
                result = await session.execute(insert_stmt)
                total_inserted += result.rowcount

    logger.info(f"Successfully inserted {total_inserted} articles")
    return total_inserted


@flow(name="Pipeline", timeout_seconds=30)
async def orchestrate_pipeline(file_path: str):
    """Orchestrate the pipeline flow."""
    logger: Logger = get_run_logger()
    logger.info("Starting the pipeline orchestration flow.")
    file_path: str = get_file_path(file_path=file_path)
    logger.info(f"File path obtained: {file_path}")
    logger.info("Importing CSV into duckdb")
    ddb_connection: duckdb.DuckDBPyConnection = load_csv_into_duckdb.submit(
        file_path=file_path
    )
    wait([ddb_connection, create_publications_table.submit(ddb_connection)])
    conn = ddb_connection.result()
    logger.info("Preparing to import publications into Postgres")
    publication_map: dict = await import_publications_to_postgres(conn)
    logger.info("Imported publications into Postgres")
    logger.info("Preparing to import articles into Postgres")
    await import_articles_to_postgres(conn, publication_map)
    logger.info("Pipeline orchestration flow completed.")
    return Completed()


if __name__ == "__main__":
    orchestrate_pipeline(file_path="/Users/williamle/Downloads/medium_data.csv")
