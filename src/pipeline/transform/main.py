import asyncio
from datetime import datetime
from pathlib import Path

import duckdb
from polars import DataFrame
from sqlalchemy.dialects.postgresql import insert

from src.pipeline.db import AsyncSessionLocal
from src.pipeline.models.articles import Article
from src.pipeline.models.publication import Publication

CHUNK_SIZE = 100


def load_csv_into_duckdb(
    file_path: str = "/Users/williamle/Downloads/medium_data.csv",
):
    """
    Load a CSV file into a DuckDB table.

    :param file_path: Path to the CSV file.
    :param table_name: Name of the DuckDB table to create or replace.
    """
    if not Path(file_path).exists() and not Path(file_path).is_file():
        raise FileNotFoundError(
            f"The file {file_path} does not exist or is not a file."
        )

    conn: duckdb.DuckDBPyConnection = duckdb.connect(database=":memory:")
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE articles AS
        FROM '{file_path}';
    """
    )
    row_count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"Number of rows ingested: {row_count}")
    return conn


def create_publications_table(connection: duckdb.DuckDBPyConnection):
    if connection is None:
        raise ValueError("The DuckDB connection is not established.")

    connection.execute(
        """
        CREATE OR REPLACE TABLE publications AS
        SELECT DISTINCT publication AS name
        FROM articles;
    """
    )
    row_count = connection.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
    print(f"Number of unique publications: {row_count}")

    return connection


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


async def import_articles_to_postgres(
    connection: duckdb.DuckDBPyConnection, publication_mapping
):
    if connection is None:
        raise ValueError("The DuckDB connection is not established.")

    if not publication_mapping:
        raise ValueError("Publication mapping is empty. Insert publications first.")

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
    print(f"Processing {len(articles_data)} articles")

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
        print(f"missing publications have been found: {missing_publications}")

    # Bulk insert articles in chunks
    total_inserted = 0
    async with AsyncSessionLocal() as session:
        for i in range(0, len(articles_for_insert), CHUNK_SIZE):
            chunk = articles_for_insert[i : i + CHUNK_SIZE]

            async with session.begin():
                insert_stmt = insert(Article).values(chunk)
                result = await session.execute(insert_stmt)
                total_inserted += result.rowcount

    print(f"Successfully inserted {total_inserted} articles")
    return total_inserted


if __name__ == "__main__":
    conn: duckdb.DuckDBPyConnection = load_csv_into_duckdb(
        file_path="/Users/williamle/Downloads/medium_data.csv"
    )
    conn = create_publications_table(conn)
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    publication_id_map: dict = loop.run_until_complete(
        import_publications_to_postgres(conn)
    )

    loop.run_until_complete(import_articles_to_postgres(conn, publication_id_map))
