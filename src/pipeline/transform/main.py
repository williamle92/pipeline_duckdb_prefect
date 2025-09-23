import asyncio
from datetime import datetime
from pathlib import Path

import duckdb
from sqlalchemy.dialects.postgresql import insert

from src.pipeline.db import AsyncSessionLocal
from src.pipeline.models.publication import Publication
from src.pipeline.transform.validator.publications import (
    PublicationIn,
    validate_publications,
)

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

    publications = connection.execute("SELECT name FROM publications").fetchall()
    values: list[PublicationIn] = validate_publications(data=publications)

    async with AsyncSessionLocal() as session:
        for i in range(0, len(values), CHUNK_SIZE):
            chunk: list[str] = values[i : i + CHUNK_SIZE]
            insert_pub = insert(Publication).values(chunk)

            async with session.begin():
                do_update_stmt = insert_pub.on_conflict_do_update(
                    index_elements=["name"],
                    set_={"updated_on": datetime.utcnow()},
                )
                await session.execute(do_update_stmt)
                await session.commit()


if __name__ == "__main__":
    conn: duckdb.DuckDBPyConnection = load_csv_into_duckdb(
        file_path="/Users/williamle/Downloads/medium_data.csv"
    )
    conn = create_publications_table(conn)
    conn = asyncio.run(import_publications_to_postgres(conn))
