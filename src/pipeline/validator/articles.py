from datetime import date
from typing import Optional
from uuid import UUID

import msgspec


class ArticleDict(msgspec.Struct, dict=True):
    """msgspec validator for Article data from fetchall results."""

    url: str
    title: str
    subtitle: Optional[str] = None
    publication_id: UUID
    date_published: Optional[date] = None

    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}


def validate_articles(data: list[dict]) -> list[ArticleDict]:
    """Validate a list of article dictionaries from fetchall results."""
    return [ArticleDict(row[0]).to_dict() for row in data]
