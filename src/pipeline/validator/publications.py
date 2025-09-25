import msgspec


class PublicationIn(msgspec.Struct, dict=True):
    """msgspec validator for Publication data from fetchall results."""

    name: str

    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}


def validate_publications(data: list[tuple]) -> list[PublicationIn]:
    """Validate a list of publication dictionaries from fetchall results."""
    return [PublicationIn(row[0]).to_dict() for row in data]
