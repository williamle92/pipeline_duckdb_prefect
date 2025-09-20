from datetime import datetime
from typing import List, TYPE_CHECKING
from uuid import uuid4
from sqlalchemy import String, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID

from ..db import Base

if TYPE_CHECKING:
    from .articles import Article


class Publication(Base):
    __tablename__ = "publications"

    id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    created_on: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_on: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


    def __repr__(self) -> str:
        return f"<Publication(id={self.id}, name='{self.name}')>"