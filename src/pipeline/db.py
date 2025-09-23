from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from .configs import Configurations


# Base class for all database models
class Base(DeclarativeBase):
    """Base class for all database models"""

    pass


# Create async engine using configuration
async_engine: AsyncEngine = create_async_engine(
    Configurations.database.generate_db_url(dialect="postgresql+asyncpg"), echo=True
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Get asynchronous database session"""
    async with AsyncSessionLocal() as session:
        yield session
