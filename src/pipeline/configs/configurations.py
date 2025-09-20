import functools
import os

import msgspec
from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig(msgspec.Struct):
    """Database configuration using msgspec"""

    name: str
    username: str
    password: str
    host: str = "localhost"
    port: int = 5432

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create database config from environment variables"""
        return cls(
            name=os.getenv("DB_NAME"),
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
        )

    def generate_db_url(self, use_async: bool = True) -> str:
        """Get asynchronous database URL for asyncpg"""
        dbapi: str = "asyncpg" if use_async else "psycopg2"
        return f"postgresql+{dbapi}://{self.username}:{self.password}@{self.host}:{self.port}/{self.name}"


class Configuration(msgspec.Struct):
    """Main application configuration"""

    database: DatabaseConfig
    environment: str = "development"

    @classmethod
    def from_env(cls) -> "Configuration":
        """Create configuration from environment variables"""
        return cls(
            database=DatabaseConfig.from_env(),
            environment=os.getenv("ENVIRONMENT", "development"),
        )


# Global configuration instance
@functools.cache
def get_configs() -> Configuration:
    return Configuration.from_env()


Configurations: Configuration = get_configs()
