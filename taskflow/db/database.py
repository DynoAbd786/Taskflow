"""
Database connection and session management for TaskFlow.
Handles database initialization, connection pooling, and session lifecycle.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from taskflow.db.models import Base
from taskflow.utils.config import get_config


class DatabaseManager:
    """Manages database connections and sessions."""

    def __init__(self, database_url: Optional[str] = None):
        self.config = get_config()
        self.database_url = database_url or self.config.database.url
        self.async_database_url = database_url or self.config.database.async_url

        self.engine = None
        self.async_engine = None
        self.session_factory = None
        self.async_session_factory = None

        self._initialized = False

    def initialize(self) -> None:
        """Initialize database engines and session factories."""
        if self._initialized:
            return

        self.engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=self.config.database.pool_size,
            max_overflow=self.config.database.max_overflow,
            pool_pre_ping=True,
            echo=self.config.debug,
        )

        self.async_engine = create_async_engine(
            self.async_database_url,
            poolclass=QueuePool,
            pool_size=self.config.database.pool_size,
            max_overflow=self.config.database.max_overflow,
            pool_pre_ping=True,
            echo=self.config.debug,
        )

        self.session_factory = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
        )

        self.async_session_factory = async_sessionmaker(
            bind=self.async_engine,
            class_=AsyncSession,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )

        self._initialized = True

    async def create_tables(self) -> None:
        """Create all database tables."""
        if not self._initialized:
            self.initialize()

        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_tables(self) -> None:
        """Drop all database tables."""
        if not self._initialized:
            self.initialize()

        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    def get_session(self) -> Session:
        """Get a synchronous database session."""
        if not self._initialized:
            self.initialize()

        return self.session_factory()

    def get_async_session(self) -> AsyncSession:
        """Get an asynchronous database session."""
        if not self._initialized:
            self.initialize()

        return self.async_session_factory()

    @asynccontextmanager
    async def session_scope(self) -> AsyncGenerator[AsyncSession, None]:
        """Provide a transactional scope around database operations."""
        if not self._initialized:
            self.initialize()

        session = self.async_session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def close(self) -> None:
        """Close database connections."""
        if self.async_engine:
            await self.async_engine.dispose()

        if self.engine:
            self.engine.dispose()

        self._initialized = False

    async def health_check(self) -> dict:
        """Perform database health check."""
        if not self._initialized:
            self.initialize()

        try:
            async with self.session_scope() as session:
                result = await session.execute("SELECT 1")
                await result.fetchone()

            return {
                "status": "healthy",
                "database": "connected",
                "url": self.database_url.split("@")[-1],  # Hide credentials
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(e),
            }


_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def set_database_manager(manager: DatabaseManager) -> None:
    """Set the global database manager instance."""
    global _db_manager
    _db_manager = manager


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Get database session context manager."""
    manager = get_database_manager()
    async with manager.session_scope() as session:
        yield session


class Repository:
    """Base repository class for database operations."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self.session.commit()

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.session.rollback()

    async def refresh(self, instance) -> None:
        """Refresh an instance from the database."""
        await self.session.refresh(instance)

    async def flush(self) -> None:
        """Flush pending changes to the database."""
        await self.session.flush()


def init_database() -> None:
    """Initialize the database connection."""
    manager = get_database_manager()
    manager.initialize()


async def create_database_tables() -> None:
    """Create all database tables."""
    manager = get_database_manager()
    await manager.create_tables()


async def close_database() -> None:
    """Close database connections."""
    manager = get_database_manager()
    await manager.close()
