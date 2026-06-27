"""
异步数据库连接池 (SQLAlchemy + asyncpg)
架构原则: 异步连接池 + ORM 映射 TimescaleDB 超表
"""
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import get_settings

settings = get_settings()

engine = create_async_engine(
    settings.database_url,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
    echo=settings.fastapi_debug,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    """ORM 基类"""
    pass


async def get_db() -> AsyncSession:
    """FastAPI 依赖注入: 获取数据库会话"""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_redis():
    """FastAPI 依赖注入: 获取 Redis 连接 (连接池复用)"""
    import redis.asyncio as aioredis
    # 使用连接池而非每次新建连接
    r = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        max_connections=20,
    )
    try:
        yield r
    finally:
        await r.aclose()
