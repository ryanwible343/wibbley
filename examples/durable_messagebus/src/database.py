from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("postgresql+asyncpg://local:local@postgres:5432/local")
