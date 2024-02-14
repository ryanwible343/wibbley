from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import registry
from src.model import Shape

mapper_registry = registry()

shape = Table(
    "shape",
    mapper_registry.metadata,
    Column("id", UUID, primary_key=True),
    Column("type", String),
    Column("volume", Integer),
)


def map_orm_to_model():
    mapper_registry.map_imperatively(Shape, shape)
