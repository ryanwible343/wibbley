from sqlalchemy import Column, DateTime, Index, PrimaryKeyConstraint, String, Table
from sqlalchemy.orm import registry

mapper_registry = registry()

submission = Table(
    "submission",
    mapper_registry.metadata,
    Column("submission_id", String),
    Column("user_id", String),
    Column("file_path", String, nullable=False),
    Column("datetime_created", DateTime, nullable=False),
    Column("status", String, nullable=False),
    PrimaryKeyConstraint("submission_id", "user_id"),
    Index("submission_user_id_idx", "user_id"),
)
