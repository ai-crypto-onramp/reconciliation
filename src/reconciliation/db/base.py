"""Declarative base + shared metadata for all ORM models."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base for the reconciliation schema."""


metadata = Base.metadata