"""
Query Registry for S&OP Streamlit Application

All SQL queries must be registered here for testability and fail-fast error handling.
"""

from typing import Dict, Optional
from dataclasses import dataclass

_REGISTERED_QUERIES: Dict[str, 'RegisteredQuery'] = {}


@dataclass
class RegisteredQuery:
    """A registered SQL query with metadata for testing."""
    name: str
    sql: str
    description: str
    min_rows: int = 0


def register_query(name: str, sql: str, description: str, min_rows: int = 0) -> str:
    """
    Register a SQL query for testability.
    
    Args:
        name: Unique identifier for the query
        sql: The SQL statement
        description: Human-readable description
        min_rows: Minimum expected rows (0 = any)
    
    Returns:
        The SQL string for use in queries
    """
    _REGISTERED_QUERIES[name] = RegisteredQuery(
        name=name,
        sql=sql,
        description=description,
        min_rows=min_rows
    )
    return sql


def get_all_queries() -> Dict[str, RegisteredQuery]:
    """Return all registered queries for testing."""
    return _REGISTERED_QUERIES.copy()


def get_query(name: str) -> Optional[RegisteredQuery]:
    """Get a specific registered query by name."""
    return _REGISTERED_QUERIES.get(name)

