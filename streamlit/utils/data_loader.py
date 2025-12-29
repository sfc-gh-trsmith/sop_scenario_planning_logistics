"""
Data Loader for S&OP Streamlit Application

Provides parallel query execution with fail-fast error handling.
Uses ThreadPoolExecutor for concurrent query execution.
"""

import pandas as pd
import logging
import time
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st

logger = logging.getLogger(__name__)


def run_query(session, sql: str, name: str = "query") -> pd.DataFrame:
    """
    Execute a single SQL query with fail-fast error handling.
    
    Args:
        session: Snowflake session
        sql: SQL query string
        name: Query name for error messages
    
    Returns:
        DataFrame with query results
    
    Raises:
        RuntimeError: If query fails or returns None
    """
    try:
        result = session.sql(sql).to_pandas()
        if result is None:
            raise RuntimeError(f"Query '{name}' returned None")
        return result
    except Exception as e:
        raise RuntimeError(f"Query '{name}' failed: {e}") from e


def run_queries_parallel(
    session, 
    queries: Dict[str, str], 
    max_workers: int = 4
) -> Dict[str, pd.DataFrame]:
    """
    Execute multiple independent SQL queries in parallel.
    
    FAIL-FAST: Any query failure raises an exception immediately.
    
    Args:
        session: Snowflake Snowpark session
        queries: Dict mapping names to SQL strings
        max_workers: Max concurrent queries (4 recommended for Snowflake)
    
    Returns:
        Dict mapping query names to result DataFrames
    
    Raises:
        RuntimeError: If any query fails
    """
    if not queries:
        return {}
    
    start_time = time.time()
    results: Dict[str, pd.DataFrame] = {}
    errors: list = []
    
    def execute_query(name: str, query: str) -> tuple:
        """Execute a single query and return (name, result, error)."""
        try:
            df = session.sql(query).to_pandas()
            if df is None:
                raise RuntimeError(f"Query '{name}' returned None")
            return name, df, None
        except Exception as e:
            logger.error(f"Query '{name}' failed: {e}")
            return name, None, str(e)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {
            executor.submit(execute_query, name, query): name
            for name, query in queries.items()
        }
        
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                query_name, result_df, error = future.result()
                if error:
                    errors.append(f"{query_name}: {error}")
                else:
                    results[query_name] = result_df
            except Exception as e:
                errors.append(f"{name}: {e}")
    
    # FAIL-FAST: Raise if any queries failed
    if errors:
        error_msg = f"Query execution failed:\n" + "\n".join(f"  - {e}" for e in errors)
        raise RuntimeError(error_msg)
    
    elapsed = time.time() - start_time
    logger.info(f"Parallel execution: {len(queries)} queries in {elapsed:.2f}s")
    return results


@st.cache_data(ttl=300)
def cached_query(_session, sql: str, name: str = "query") -> pd.DataFrame:
    """
    Execute a query with Streamlit caching (5-minute TTL).
    
    Args:
        _session: Snowflake session (underscore prefix for Streamlit cache)
        sql: SQL query string
        name: Query name for error messages
    
    Returns:
        Cached DataFrame with query results
    """
    return run_query(_session, sql, name)

