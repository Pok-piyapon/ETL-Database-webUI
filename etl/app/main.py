"""
ETL Pipeline with Python + MySQL Connector + Pandas
Quality like AWS Glue ETL
-----------------------------------------
Extract → Transform → Load
Optimized for Medical Database (smartmedkku_2025.10.11)
"""

import os
import sys
import time
import logging
import threading
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import mysql.connector
import pandas as pd
import numpy as np
import polars as pl
import aiomysql
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Import cache storage
from cache_storage import cache_storage, monitor_cache

# Load environment variables
load_dotenv()

# Import monitor (optional - won't crash if not available)
try:
    from monitor import start_monitor_server, update_state, get_state, add_log
    MONITOR_AVAILABLE = True
except ImportError:
    MONITOR_AVAILABLE = False
    def update_state(*args, **kwargs): pass
    def get_state(): return {}
    def add_log(*args, **kwargs): pass

# Import metadata helper
from db_metadata import get_tables, get_primary_key_columns

# -----------------------------------------
# 🔹 LOGGING CONFIGURATION
# -----------------------------------------
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"{log_dir}/etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)


# -----------------------------------------
# 🔹 CONFIGURATION
# -----------------------------------------
def get_env(key: str, default: str = "") -> str:
    """Get environment variable."""
    return os.getenv(key, default)


def get_dynamic_db_name() -> str:
    """
    Get dynamic database name with date/time placeholders replaced
    
    Placeholders:
    - {YYYY} = Year (2025)
    - {MM} = Month (01-12)
    - {DD} = Day (01-31)
    - {HH} = Hour (00-23)
    - {mm} = Minute (00-59)
    - {ss} = Second (00-59)
    
    Example: medkku_{YYYY}_{MM}_{DD} → medkku_2025_11_08
    """
    from datetime import datetime
    
    db_name = get_env('DST_DB_NAME', 'test')
    is_dynamic = get_env('DST_DB_DYNAMIC', 'false').lower() == 'true'
    
    if not is_dynamic:
        return db_name
    
    # Get current datetime
    now = datetime.now()
    
    # Replace placeholders
    db_name = db_name.replace('{YYYY}', now.strftime('%Y'))
    db_name = db_name.replace('{MM}', now.strftime('%m'))
    db_name = db_name.replace('{DD}', now.strftime('%d'))
    db_name = db_name.replace('{HH}', now.strftime('%H'))
    db_name = db_name.replace('{mm}', now.strftime('%M'))
    db_name = db_name.replace('{ss}', now.strftime('%S'))
    
    logger.info(f"🗓️  Dynamic database name: {db_name}")
    return db_name


def calculate_dynamic_config(total_rows: int) -> dict:
    """
    Calculate STABLE configuration focused on COMPLETION not SPEED
    Conservative settings to ensure all tables complete successfully
    
    Args:
        total_rows: Total number of rows in table
        
    Returns:
        dict with stable batch_size, num_producers, num_consumers
    """
    # Get configuration (conservative defaults)
    batch_size = int(get_env("BATCH_SIZE", "5000"))
    max_workers = int(get_env("MAX_WORKERS", "20"))
    
    # Use FIXED conservative configuration for stability
    # Focus: Complete all tables reliably, not speed
    
    if total_rows < 1000:
        # Tiny table: minimal workers
        num_producers = 2
        num_consumers = 2
        batch_size = 1000
    elif total_rows < 10000:
        # Small table: few workers
        num_producers = 3
        num_consumers = 3
        batch_size = 2000
    elif total_rows < 100000:
        # Medium table: moderate workers
        num_producers = 5
        num_consumers = 5
        batch_size = 5000
    elif total_rows < 1000000:
        # Large table: balanced workers
        num_producers = 8
        num_consumers = 8
        batch_size = 5000
    else:
        # Very large table: still conservative
        num_producers = 10
        num_consumers = 10
        batch_size = 5000
    
    # Never exceed max_workers
    total_workers = num_producers + num_consumers
    if total_workers > max_workers:
        num_producers = max_workers // 2
        num_consumers = max_workers - num_producers
    
    # Ensure all values are integers
    batch_size = int(batch_size)
    num_producers = int(num_producers)
    num_consumers = int(num_consumers)
    
    logger.info(f"📊 Stable Configuration (Focus: Completion):")
    logger.info(f"   Table size: {total_rows:,} rows")
    logger.info(f"   Batch size: {batch_size:,} rows")
    logger.info(f"   Producers: {num_producers}")
    logger.info(f"   Consumers: {num_consumers}")
    logger.info(f"   Strategy: Conservative & Reliable")
    
    return {
        'batch_size': int(batch_size),
        'num_producers': int(num_producers),
        'num_consumers': int(num_consumers)
    }


# -----------------------------------------
# 🔹 AIOMYSQL CONNECTION POOL
# -----------------------------------------
db_pool_src = None
db_pool_dst = None

async def init_db_pools():
    """Initialize async database connection pools for source and destination"""
    global db_pool_src, db_pool_dst
    
    # Source pool (minimal for stability)
    db_pool_src = await aiomysql.create_pool(
        host=get_env('SRC_DB_HOST', 'localhost'),
        port=int(get_env('SRC_DB_PORT', '3306')),
        user=get_env('SRC_DB_USER', 'root'),
        password=get_env('SRC_DB_PASSWORD', ''),
        db=get_env('SRC_DB_NAME', 'test'),
        minsize=2,
        maxsize=10,
        autocommit=False,
        charset='utf8mb4',
        connect_timeout=60,
        pool_recycle=3600
    )
    
    # Destination pool (minimal for stability) - Use dynamic database name
    dst_db_name = get_dynamic_db_name()
    db_pool_dst = await aiomysql.create_pool(
        host=get_env('DST_DB_HOST', 'localhost'),
        port=int(get_env('DST_DB_PORT', '3306')),
        user=get_env('DST_DB_USER', 'root'),
        password=get_env('DST_DB_PASSWORD', ''),
        db=dst_db_name,
        minsize=3,
        maxsize=15,
        autocommit=False,
        charset='utf8mb4',
        connect_timeout=60,
        pool_recycle=3600
    )
    
    logger.info("✅ Database pools initialized (SRC: 2-10, DST: 3-15 connections - STABLE MODE)")

async def close_db_pools():
    """Close database connection pools"""
    global db_pool_src, db_pool_dst
    
    if db_pool_src:
        db_pool_src.close()
        await db_pool_src.wait_closed()
    
    if db_pool_dst:
        db_pool_dst.close()
        await db_pool_dst.wait_closed()
    
    logger.info("✅ Database pools closed")


def get_connection_params(prefix: str) -> Dict:
    """Get database connection parameters from .env"""
    # Use dynamic database name for destination
    if prefix == "DST":
        db_name = get_dynamic_db_name()
    else:
        db_name = get_env(f"{prefix}_DB_NAME")
    
    return {
        "host": get_env(f"{prefix}_DB_HOST"),
        "port": int(get_env(f"{prefix}_DB_PORT", "3306")),
        "database": db_name,
        "user": get_env(f"{prefix}_DB_USER"),
        "password": get_env(f"{prefix}_DB_PASSWORD"),
        "connect_timeout": 60,
        "autocommit": False,
        "use_pure": False,  # Use C extension for faster protocol
        "consume_results": True,
        "get_warnings": False,
        "raise_on_warnings": False
    }


def get_sqlalchemy_engine(prefix: str):
    """Get SQLAlchemy engine for pandas (no warnings)"""
    params = get_connection_params(prefix)
    
    # URL encode password to handle special characters
    password = quote_plus(params['password'])
    
    # Build connection string
    connection_string = (
        f"mysql+pymysql://{params['user']}:{password}@"
        f"{params['host']}:{params['port']}/{params['database']}"
        f"?charset=utf8mb4"
    )
    
    # Create engine with connection pooling
    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,  # Verify connections before using
        pool_recycle=3600,  # Recycle connections after 1 hour
        echo=False
    )
    
    return engine


def configure_session(connection, is_source=False):
    """Configure session variables for maximum performance"""
    try:
        cursor = connection.cursor()
        
        if is_source:
            # Source: READ optimizations
            cursor.execute("SET SESSION transaction_isolation = 'READ-UNCOMMITTED'")
            cursor.execute("SET SESSION net_read_timeout = 3600")
            cursor.execute("SET SESSION net_write_timeout = 3600")
            cursor.execute("SET SESSION max_execution_time = 0")
            try:
                cursor.execute("SET SESSION read_buffer_size = 4194304")  # 4MB
                cursor.execute("SET SESSION read_rnd_buffer_size = 4194304")
                cursor.execute("SET SESSION sort_buffer_size = 4194304")
            except:
                pass
        else:
            # Destination: WRITE optimizations
            try:
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 300")
                cursor.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
                cursor.execute("SET SESSION unique_checks = 0")
                cursor.execute("SET SESSION foreign_key_checks = 0")
                cursor.execute("SET SESSION autocommit = 0")
                cursor.execute("SET SESSION net_read_timeout = 3600")
                cursor.execute("SET SESSION net_write_timeout = 3600")
                cursor.execute("SET SESSION bulk_insert_buffer_size = 33554432")  # 32MB
            except Exception as e:
                logger.debug(f"Some optimizations not available: {e}")
        
        cursor.close()
    except Exception as e:
        pass


# -----------------------------------------
# 🔹 STEP 1: EXTRACT WITH LIMIT/OFFSET (ASYNC)
# -----------------------------------------
async def extract_chunk_with_offset_polars(table: str, offset: int, limit: int) -> pl.DataFrame:
    """
    Extract data chunk using Polars (5-10x faster than Pandas)
    
    Args:
        table: Table name
        offset: Starting offset
        limit: Number of rows to fetch
        
    Returns:
        Polars DataFrame
    """
    try:
        # Use Pandas first to handle MySQL datetime issues, then convert to Polars
        loop = asyncio.get_event_loop()
        
        # Extract with Pandas (handles invalid datetime)
        df_pandas = await extract_chunk_with_offset(table, offset, limit)
        
        # Convert to Polars (faster processing)
        def _to_polars():
            return pl.from_pandas(df_pandas)
        
        df = await loop.run_in_executor(None, _to_polars)
        
        logger.debug(f"📥 Extracted {len(df):,} rows using Polars (offset={offset})")
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Error extracting chunk from {table}: {e}")
        if MONITOR_AVAILABLE:
            add_log(f"Extract error: {table} - {str(e)[:100]}", "ERROR")
        raise

async def extract_chunk_with_offset(table: str, offset: int, limit: int) -> pd.DataFrame:
    """
    Extract data chunk using LIMIT/OFFSET
    
    Args:
        table: Table name to extract
        offset: Starting row offset
        limit: Number of rows to fetch
    
    Returns:
        DataFrame with extracted chunk
    """
    engine = None
    try:
        loop = asyncio.get_event_loop()
        engine = await loop.run_in_executor(None, get_sqlalchemy_engine, "SRC")
        
        # Build query with LIMIT/OFFSET
        query = f"SELECT * FROM `{table}` LIMIT {limit} OFFSET {offset}"
        
        logger.info(f"📥 Extracting chunk from {table} (offset={offset:,}, limit={limit:,})...")
        
        # Read data chunk (async)
        def _read_sql():
            return pd.read_sql(query, engine)
        
        df = await loop.run_in_executor(None, _read_sql)
        
        logger.info(f"✓ Extracted {len(df):,} records from {table} (offset={offset:,})")
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Error extracting chunk from {table}: {e}")
        raise
    finally:
        if engine:
            engine.dispose()


async def extract_data(table: str, chunk_size: int = 50000, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Extract data from source database using Pandas + SQLAlchemy
    
    Args:
        table: Table name to extract
        chunk_size: Number of rows per chunk
        limit: Optional limit for testing
    
    Returns:
        DataFrame with extracted data
    """
    engine = None
    try:
        # Get SQLAlchemy engine (no warnings!)
        loop = asyncio.get_event_loop()
        engine = await loop.run_in_executor(None, get_sqlalchemy_engine, "SRC")
        
        # Build query
        query = f"SELECT * FROM `{table}`"
        if limit:
            query += f" LIMIT {limit}"
        
        logger.info(f"📥 Extracting data from {table}...")
        
        # Read data in chunks (async)
        def _read_sql():
            chunks = []
            for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
                chunks.append(chunk)
                logger.info(f"  Extracted chunk: {len(chunk):,} rows")
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        
        df = await loop.run_in_executor(None, _read_sql)
        
        logger.info(f"✓ Extracted {len(df):,} records from {table}")
        
        if MONITOR_AVAILABLE:
            add_log(f"Extracted {len(df):,} rows from {table}", "INFO")
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Error extracting from {table}: {e}")
        if MONITOR_AVAILABLE:
            add_log(f"Extract error: {table} - {str(e)[:100]}", "ERROR")
        raise
    finally:
        if engine:
            engine.dispose()


# -----------------------------------------
# 🔹 STEP 2: TRANSFORM (ASYNC)
# -----------------------------------------
async def transform_data_polars(df: pl.DataFrame, table: str) -> pl.DataFrame:
    """
    Transform data using Polars (5-10x faster than Pandas)
    
    Args:
        df: Polars DataFrame
        table: Table name
        
    Returns:
        Transformed Polars DataFrame
    """
    try:
        logger.info(f"🔄 Transforming data for {table} (Polars)...")
        
        loop = asyncio.get_event_loop()
        
        def _transform_polars():
            nonlocal df
            original_count = len(df)
            
            # 1. Remove duplicates (Polars is much faster)
            df = df.unique()
            duplicates_removed = original_count - len(df)
            if duplicates_removed > 0:
                logger.info(f"  Removed {duplicates_removed:,} duplicate rows")
            
            # 2. Clean string columns - strip whitespace
            string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]
            if string_cols:
                df = df.with_columns([
                    pl.col(col).str.strip_chars() for col in string_cols
                ])
            
            # 3. Fill nulls in numeric columns
            numeric_cols = [col for col in df.columns if df[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]]
            if numeric_cols:
                df = df.with_columns([
                    pl.col(col).fill_null(0) for col in numeric_cols
                ])
            
            # 4. Replace NaN and Inf in float columns
            float_cols = [col for col in df.columns if df[col].dtype in [pl.Float64, pl.Float32]]
            if float_cols:
                for col in float_cols:
                    df = df.with_columns([
                        pl.when(pl.col(col).is_nan() | pl.col(col).is_infinite())
                        .then(pl.lit(0.0))
                        .otherwise(pl.col(col))
                        .alias(col)
                    ])
            
            # 5. Data quality report
            null_counts = df.null_count()
            total_nulls = sum(null_counts.row(0))
            if total_nulls > 0:
                logger.info(f"  NULL values found:")
                for i, col in enumerate(df.columns):
                    count = null_counts.row(0)[i]
                    if count > 0:
                        logger.info(f"    - {col}: {count:,} NULLs")
            
            return df
        
        df = await loop.run_in_executor(None, _transform_polars)
        
        logger.info(f"✓ Transformation complete: {len(df):,} rows (Polars)")
        
        if MONITOR_AVAILABLE:
            add_log(f"Transformed {len(df):,} rows for {table} (Polars)", "INFO")
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Error transforming {table}: {e}")
        if MONITOR_AVAILABLE:
            add_log(f"Transform error: {table} - {str(e)[:100]}", "ERROR")
        raise

async def transform_data(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """
    Transform data with quality checks (AWS Glue style)
    
    Transformations:
    - Remove duplicates
    - Handle NULL values
    - Standardize data types
    - Data quality validation
    """
    try:
        logger.info(f"🔄 Transforming data for {table}...")
        
        # Run transformation in thread pool (CPU-bound)
        loop = asyncio.get_event_loop()
        
        def _transform():
            nonlocal df
            original_count = len(df)
            
            # 1. Remove exact duplicates
            df = df.drop_duplicates()
            duplicates_removed = original_count - len(df)
            if duplicates_removed > 0:
                logger.info(f"  Removed {duplicates_removed:,} duplicate rows")
            
            # 2. Handle datetime columns (suppress warnings)
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    try:
                        df[col] = pd.to_datetime(
                            df[col], 
                            format='mixed',
                            errors='coerce',
                            dayfirst=False
                        )
                    except:
                        pass
            
            # 3. Standardize string columns
            string_cols = df.select_dtypes(include=['object']).columns
            for col in string_cols:
                try:
                    df[col] = df[col].str.strip()
                except:
                    pass
            
            # 4. Convert numeric columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass
            
            # 5. Data quality report
            null_counts = df.isnull().sum()
            if null_counts.sum() > 0:
                logger.info(f"  NULL values found:")
                for col, count in null_counts[null_counts > 0].items():
                    logger.info(f"    - {col}: {count:,} NULLs")
            
            return df
        
        df = await loop.run_in_executor(None, _transform)
        
        logger.info(f"✓ Transformation complete: {len(df):,} rows")
        
        if MONITOR_AVAILABLE:
            add_log(f"Transformed {len(df):,} rows for {table}", "INFO")
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Error transforming {table}: {e}")
        if MONITOR_AVAILABLE:
            add_log(f"Transform error: {table} - {str(e)[:100]}", "ERROR")
        raise


# -----------------------------------------
# 🔹 STEP 3: LOAD (ASYNC)
# -----------------------------------------
async def load_data_aiomysql(df: pd.DataFrame, table: str, pk_columns: List[str], batch_size: int = 5000):
    """
    Load data using aiomysql (3x faster than mysql-connector)
    
    Args:
        df: Pandas DataFrame
        table: Table name
        pk_columns: Primary key columns
        batch_size: Batch size for inserts
    """
    try:
        logger.info(f"📤 Loading data to {table} (aiomysql)...")
        
        # Prepare data conversion function
        def prepare_batch_data(batch_df):
            # Replace NaT/NaN with None
            batch_df = batch_df.replace({pd.NaT: None, pd.NA: None, np.nan: None})
            batch_df = batch_df.where(pd.notnull(batch_df), None)
            
            # Convert datetime columns to string
            for col in batch_df.columns:
                if pd.api.types.is_datetime64_any_dtype(batch_df[col]):
                    batch_df[col] = batch_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # Convert to list of tuples and clean up any remaining nan/inf values
            data = []
            for row in batch_df.to_numpy():
                cleaned_row = []
                for val in row:
                    if val is None:
                        cleaned_row.append(None)
                    elif isinstance(val, float):
                        if np.isnan(val) or np.isinf(val):
                            cleaned_row.append(None)
                        else:
                            cleaned_row.append(val)
                    elif isinstance(val, str):
                        if val.lower() in ('nan', 'inf', '-inf', 'null', 'none'):
                            cleaned_row.append(None)
                        else:
                            cleaned_row.append(val)
                    else:
                        cleaned_row.append(val)
                data.append(tuple(cleaned_row))
            
            return data
        
        # Prepare data
        columns = df.columns.tolist()
        data = prepare_batch_data(df)
        
        # Build UPSERT statement
        columns_str = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        
        if pk_columns:
            update_cols = [col for col in columns if col not in pk_columns]
            update_str = ", ".join([f"`{col}`=VALUES(`{col}`)" for col in update_cols])
            insert_stmt = f"""
                INSERT INTO `{table}` ({columns_str})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_str}
            """
        else:
            insert_stmt = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"
        
        # Get connection from pool
        async with db_pool_dst.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    # Optimize for bulk insert
                    await cursor.execute("SET SESSION foreign_key_checks=0")
                    await cursor.execute("SET SESSION unique_checks=0")
                    
                    # Batch insert
                    total_rows = len(data)
                    for i in range(0, total_rows, batch_size):
                        batch = data[i:i + batch_size]
                        await cursor.executemany(insert_stmt, batch)
                        logger.debug(f"  Inserted batch {i//batch_size + 1}: {len(batch):,} rows")
                    
                    # Commit
                    await conn.commit()
                    
                    # Re-enable checks
                    await cursor.execute("SET SESSION foreign_key_checks=1")
                    await cursor.execute("SET SESSION unique_checks=1")
                    
                    logger.info(f"✓ Loaded {total_rows:,} records to {table} (aiomysql)")
                    
                    if MONITOR_AVAILABLE:
                        add_log(f"Loaded {total_rows:,} rows to {table}", "INFO")
                    
                except Exception as e:
                    await conn.rollback()
                    logger.error(f"❌ Batch insert failed: {e}")
                    
                    # Fallback: row-by-row with error handling
                    logger.warning("Falling back to row-by-row insertion...")
                    success_count = 0
                    
                    for row in data:
                        try:
                            await cursor.execute(insert_stmt, row)
                            success_count += 1
                        except Exception as row_error:
                            logger.debug(f"Row insert failed: {row_error}")
                            continue
                    
                    await conn.commit()
                    logger.info(f"✓ Loaded {success_count:,}/{len(data):,} records (row-by-row)")
                    
                    if MONITOR_AVAILABLE:
                        add_log(f"Loaded {success_count:,} rows to {table} (with errors)", "WARNING")
                    
    except Exception as e:
        logger.error(f"✗ Error loading data to {table}: {e}")
        if MONITOR_AVAILABLE:
            add_log(f"Load error: {table} - {str(e)[:100]}", "ERROR")
        raise

async def load_data(df: pd.DataFrame, table: str, pk_columns: List[str], batch_size: int = 50000):
    """
    Load data to destination database with UPSERT
    
    Args:
        df: DataFrame to load
        table: Target table name
        pk_columns: Primary key columns for UPSERT
        batch_size: Batch size for inserts
    """
    try:
        loop = asyncio.get_event_loop()
        
        # Get connection params
        dst_params = await loop.run_in_executor(None, get_connection_params, "DST")
        
        # Connect to database (async) - use lambda to handle kwargs
        def _connect():
            return mysql.connector.connect(**dst_params)
        
        conn = await loop.run_in_executor(None, _connect)
        await loop.run_in_executor(None, configure_session, conn, False)
        
        # Optimize for bulk insert
        def _optimize_connection():
            cursor = conn.cursor()
            # Disable autocommit for better performance
            conn.autocommit = False
            # Disable foreign key checks for faster inserts
            cursor.execute("SET SESSION foreign_key_checks=0")
            # Disable unique checks temporarily
            cursor.execute("SET SESSION unique_checks=0")
            return cursor
        
        cursor = await loop.run_in_executor(None, _optimize_connection)
        
        logger.info(f"📤 Loading data to {table}...")
        
        # Get columns
        columns = df.columns.tolist()
        columns_str = ", ".join([f"`{col}`" for col in columns])
        
        # Build UPSERT statement
        placeholders = ", ".join(["%s"] * len(columns))
        
        if pk_columns:
            # UPSERT: INSERT ... ON DUPLICATE KEY UPDATE
            update_cols = [col for col in columns if col not in pk_columns]
            update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_cols])
            
            sql_template = f"""
                INSERT INTO `{table}` ({columns_str}) 
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
        else:
            # No PK: Simple INSERT (may create duplicates)
            sql_template = f"""
                INSERT INTO `{table}` ({columns_str}) 
                VALUES ({placeholders})
            """
        
        # Prepare data conversion function (vectorized)
        def prepare_batch_data(batch_df):
            # Replace NaT/NaN with None
            batch_df = batch_df.replace({pd.NaT: None, pd.NA: None, np.nan: None})
            batch_df = batch_df.where(pd.notnull(batch_df), None)
            
            # Convert datetime columns to string
            for col in batch_df.columns:
                if pd.api.types.is_datetime64_any_dtype(batch_df[col]):
                    batch_df[col] = batch_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # Convert to list of tuples and clean up any remaining nan/inf values
            data = []
            for row in batch_df.to_numpy():
                cleaned_row = []
                for val in row:
                    # Check for nan, inf, or string 'nan'
                    if val is None:
                        cleaned_row.append(None)
                    elif isinstance(val, float):
                        if np.isnan(val) or np.isinf(val):
                            cleaned_row.append(None)
                        else:
                            cleaned_row.append(val)
                    elif isinstance(val, str):
                        if val.lower() in ('nan', 'inf', '-inf', 'null', 'none'):
                            cleaned_row.append(None)
                        else:
                            cleaned_row.append(val)
                    else:
                        cleaned_row.append(val)
                data.append(tuple(cleaned_row))
            
            return data
        
        # Load in batches
        total_loaded = 0
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size].copy()
            
            # Convert DataFrame to list of tuples (vectorized - FAST!)
            batch_data = prepare_batch_data(batch_df)
            
            # Execute batch insert with better error handling (async)
            try:
                await loop.run_in_executor(None, cursor.executemany, sql_template, batch_data)
                await loop.run_in_executor(None, conn.commit)
                total_loaded += len(batch_data)
                
                # Log every 100k rows instead of every batch
                if total_loaded % 100000 == 0 or total_loaded == len(df):
                    logger.info(f"  Loaded batch: {total_loaded:,}/{len(df):,} rows")
            except Exception as batch_error:
                error_str = str(batch_error)
                logger.warning(f"  Batch insert failed: {error_str[:200]}")
                
                # Check if it's max_allowed_packet error
                if '1153' in error_str or 'max_allowed_packet' in error_str:
                    logger.warning(f"  Packet too large, splitting batch into smaller chunks...")
                    
                    # Rollback failed transaction
                    try:
                        await loop.run_in_executor(None, conn.rollback)
                    except:
                        pass
                    
                    # Split batch into smaller chunks (1/50 of original, minimum 10 rows)
                    smaller_batch_size = max(10, len(batch_data) // 50)
                    success_count = 0
                    
                    logger.info(f"  Splitting {len(batch_data)} rows into batches of {smaller_batch_size} rows")
                    
                    for j in range(0, len(batch_data), smaller_batch_size):
                        small_batch = batch_data[j:j + smaller_batch_size]
                        try:
                            await loop.run_in_executor(None, cursor.executemany, sql_template, small_batch)
                            await loop.run_in_executor(None, conn.commit)
                            success_count += len(small_batch)
                            
                            # Log progress every 10 batches
                            if (j // smaller_batch_size) % 10 == 0:
                                logger.info(f"  Progress: {success_count}/{len(batch_data)} rows")
                        except Exception as small_error:
                            logger.warning(f"  Small batch failed, trying row-by-row for {len(small_batch)} rows...")
                            
                            # Rollback
                            try:
                                await loop.run_in_executor(None, conn.rollback)
                            except:
                                pass
                            
                            # Row-by-row for this small batch
                            for row_data in small_batch:
                                try:
                                    await loop.run_in_executor(None, cursor.execute, sql_template, row_data)
                                    await loop.run_in_executor(None, conn.commit)
                                    success_count += 1
                                except Exception as row_error:
                                    # Check if connection lost
                                    if 'Lost connection' in str(row_error):
                                        logger.warning(f"  Connection lost, reconnecting...")
                                        # Reconnect
                                        try:
                                            conn.close()
                                        except:
                                            pass
                                        
                                        conn = mysql.connector.connect(**dst_params)
                                        cursor = conn.cursor()
                                        
                                        # Disable checks again
                                        cursor.execute("SET SESSION foreign_key_checks=0")
                                        cursor.execute("SET SESSION unique_checks=0")
                                        cursor.execute("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'")
                                        
                                        # Retry this row
                                        try:
                                            cursor.execute(sql_template, row_data)
                                            conn.commit()
                                            success_count += 1
                                        except:
                                            continue
                                    continue
                    
                    total_loaded += success_count
                    logger.info(f"  Loaded {success_count}/{len(batch_data)} rows after splitting")
                    
                else:
                    # Other errors: try row-by-row
                    logger.warning(f"  Trying row-by-row...")
                    
                    # Rollback failed transaction
                    try:
                        await loop.run_in_executor(None, conn.rollback)
                    except:
                        pass
                    
                    # Fallback: row-by-row with detailed error logging
                    success_count = 0
                    for idx, row_data in enumerate(batch_data):
                        try:
                            await loop.run_in_executor(None, cursor.execute, sql_template, row_data)
                            await loop.run_in_executor(None, conn.commit)
                            success_count += 1
                        except Exception as row_error:
                            # Check if connection lost
                            if 'Lost connection' in str(row_error):
                                logger.warning(f"  Connection lost, reconnecting...")
                                # Reconnect
                                try:
                                    conn.close()
                                except:
                                    pass
                                
                                conn = mysql.connector.connect(**dst_params)
                                cursor = conn.cursor()
                                
                                # Disable checks again
                                cursor.execute("SET SESSION foreign_key_checks=0")
                                cursor.execute("SET SESSION unique_checks=0")
                                cursor.execute("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'")
                                
                                # Retry this row
                                try:
                                    cursor.execute(sql_template, row_data)
                                    conn.commit()
                                    success_count += 1
                                except:
                                    continue
                            else:
                                # Log first few errors only
                                if idx < 3:
                                    logger.debug(f"  Row {idx} error: {str(row_error)[:150]}")
                            continue
                    
                    total_loaded += success_count
                    
                    if success_count < len(batch_data):
                        logger.warning(f"  Loaded {success_count}/{len(batch_data)} rows from failed batch")
        
        logger.info(f"✓ Loaded {total_loaded:,} records to {table}")
        
        # Re-enable checks
        def _restore_checks():
            cursor.execute("SET SESSION foreign_key_checks=1")
            cursor.execute("SET SESSION unique_checks=1")
            conn.commit()
        
        await loop.run_in_executor(None, _restore_checks)
        
        if MONITOR_AVAILABLE:
            add_log(f"Loaded {total_loaded:,} rows to {table}", "INFO")
        
    except Exception as e:
        logger.error(f"✗ Error loading to {table}: {e}")
        if MONITOR_AVAILABLE:
            add_log(f"Load error: {table} - {str(e)[:100]}", "ERROR")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


# -----------------------------------------
# 🔹 TABLE CREATION (ASYNC)
# -----------------------------------------
async def create_table_if_not_exists(table: str):
    """Create destination table with all TEXT columns and NULL allowed"""
    try:
        loop = asyncio.get_event_loop()
        
        # Get connection params (async)
        src_params = await loop.run_in_executor(None, get_connection_params, "SRC")
        dst_params = await loop.run_in_executor(None, get_connection_params, "DST")
        
        # Get column names from source (async)
        def _get_schema():
            src_conn = mysql.connector.connect(**src_params)
            src_cursor = src_conn.cursor()
            src_cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            columns_info = src_cursor.fetchall()
            
            src_cursor.execute(f"SHOW KEYS FROM `{table}` WHERE Key_name = 'PRIMARY'")
            pk_info = src_cursor.fetchall()
            pk_columns = [row[4] for row in pk_info] if pk_info else []
            
            src_conn.close()
            return columns_info, pk_columns
        
        columns_info, pk_columns = await loop.run_in_executor(None, _get_schema)
        
        # Build CREATE TABLE statement with TEXT columns
        logger.info(f"Creating table {table} (all TEXT, NULL allowed)...")
        
        column_defs = []
        for col_info in columns_info:
            col_name = col_info[0]
            
            # All columns as TEXT NULL, except PK uses VARCHAR
            if col_name in pk_columns:
                # Primary key: VARCHAR(255) NOT NULL (TEXT can't be PK without length)
                column_defs.append(f"`{col_name}` VARCHAR(255) NOT NULL")
            else:
                # Other columns: TEXT NULL (flexible)
                column_defs.append(f"`{col_name}` TEXT NULL")
        
        # Add primary key constraint if exists
        if pk_columns:
            pk_cols_str = ", ".join([f"`{col}`" for col in pk_columns])
            column_defs.append(f"PRIMARY KEY ({pk_cols_str})")
        
        create_stmt = f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                {', '.join(column_defs)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # Create in destination (async)
        def _create_table():
            dst_conn = mysql.connector.connect(**dst_params)
            dst_cursor = dst_conn.cursor()
            
            dst_cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
            dst_cursor.execute(create_stmt)
            dst_conn.commit()
            dst_conn.close()
        
        await loop.run_in_executor(None, _create_table)
        
        logger.info(f"✓ Table {table} created (TEXT columns, NULL allowed)")
        
    except Exception as e:
        logger.error(f"Error creating table {table}: {e}")
        raise


# -----------------------------------------
# 🔹 PRODUCER: Extract Workers
# -----------------------------------------
async def producer_worker(table: str, queue: asyncio.Queue, chunk_size: int, total_rows: int, worker_id: int):
    """
    Producer worker: Extract data chunks and put into queue
    
    Args:
        table: Table name
        queue: Async queue for chunks
        chunk_size: Chunk size (e.g., 1,000,000)
        total_rows: Total rows in table
        worker_id: Worker ID for logging
    """
    try:
        offset = 0
        chunk_num = 0
        
        while offset < total_rows:
            # Extract chunk
            df_chunk = await extract_chunk_with_offset(table, offset, chunk_size)
            
            if len(df_chunk) == 0:
                break
            
            # Transform chunk
            df_transformed = await transform_data(df_chunk, table)
            
            # Put into queue
            await queue.put((chunk_num, df_transformed))
            logger.info(f"🟢 Producer-{worker_id}: Chunk {chunk_num} queued ({len(df_transformed):,} rows)")
            
            offset += chunk_size
            chunk_num += 1
        
        logger.info(f"✓ Producer-{worker_id} completed")
        
    except Exception as e:
        logger.error(f"✗ Producer-{worker_id} error: {e}")
        raise


# -----------------------------------------
# 🔹 CONSUMER: Load Workers
# -----------------------------------------
async def consumer_worker(table: str, queue: asyncio.Queue, pk_columns: List[str], batch_size: int, worker_id: int, stop_event: asyncio.Event):
    """
    Consumer worker that loads data from queue
    """
    try:
        rows_loaded = 0
        
        # Update monitor - initialize worker
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' not in state:
                state['workers'] = {}
            if table not in state['workers']:
                state['workers'][table] = {}
            state['workers'][table][f'consumer-{worker_id}'] = {
                'type': 'consumer',
                'rows_loaded': 0,
                'status': 'running'
            }
            update_state('workers', state['workers'])
        
        while True:
            try:
                # ✅ Wait and compete for data from cache storage (async/await)
                cache_item = await cache_storage.get(timeout=5.0)
                
                if cache_item:
                    # Got data from cache
                    chunk_num = cache_item.chunk_id
                    df_chunk = cache_item.data
                    
                    logger.info(f"🔵 Consumer-{worker_id}: Loading chunk {chunk_num} ({len(df_chunk):,} rows) from cache...")
                    
                    # ✅ Load data with aiomysql (3x faster)
                    await load_data_aiomysql(df_chunk, table, pk_columns, batch_size)
                    
                    # Free memory after loading
                    chunk_size_loaded = len(df_chunk)
                    del df_chunk
                    del cache_item
                    import gc
                    gc.collect()
                    
                    rows_loaded += chunk_size_loaded
                    
                    logger.info(f"✓ Consumer-{worker_id}: Chunk {chunk_num} loaded")
                else:
                    # Try fallback queue if cache timeout
                    try:
                        chunk_num, df_chunk = await asyncio.wait_for(queue.get(), timeout=1.0)
                        
                        logger.info(f"🔵 Consumer-{worker_id}: Loading chunk {chunk_num} from queue...")
                        
                        # ✅ Load data with aiomysql (3x faster)
                        await load_data_aiomysql(df_chunk, table, pk_columns, batch_size)
                        
                        chunk_size_loaded = len(df_chunk)
                        del df_chunk
                        import gc
                        gc.collect()
                        
                        queue.task_done()
                        rows_loaded += chunk_size_loaded
                        
                        logger.info(f"✓ Consumer-{worker_id}: Chunk {chunk_num} loaded from queue")
                    except asyncio.TimeoutError:
                        # No data in cache or queue, check if we should stop
                        if stop_event.is_set():
                            break
                        continue
                
                # Update monitor - progress
                if MONITOR_AVAILABLE:
                    state = get_state()
                    if 'workers' in state and table in state['workers']:
                        state['workers'][table][f'consumer-{worker_id}']['rows_loaded'] = rows_loaded
                        update_state('workers', state['workers'])
                
            except asyncio.TimeoutError:
                # Check if we should stop
                if stop_event.is_set() and queue.empty():
                    break
                continue
        
        logger.info(f"✓ Consumer-{worker_id} completed (no more chunks)")
        
        # Update monitor - completed
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' in state and table in state['workers']:
                state['workers'][table][f'consumer-{worker_id}']['status'] = 'completed'
                update_state('workers', state['workers'])
        
    except Exception as e:
        logger.error(f"✗ Consumer-{worker_id} error: {e}")
        
        # Update monitor - failed
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' in state and table in state['workers']:
                state['workers'][table][f'consumer-{worker_id}']['status'] = 'failed'
                state['workers'][table][f'consumer-{worker_id}']['error'] = str(e)[:50]
                update_state('workers', state['workers'])
        
        raise


# -----------------------------------------
# 🔹 PRODUCER: Extract Workers
# -----------------------------------------
async def producer_worker(table: str, queue: asyncio.Queue, chunk_size: int, total_rows: int, worker_id: int):
    """
    Producer worker: Extract data chunks and put into queue
    
    Args:
        table: Table name
        queue: Async queue for chunks
        chunk_size: Chunk size (e.g., 1,000,000)
        total_rows: Total rows in table
        worker_id: Worker ID for logging
    """
    try:
        offset = 0
        chunk_num = 0
        rows_processed = 0
        
        # Update monitor - initialize worker
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' not in state:
                state['workers'] = {}
            if table not in state['workers']:
                state['workers'][table] = {}
            state['workers'][table][f'producer-{worker_id}'] = {
                'type': 'producer',
                'rows_processed': 0,
                'total_rows': total_rows,
                'status': 'running'
            }
            update_state('workers', state['workers'])
        
        while offset < total_rows:
            # Extract chunk
            df_chunk = await extract_chunk_with_offset(table, offset, chunk_size)
            
            if len(df_chunk) == 0:
                break
            
            # Transform chunk
            df_transformed = await transform_data(df_chunk, table)
            
            # Put into queue
            await queue.put((chunk_num, df_transformed))
            logger.info(f"🟢 Producer-{worker_id}: Chunk {chunk_num} queued ({len(df_transformed):,} rows)")
            
            offset += chunk_size
            chunk_num += 1
            rows_processed += len(df_chunk)
            
            # Update monitor - progress
            if MONITOR_AVAILABLE:
                state = get_state()
                if 'workers' in state and table in state['workers']:
                    state['workers'][table][f'producer-{worker_id}']['rows_processed'] = rows_processed
                    update_state('workers', state['workers'])
        
        logger.info(f"✓ Producer-{worker_id} completed")
        
        # Update monitor - completed
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' in state and table in state['workers']:
                state['workers'][table][f'producer-{worker_id}']['status'] = 'completed'
                update_state('workers', state['workers'])
        
    except Exception as e:
        logger.error(f"✗ Producer-{worker_id} error: {e}")
        
        # Update monitor - failed
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' in state and table in state['workers']:
                state['workers'][table][f'producer-{worker_id}']['status'] = 'failed'
                state['workers'][table][f'producer-{worker_id}']['error'] = str(e)[:50]
                update_state('workers', state['workers'])
        
        raise


async def producer_worker_with_offset(table: str, queue: asyncio.Queue, chunk_size: int, start_offset: int, total_rows: int, worker_id: int):
    """
    Producer worker with custom offset range
    """
    try:
        offset = start_offset
        end_offset = start_offset + total_rows
        chunk_num = start_offset // chunk_size
        rows_processed = 0
        
        logger.info(f"🟢 Producer-{worker_id}: Processing rows {start_offset:,} to {end_offset:,}")
        
        # Update monitor - initialize worker
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' not in state:
                state['workers'] = {}
            if table not in state['workers']:
                state['workers'][table] = {}
            state['workers'][table][f'producer-{worker_id}'] = {
                'type': 'producer',
                'rows_processed': 0,
                'total_rows': total_rows,
                'status': 'running'
            }
            update_state('workers', state['workers'])
        
        while offset < end_offset:
            # ✅ Use fixed chunk_size
            current_chunk_size = min(chunk_size, end_offset - offset)
            
            # Extract chunk
            df_chunk = await extract_chunk_with_offset(table, offset, current_chunk_size)
            
            if len(df_chunk) == 0:
                break
            
            # Transform chunk
            df_transformed = await transform_data(df_chunk, table)
            
            # Free memory
            del df_chunk
            import gc
            gc.collect()
            
            # ✅ Put into queue immediately - consumer processes while we fetch next
            await queue.put((chunk_num, df_transformed))
            logger.info(f"🟢 Producer-{worker_id}: Chunk {chunk_num} queued ({len(df_transformed):,} rows) - fetching next...")
            
            # ✅ Continue to next chunk immediately
            offset += current_chunk_size
            chunk_num += 1
            rows_processed += len(df_transformed)
            
            # Update monitor - progress
            if MONITOR_AVAILABLE:
                state = get_state()
                if 'workers' in state and table in state['workers']:
                    state['workers'][table][f'producer-{worker_id}']['rows_processed'] = rows_processed
                    update_state('workers', state['workers'])
        
        logger.info(f"✓ Producer-{worker_id} completed")
        
        # Update monitor - completed
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' in state and table in state['workers']:
                state['workers'][table][f'producer-{worker_id}']['status'] = 'completed'
                update_state('workers', state['workers'])
        
    except Exception as e:
        logger.error(f"✗ Producer-{worker_id} error: {e}")
        
        # Update monitor - failed
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' in state and table in state['workers']:
                state['workers'][table][f'producer-{worker_id}']['status'] = 'failed'
                state['workers'][table][f'producer-{worker_id}']['error'] = str(e)[:50]
                update_state('workers', state['workers'])
        
        raise


# -----------------------------------------
# 🔹 MAIN ETL RUNNER (Single Table - PRODUCER/CONSUMER)
# -----------------------------------------
async def run_etl_for_table_parallel(table: str, schema: str, num_producers: int = None, num_consumers: int = None):
    """
    Run ETL with parallel producers (extract) and consumers (load)
    Dynamic configuration based on table size
    
    Args:
        table: Table name
        schema: Schema name
        num_producers: Number of producer workers (None = auto-calculate)
        num_consumers: Number of consumer workers (None = auto-calculate)
    """
    try:
        start_time = time.time()
        logger.info("="*80)
        logger.info(f"🚀 Starting Parallel ETL for table: {table} (Dynamic + aiomysql)")
        logger.info("="*80)
        
        # ✅ Initialize database pools if not already done
        global db_pool_src, db_pool_dst
        if db_pool_src is None or db_pool_dst is None:
            await init_db_pools()
        
        # Get table info
        loop = asyncio.get_event_loop()
        src_params = await loop.run_in_executor(None, get_connection_params, "SRC")
        pk_columns = await loop.run_in_executor(None, get_primary_key_columns, src_params, schema, table)
        logger.info(f"Primary key: {pk_columns if pk_columns else 'None'}")
        
        # Get total row count
        def _get_count():
            conn = mysql.connector.connect(**src_params)
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        
        total_rows = await loop.run_in_executor(None, _get_count)
        logger.info(f"Total rows: {total_rows:,}")
        
        if total_rows == 0:
            logger.warning(f"⚠️  Table {table} is empty, skipping...")
            return True
        
        # ✅ Calculate dynamic configuration based on table size
        config = calculate_dynamic_config(total_rows)
        chunk_size = config['batch_size']
        batch_size = chunk_size
        num_producers = config['num_producers']
        num_consumers = config['num_consumers']
        
        # Update monitor with batch/chunk info
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'tables_status' not in state:
                state['tables_status'] = {}
            state['tables_status'][table] = {
                'status': 'processing',
                'progress': 0,
                'error': None,
                'chunk_size': chunk_size,
                'batch_size': batch_size,
                'num_producers': num_producers,
                'num_consumers': num_consumers
            }
            update_state('tables_status', state['tables_status'])
        
        # Create table
        await create_table_if_not_exists(table)
        
        # Create queue and stop event
        queue = asyncio.Queue(maxsize=num_consumers * 2)  # Buffer size
        stop_event = asyncio.Event()
        
        # Calculate rows per producer
        rows_per_producer = total_rows // num_producers
        
        # ✅ Start cache monitor
        cache_monitor_task = asyncio.create_task(monitor_cache(interval=10))
        logger.info("📦 Cache monitor started")
        
        # ✅ Start consumers first - they will wait for data in cache
        logger.info(f"🔵 Starting {num_consumers} consumers (waiting for cache data)...")
        consumer_tasks = [
            asyncio.create_task(
                consumer_worker(table, queue, pk_columns, batch_size, i+1, stop_event)
            )
            for i in range(num_consumers)
        ]
        
        # ✅ Start producers - they will fetch and cache data independently
        logger.info(f"🟢 Starting {num_producers} producers (fetching data)...")
        producer_tasks = []
        
        for i in range(num_producers):
            start_offset = i * rows_per_producer
            # Last producer takes remaining rows
            end_offset = total_rows if i == num_producers - 1 else (i + 1) * rows_per_producer
            producer_rows = end_offset - start_offset
            
            if producer_rows > 0:
                task = asyncio.create_task(
                    producer_worker_with_offset(table, queue, chunk_size, start_offset, producer_rows, i+1)
                )
                producer_tasks.append(task)
        
        # ✅ Run producers and consumers concurrently (fully async/await)
        logger.info("⚡ Producers and Consumers running concurrently...")
        
        # Wait for all producers to finish (consumers still running)
        await asyncio.gather(*producer_tasks)
        logger.info("✓ All producers completed")
        
        # Signal consumers to stop (after all data cached)
        stop_event.set()
        
        # Wait for all consumers to finish processing remaining cache
        await asyncio.gather(*consumer_tasks)
        logger.info("✓ All consumers completed")
        
        # ✅ Stop cache monitor and print stats
        cache_monitor_task.cancel()
        cache_storage.print_stats()
        
        # ✅ Clear cache for next table
        await cache_storage.clear()
        
        # Success
        elapsed = time.time() - start_time
        logger.info("="*80)
        logger.info(f"✓ Parallel ETL completed for {table} in {elapsed:.1f}s")
        logger.info("="*80)
        
        # Update monitor
        if MONITOR_AVAILABLE:
            state = get_state()
            state['tables_status'][table] = {
                'status': 'completed',
                'progress': 100,
                'error': None
            }
            update_state('tables_status', state['tables_status'])
            
            # Clear workers for this table
            if 'workers' in state and table in state['workers']:
                del state['workers'][table]
                update_state('workers', state['workers'])
            
            completed = state.get('completed_tables', 0) + 1
            update_state('completed_tables', completed)
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Parallel ETL failed for {table}: {e}")
        
        # Update monitor
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'tables_status' in state:
                state['tables_status'][table] = {
                    'status': 'failed',
                    'progress': 0,
                    'error': str(e)[:200]
                }
                update_state('tables_status', state['tables_status'])
            
            # Clear workers for this table
            if 'workers' in state and table in state['workers']:
                del state['workers'][table]
                update_state('workers', state['workers'])
            
            failed = state.get('failed_tables', 0) + 1
            update_state('failed_tables', failed)
        
        return False


async def producer_worker_with_offset(table: str, queue: asyncio.Queue, chunk_size: int, start_offset: int, total_rows: int, worker_id: int):
    """
    Producer worker with custom offset range
    """
    try:
        offset = start_offset
        end_offset = start_offset + total_rows
        chunk_num = start_offset // chunk_size
        rows_processed = 0
        
        logger.info(f"🟢 Producer-{worker_id}: Processing rows {start_offset:,} to {end_offset:,}")
        
        # Update monitor - initialize worker
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' not in state:
                state['workers'] = {}
            if table not in state['workers']:
                state['workers'][table] = {}
            state['workers'][table][f'producer-{worker_id}'] = {
                'type': 'producer',
                'rows_processed': 0,
                'total_rows': total_rows,
                'status': 'running'
            }
            update_state('workers', state['workers'])
        
        while offset < end_offset:
            # ✅ Always use full chunk_size - don't reduce based on remaining rows
            # Extract exactly chunk_size rows (or whatever remains if less than chunk_size)
            current_chunk_size = min(chunk_size, end_offset - offset)
            
            # ✅ Extract chunk with Polars (Pandas → Polars conversion)
            df_chunk = await extract_chunk_with_offset_polars(table, offset, current_chunk_size)
            
            if len(df_chunk) == 0:
                break
            
            # ✅ Transform chunk with Polars (5-10x faster)
            df_transformed = await transform_data_polars(df_chunk, table)
            
            # ✅ Convert to Pandas for cache/load
            df_pandas = df_transformed.to_pandas()
            
            # Free memory from original chunks
            del df_chunk
            del df_transformed
            import gc
            gc.collect()
            
            # Log memory usage periodically
            if chunk_num % 10 == 0:
                import psutil
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                logger.debug(f"🟢 Producer-{worker_id}: Memory usage: {memory_mb:.1f} MB")
            
            # ✅ Put into cache storage (async/await - non-blocking)
            # Consumers will compete to get this data
            success = await cache_storage.put(table, chunk_num, df_pandas, worker_id)
            if success:
                logger.info(f"🟢 Producer-{worker_id}: Chunk {chunk_num} cached ({len(df_pandas):,} rows) → consumers competing...")
            else:
                # Fallback to direct queue if cache is full
                await queue.put((chunk_num, df_pandas))
                logger.warning(f"⚠️  Producer-{worker_id}: Cache full, using direct queue for chunk {chunk_num}")
            
            # ✅ Continue fetching next chunk immediately (independent of consumers)
            offset += current_chunk_size
            chunk_num += 1
            rows_processed += len(df_pandas)
            
            # Update monitor - progress
            if MONITOR_AVAILABLE:
                state = get_state()
                if 'workers' in state and table in state['workers']:
                    state['workers'][table][f'producer-{worker_id}']['rows_processed'] = rows_processed
                    update_state('workers', state['workers'])
        
        logger.info(f"✓ Producer-{worker_id} completed")
        
        # Update monitor - completed
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' in state and table in state['workers']:
                state['workers'][table][f'producer-{worker_id}']['status'] = 'completed'
                update_state('workers', state['workers'])
        
    except Exception as e:
        logger.error(f"✗ Producer-{worker_id} error: {e}")
        
        # Update monitor - failed
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'workers' in state and table in state['workers']:
                state['workers'][table][f'producer-{worker_id}']['status'] = 'failed'
                state['workers'][table][f'producer-{worker_id}']['error'] = str(e)[:50]
                update_state('workers', state['workers'])
        
        raise


# -----------------------------------------
# 🔹 MAIN ETL RUNNER (Single Table - ASYNC)
# -----------------------------------------
async def run_etl_for_table(table: str, schema: str):
    """
    Run complete ETL pipeline for a single table
    
    Steps:
    1. Extract from source
    2. Transform data
    3. Create table if needed
    4. Load to destination
    """
    try:
        logger.info("="*80)
        logger.info(f"🚀 Starting ETL for table: {table}")
        logger.info("="*80)
        
        start_time = time.time()
        
        # Update monitor
        if MONITOR_AVAILABLE:
            state = get_state()
            if 'tables_status' not in state:
                state['tables_status'] = {}
            state['tables_status'][table] = {
                'status': 'processing',
                'progress': 0,
                'error': None
            }
            update_state('tables_status', state['tables_status'])
        
        # Get primary key columns (async)
        loop = asyncio.get_event_loop()
        src_params = await loop.run_in_executor(None, get_connection_params, "SRC")
        pk_columns = await loop.run_in_executor(None, get_primary_key_columns, src_params, schema, table)
        logger.info(f"Primary key: {pk_columns if pk_columns else 'None'}")
        
        # Step 1: Extract (async)
        df = await extract_data(table)
        
        if len(df) == 0:
            logger.warning(f"⚠️  Table {table} is empty, skipping...")
            return True
        
        # Update progress
        if MONITOR_AVAILABLE:
            state = get_state()
            state['tables_status'][table]['progress'] = 33
            update_state('tables_status', state['tables_status'])
        
        # Step 2: Transform (async)
        df_transformed = await transform_data(df, table)
        
        # Update progress
        if MONITOR_AVAILABLE:
            state = get_state()
            state['tables_status'][table]['progress'] = 66
            update_state('tables_status', state['tables_status'])
        
        # Step 3: Create table (async)
        await create_table_if_not_exists(table)
        
        # Step 4: Load (async)
        batch_size = int(get_env("BATCH_SIZE", "10000"))
        await load_data(df_transformed, table, pk_columns, batch_size)
        
        # Success
        elapsed = time.time() - start_time
        logger.info("="*80)
        logger.info(f"✓ ETL completed for {table} in {elapsed:.1f}s")
        logger.info("="*80)
        
        # Update monitor
        if MONITOR_AVAILABLE:
            state = get_state()
            state['tables_status'][table]['status'] = 'completed'
            state['tables_status'][table]['progress'] = 100
            update_state('tables_status', state['tables_status'])
            
            completed = state.get('completed_tables', 0) + 1
            update_state('completed_tables', completed)
        
        return True
        
    except Exception as e:
        logger.error(f"✗ ETL failed for {table}: {e}")
        
        # Update monitor
        if MONITOR_AVAILABLE:
            state = get_state()
            if table in state.get('tables_status', {}):
                state['tables_status'][table]['status'] = 'failed'
                state['tables_status'][table]['error'] = str(e)[:100]
                state['tables_status'][table]['progress'] = 0
                update_state('tables_status', state['tables_status'])
            
            failed = state.get('failed_tables', 0) + 1
            update_state('failed_tables', failed)
        
        return False


# -----------------------------------------
# 🔹 MAIN ETL RUNNER (All Tables - ASYNC)
# -----------------------------------------
async def run_etl_all_tables():
    """Run ETL for all tables in source database"""
    logger.info("="*80)
    logger.info("🚀 Starting AWS Glue-Style ETL Pipeline")
    logger.info("="*80)
    
    # Initialize monitor
    if MONITOR_AVAILABLE:
        update_state({
            'status': 'running',
            'start_time': time.time(),
            'mode': 'pandas_etl',
            'tables_status': {}
        })
        add_log("Starting Pandas ETL Pipeline", "INFO")
    
    # Get connection params
    src_params = get_connection_params("SRC")
    schema = src_params['database']
    
    logger.info(f"Source: {src_params['host']}:{src_params['port']}/{src_params['database']}")
    
    dst_params = get_connection_params("DST")
    logger.info(f"Destination: {dst_params['host']}:{dst_params['port']}/{dst_params['database']}")
    
    # Create destination database if dynamic mode is enabled
    is_dynamic = get_env('DST_DB_DYNAMIC', 'false').lower() == 'true'
    if is_dynamic:
        logger.info(f"🗓️  Dynamic database mode enabled - creating new database")
        try:
            # Connect without database to create it
            conn_params = dst_params.copy()
            conn_params.pop('database')
            conn = mysql.connector.connect(**conn_params)
            cursor = conn.cursor()
            
            db_name = dst_params['database']
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"✅ Database '{db_name}' created successfully")
            
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"❌ Error creating database: {e}")
            raise
    
    # Get tables
    all_tables = get_tables(src_params)
    
    # Filter tables
    include = get_env('INCLUDE_TABLES', '')
    exclude = get_env('EXCLUDE_TABLES', '')
    
    tables = all_tables
    if include:
        include_list = [t.strip() for t in include.split(',')]
        tables = [(s, t) for s, t in tables if t in include_list]
    if exclude:
        exclude_list = [t.strip() for t in exclude.split(',')]
        tables = [(s, t) for s, t in tables if t not in exclude_list]
    
    logger.info(f"Found {len(tables)} tables to process")
    
    # Initialize monitor
    if MONITOR_AVAILABLE:
        update_state({
            'total_tables': len(tables),
            'completed_tables': 0,
            'failed_tables': 0
        })
    
    # Process each table
    successful = 0
    failed = 0
    
    # Get number of workers from env
    max_workers = int(get_env("MAX_WORKERS", "10"))
    num_producers = max_workers // 2
    num_consumers = max_workers - num_producers
    
    logger.info(f"Worker configuration: {num_producers} producers, {num_consumers} consumers")
    
    for schema, table in tables:
        # Retry configuration
        max_retries = 3
        retry_delay = 5  # seconds
        attempt = 0
        success = False
        
        # Retry loop until success or max retries
        while attempt < max_retries and not success:
            attempt += 1
            
            if attempt > 1:
                logger.warning(f"🔄 Retry attempt {attempt}/{max_retries} for table {table}")
                
                # Update monitor with retry status
                if MONITOR_AVAILABLE:
                    state = get_state()
                    if table in state.get('tables_status', {}):
                        state['tables_status'][table]['status'] = 'retrying'
                        state['tables_status'][table]['error'] = f'Retry attempt {attempt}/{max_retries}'
                        update_state('tables_status', state['tables_status'])
                
                await asyncio.sleep(retry_delay)
            
            # Use parallel ETL with producer/consumer pattern
            success = await run_etl_for_table_parallel(table, schema, num_producers, num_consumers)
            
            if success:
                successful += 1
                logger.info(f"✅ Table {table} completed successfully")
                break
            else:
                if attempt < max_retries:
                    logger.warning(f"❌ Table {table} failed, retrying in {retry_delay}s...")
                else:
                    logger.error(f"❌ Table {table} failed after {max_retries} attempts")
                    failed += 1
    
    # Summary
    logger.info("="*80)
    logger.info("📊 ETL PIPELINE COMPLETED")
    logger.info(f"✓ Successful: {successful} tables")
    logger.info(f"✗ Failed: {failed} tables")
    logger.info("="*80)
    
    # Update monitor
    if MONITOR_AVAILABLE:
        update_state({
            'status': 'completed',
            'end_time': time.time()
        })
        add_log(f"ETL completed: {successful} successful, {failed} failed", "INFO")


# -----------------------------------------
# 🔹 ENTRY POINT
# -----------------------------------------
def main():
    """Entry point - starts monitor and runs async ETL"""
    # Start monitoring web server
    if MONITOR_AVAILABLE:
        monitor_thread = threading.Thread(
            target=start_monitor_server,
            kwargs={'host': '0.0.0.0', 'port': 5000},
            daemon=True
        )
        monitor_thread.start()
        logger.info("="*80)
        logger.info("📊 ETL Monitor Dashboard: http://localhost:5000")
        logger.info("="*80)
        time.sleep(2)
    
    # Run async ETL
    asyncio.run(run_etl_all_tables())


if __name__ == "__main__":
    main()
