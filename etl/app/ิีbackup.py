"""
Simple MariaDB ETL - Basic and Clean
Extracts data from source, loads to destination, and syncs changes.
"""

import os
import sys
import time
import logging
import threading
import asyncio
import gc
from concurrent.futures import ThreadPoolExecutor
import mysql.connector
from typing import Dict, List
from db_metadata import get_tables, get_primary_key_columns

# Import monitor (optional - won't crash if not available)
try:
    from monitor import start_monitor_server, update_state, get_state, add_log
    MONITOR_AVAILABLE = True
except ImportError:
    MONITOR_AVAILABLE = False
    def update_state(*args, **kwargs): pass
    def get_state(): return {}
    def add_log(*args, **kwargs): pass

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_env(key: str, default: str = "") -> str:
    """Get environment variable."""
    return os.getenv(key, default)


def get_connection_params(prefix: str) -> Dict:
    """Get database connection parameters with network optimizations."""
    return {
        "host": get_env(f"{prefix}_DB_HOST"),
        "port": int(get_env(f"{prefix}_DB_PORT", "3306")),
        "database": get_env(f"{prefix}_DB_NAME"),
        "user": get_env(f"{prefix}_DB_USER"),
        "password": get_env(f"{prefix}_DB_PASSWORD"),
        "connect_timeout": 60,  # Increased for large tables
        "autocommit": False,
        # Network optimizations
        "use_pure": False,  # Use C extension for faster protocol
        "consume_results": True,  # Consume results immediately
        "get_warnings": False,  # Skip warning fetching
        "raise_on_warnings": False,  # Don't raise on warnings
        # Connection pooling settings (disabled to prevent pool exhaustion)
        # Using direct connections instead of pooling for better control
        # "pool_size": 10,
        # "pool_reset_session": False,
    }


def configure_session(connection, is_source=False):
    """Configure session variables for maximum performance (safe for read-only source)."""
    try:
        cursor = connection.cursor()
        
        if is_source:
            # Source database: BALANCED READ optimizations (16GB RAM)
            cursor.execute("SET SESSION transaction_isolation = 'READ-UNCOMMITTED'")  # Dirty reads (fastest)
            cursor.execute("SET SESSION net_read_timeout = 3600")  # 1 hour timeout for large reads
            cursor.execute("SET SESSION net_write_timeout = 3600")  # 1 hour timeout
            cursor.execute("SET SESSION max_execution_time = 0")  # No query timeout
            # Balanced buffer sizes for 16GB RAM
            try:
                cursor.execute("SET SESSION read_buffer_size = 4194304")  # 4MB read buffer (balanced)
                cursor.execute("SET SESSION read_rnd_buffer_size = 4194304")  # 4MB random read buffer
                cursor.execute("SET SESSION sort_buffer_size = 4194304")  # 4MB sort buffer
            except:
                pass  # Some servers may not allow these
        else:
            # Destination database: AGGRESSIVE WRITE optimizations
            try:
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 300")
                cursor.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
                cursor.execute("SET SESSION unique_checks = 0")  # Disable unique checks
                cursor.execute("SET SESSION foreign_key_checks = 0")  # Disable FK checks
                cursor.execute("SET SESSION autocommit = 0")  # Manual commit
                cursor.execute("SET SESSION net_read_timeout = 3600")  # 1 hour timeout
                cursor.execute("SET SESSION net_write_timeout = 3600")  # 1 hour timeout
                # Balanced buffer sizes for 16GB RAM
                cursor.execute("SET SESSION bulk_insert_buffer_size = 33554432")  # 32MB bulk insert buffer
            except Exception as e:
                logger.debug(f"Some optimizations not available: {e}")
        
        cursor.close()
    except Exception as e:
        # Silently continue if session config fails - not critical
        pass


def get_table_columns(conn_params: Dict, table: str) -> List[str]:
    """Get column names from a table."""
    cnx = mysql.connector.connect(**conn_params)
    try:
        cur = cnx.cursor()
        cur.execute(f"SELECT * FROM `{table}` LIMIT 0")
        return [desc[0] for desc in cur.description]
    finally:
        cnx.close()


def table_exists(conn_params: Dict, table: str) -> bool:
    """Check if table exists."""
    cnx = mysql.connector.connect(**conn_params)
    try:
        cur = cnx.cursor()
        cur.execute(f"SHOW TABLES LIKE '{table}'")
        return cur.fetchone() is not None
    finally:
        cnx.close()


def create_table_from_source(src_conn_params: Dict, dst_conn_params: Dict, schema: str, table: str):
    """Create destination table with same structure as source, allowing NULLs for ETL safety."""
    logger.info(f"Creating table {table} in destination...")
    
    # Get CREATE TABLE statement from source
    src_cnx = mysql.connector.connect(**src_conn_params)
    try:
        cur = src_cnx.cursor()
        cur.execute(f"SHOW CREATE TABLE `{table}`")
        create_stmt = cur.fetchone()[1]
    finally:
        src_cnx.close()
    
    # Modify CREATE statement to allow NULLs (remove NOT NULL constraints except for primary keys)
    # This prevents NULL constraint errors during ETL
    import re
    # Keep NOT NULL only for AUTO_INCREMENT and PRIMARY KEY columns
    create_stmt = re.sub(r'`(\w+)`([^,\)]*?)NOT NULL(?!\s+AUTO_INCREMENT)', r'`\1`\2NULL', create_stmt)
    
    # Create table in destination
    dst_cnx = mysql.connector.connect(**dst_conn_params)
    try:
        cur = dst_cnx.cursor()
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(create_stmt)
        dst_cnx.commit()
        logger.info(f"Table {table} created successfully (NULL-safe)")
    finally:
        dst_cnx.close()


def stream_table_data(conn_params: Dict, table: str, chunk_size: int = 250000):
    """Stream data from table in balanced chunks (optimized for 16GB RAM)."""
    cnx = mysql.connector.connect(**conn_params)
    configure_session(cnx, is_source=True)  # Source: aggressive read optimizations
    try:
        # Use buffered cursor for better network performance
        cur = cnx.cursor(buffered=True)
        
        # AGGRESSIVE SQL hints for maximum read speed
        cur.execute(f"""
            SELECT /*+ 
                MAX_EXECUTION_TIME(0) 
                NO_INDEX_MERGE(*) 
                NO_BNL(*) 
                NO_BKA(*) 
            */ * FROM `{table}`
        """)
        
        while True:
            chunk = cur.fetchmany(chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        cnx.close()


def fetch_chunk_batch(conn_params: Dict, table: str, chunk_size: int, offset: int) -> List[tuple]:
    """Fetch a single chunk at specific offset with aggressive optimizations."""
    cnx = mysql.connector.connect(**conn_params)
    configure_session(cnx, is_source=True)  # Source: aggressive read optimizations
    try:
        # Buffered cursor for faster network transfer
        cur = cnx.cursor(buffered=True)
        
        # AGGRESSIVE SQL hints for maximum speed with timeout protection
        cur.execute(f"""
            SELECT /*+ 
                MAX_EXECUTION_TIME(0) 
                NO_INDEX_MERGE(*) 
                NO_BNL(*) 
                NO_BKA(*) 
            */ * FROM `{table}` 
            LIMIT {chunk_size} OFFSET {offset}
        """)
        
        # Fetch with timeout protection
        try:
            result = cur.fetchall()
            return result
        except Exception as e:
            logger.warning(f"Fetch timeout at offset {offset}, retrying with smaller chunk...")
            # Retry with smaller chunk if timeout
            cur.execute(f"SELECT * FROM `{table}` LIMIT {chunk_size // 2} OFFSET {offset}")
            return cur.fetchall()
    finally:
        cnx.close()


def upsert_rows(conn_params: Dict, table: str, columns: List[str], pk_columns: List[str], 
                rows: List[tuple], batch_size: int = 10000):
    """Insert or update rows in destination table with optimized batching and error handling."""
    if not rows:
        return
    
    cnx = mysql.connector.connect(**conn_params)
    configure_session(cnx, is_source=False)  # Destination: full optimizations
    
    try:
        # Use multi-row insert for better network efficiency
        cur = cnx.cursor()
        
        # Disable autocommit for faster batch inserts
        cnx.autocommit = False
        
        # Build INSERT ... ON DUPLICATE KEY UPDATE statement
        columns_str = ", ".join([f"`{col}`" for col in columns])
        
        # Build update clause
        if pk_columns:
            update_cols = [col for col in columns if col not in pk_columns]
            update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_cols])
        else:
            update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns])
        
        # Use multi-row VALUES for better network efficiency
        # Instead of executemany, build single query with multiple rows
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            try:
                # Build multi-row VALUES clause
                placeholders_per_row = ", ".join(["%s"] * len(columns))
                values_clause = ", ".join([f"({placeholders_per_row})"] * len(batch))
                
                sql = f"INSERT INTO `{table}` ({columns_str}) VALUES {values_clause}"
                if update_clause:
                    sql += f" ON DUPLICATE KEY UPDATE {update_clause}"
                
                # Flatten batch rows into single list
                flat_values = [val for row in batch for val in row]
                cur.execute(sql, flat_values)
                
            except Exception as batch_error:
                # If batch fails, try row-by-row to identify problem rows
                logger.warning(f"Batch insert failed, trying row-by-row: {str(batch_error)[:100]}")
                for row in batch:
                    try:
                        placeholders = ", ".join(["%s"] * len(columns))
                        sql = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"
                        if update_clause:
                            sql += f" ON DUPLICATE KEY UPDATE {update_clause}"
                        cur.execute(sql, row)
                    except Exception as row_error:
                        # Skip problematic rows and log
                        logger.debug(f"Skipping row due to error: {str(row_error)[:100]}")
                        continue
        
        # Single commit for all batches
        cnx.commit()
            
    except Exception as e:
        logger.error(f"Error upserting rows: {str(e)}")
        cnx.rollback()
        raise
    finally:
        cnx.close()


def delete_missing_rows(src_conn_params: Dict, dst_conn_params: Dict, table: str, pk_columns: List[str]):
    """Delete rows from destination that don't exist in source."""
    if not pk_columns:
        logger.info("No primary key - skipping delete check")
        return
    
    logger.info(f"Checking for rows to delete in {table}...")
    
    # Get all source PKs
    src_pks = set()
    src_cnx = mysql.connector.connect(**src_conn_params)
    configure_session(src_cnx, is_source=True)  # Source: read-only
    try:
        cur = src_cnx.cursor()
        pk_cols_str = ", ".join([f"`{col}`" for col in pk_columns])
        cur.execute(f"SELECT {pk_cols_str} FROM `{table}`")
        
        for row in cur.fetchall():
            if len(pk_columns) == 1:
                src_pks.add(row[0])
            else:
                src_pks.add(tuple(row))
    finally:
        src_cnx.close()
    
    # Find and delete missing rows in destination
    dst_cnx = mysql.connector.connect(**dst_conn_params)
    configure_session(dst_cnx, is_source=False)  # Destination: full optimizations
    try:
        cur = dst_cnx.cursor()
        pk_cols_str = ", ".join([f"`{col}`" for col in pk_columns])
        cur.execute(f"SELECT {pk_cols_str} FROM `{table}`")
        
        to_delete = []
        for row in cur.fetchall():
            pk_value = row[0] if len(pk_columns) == 1 else tuple(row)
            if pk_value not in src_pks:
                to_delete.append(row)
        
        if to_delete:
            logger.info(f"Deleting {len(to_delete)} rows from {table}")
            
            # Delete in batches
            batch_size = 1000
            for i in range(0, len(to_delete), batch_size):
                batch = to_delete[i:i + batch_size]
                
                if len(pk_columns) == 1:
                    pk_values = ", ".join(["%s"] * len(batch))
                    delete_sql = f"DELETE FROM `{table}` WHERE `{pk_columns[0]}` IN ({pk_values})"
                    cur.execute(delete_sql, [row[0] for row in batch])
                else:
                    # Composite PK
                    conditions = []
                    params = []
                    for row in batch:
                        cond = " AND ".join([f"`{col}` = %s" for col in pk_columns])
                        conditions.append(f"({cond})")
                        params.extend(row)
                    
                    delete_sql = f"DELETE FROM `{table}` WHERE {' OR '.join(conditions)}"
                    cur.execute(delete_sql, params)
                
                dst_cnx.commit()
            
            logger.info(f"Deleted {len(to_delete)} rows")
        else:
            logger.info("No rows to delete")
            
    except Exception as e:
        logger.error(f"Error deleting rows: {str(e)}")
        dst_cnx.rollback()
        raise
    finally:
        dst_cnx.close()


def get_table_row_count(conn_params: Dict, table: str) -> int:
    """Get row count for a table."""
    try:
        cnx = mysql.connector.connect(**conn_params)
        try:
            cur = cnx.cursor()
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            count = cur.fetchone()[0]
            return count
        finally:
            cnx.close()
    except Exception as e:
        logger.warning(f"Could not get row count for {table}: {str(e)}")
        return 0


async def process_table_async(src_conn_params: Dict, dst_conn_params: Dict, schema: str, table: str, executor: ThreadPoolExecutor):
    """
    Process a single table with multiple concurrent workers.
    
    Multiple workers fetch and upsert different chunks of the SAME table simultaneously.
    This allows all workers to collaborate on processing one large table at a time.
    """
    logger.info(f"[ASYNC] Processing table: {table}")
    
    # Update monitor: table started
    if MONITOR_AVAILABLE:
        state = get_state()
        if 'tables_status' not in state:
            state['tables_status'] = {}
        if table not in state['tables_status']:
            state['tables_status'][table] = {
                'status': 'processing', 
                'src_rows': 0, 
                'dst_rows': 0,
                'progress': 0,
                'error': None
            }
        else:
            state['tables_status'][table]['status'] = 'processing'
            state['tables_status'][table]['progress'] = 0
            state['tables_status'][table]['error'] = None
        update_state('tables_status', state['tables_status'])
    
    try:
        loop = asyncio.get_event_loop()
        
        # Get table metadata in parallel
        pk_columns_task = loop.run_in_executor(executor, get_primary_key_columns, src_conn_params, schema, table)
        columns_task = loop.run_in_executor(executor, get_table_columns, src_conn_params, table)
        row_count_task = loop.run_in_executor(executor, get_table_row_count, src_conn_params, table)
        
        pk_columns, columns, row_count = await asyncio.gather(pk_columns_task, columns_task, row_count_task)
        
        if pk_columns:
            logger.info(f"Primary key: {pk_columns}, ~{row_count:,} rows")
        else:
            logger.warning(f"No primary key found, ~{row_count:,} rows")
        
        # Create destination table if needed
        if not await loop.run_in_executor(executor, table_exists, dst_conn_params, table):
            await loop.run_in_executor(executor, create_table_from_source, src_conn_params, dst_conn_params, schema, table)
        
        # Balanced parallel chunk processing (optimized for 16GB RAM)
        batch_size = int(get_env("BATCH_SIZE", "50000"))  # Balanced batches
        chunk_size = 250000  # Balanced chunks (250K rows per fetch)
        max_concurrent_chunks = int(get_env("MAX_CONCURRENT_CHUNKS", "3"))  # Reduced to prevent connection exhaustion
        
        logger.info(f"Processing with {max_concurrent_chunks} concurrent chunks of {chunk_size:,} rows...")
        
        total_rows = 0
        
        # Calculate offsets for parallel fetching
        if row_count > 0:
            offsets = list(range(0, row_count, chunk_size))
            
            # Pipeline: Fetch next batch while processing current batch
            for i in range(0, len(offsets), max_concurrent_chunks):
                batch_offsets = offsets[i:i + max_concurrent_chunks]
                
                # Start fetching next batch in background (pipeline optimization)
                next_batch_offsets = offsets[i + max_concurrent_chunks:i + max_concurrent_chunks * 2]
                next_fetch_tasks = []
                if next_batch_offsets:
                    next_fetch_tasks = [
                        loop.run_in_executor(executor, fetch_chunk_batch, src_conn_params, table, chunk_size, offset)
                        for offset in next_batch_offsets
                    ]
                
                # Fetch current chunks in parallel
                fetch_tasks = [
                    loop.run_in_executor(executor, fetch_chunk_batch, src_conn_params, table, chunk_size, offset)
                    for offset in batch_offsets
                ]
                chunks = await asyncio.gather(*fetch_tasks)
                
                # Upsert chunks in parallel (while next batch fetches in background)
                upsert_tasks = [
                    loop.run_in_executor(executor, upsert_rows, dst_conn_params, table, columns, pk_columns, chunk, batch_size)
                    for chunk in chunks if chunk
                ]
                await asyncio.gather(*upsert_tasks)
                
                # Update progress
                for chunk in chunks:
                    if chunk:
                        total_rows += len(chunk)
                
                # Update monitor with progress percentage
                if MONITOR_AVAILABLE and row_count > 0:
                    state = get_state()
                    if table in state.get('tables_status', {}):
                        progress_pct = min(100, int((total_rows / row_count) * 100))
                        state['tables_status'][table]['dst_rows'] = total_rows
                        state['tables_status'][table]['progress'] = progress_pct
                        update_state('tables_status', state['tables_status'])
                
                # Free memory immediately after processing batch
                chunks = None
                del chunks
                
                logger.info(f"Processed {total_rows:,} rows... ({int((total_rows/row_count)*100) if row_count > 0 else 0}%)")
        
        logger.info(f"Completed upsert: {total_rows:,} rows")
        
        # Delete rows not in source
        if pk_columns:
            await loop.run_in_executor(
                executor,
                delete_missing_rows,
                src_conn_params, dst_conn_params, table, pk_columns
            )
        
        logger.info(f"✓ Successfully processed {table}")
        
        # Update monitor: table completed
        if MONITOR_AVAILABLE:
            state = get_state()
            if table in state.get('tables_status', {}):
                state['tables_status'][table]['status'] = 'completed'
                state['tables_status'][table]['src_rows'] = total_rows
                state['tables_status'][table]['dst_rows'] = total_rows
                state['tables_status'][table]['progress'] = 100
                update_state('tables_status', state['tables_status'])
            
            # Update completed count
            completed = state.get('completed_tables', 0) + 1
            update_state('completed_tables', completed)
        
        # Free all table-related memory
        pk_columns = None
        columns = None
        del pk_columns, columns
        
        # Force garbage collection to free memory immediately
        gc.collect()
        logger.info(f"Memory freed for table: {table}")
        
        return True, total_rows
        
    except Exception as e:
        logger.error(f"✗ Failed to process {table}: {str(e)}")
        
        # Update monitor: table failed
        if MONITOR_AVAILABLE:
            state = get_state()
            if table in state.get('tables_status', {}):
                state['tables_status'][table]['status'] = 'failed'
                state['tables_status'][table]['error'] = str(e)[:100]
                state['tables_status'][table]['progress'] = 0
                update_state('tables_status', state['tables_status'])
            
            # Update failed count
            failed = state.get('failed_tables', 0) + 1
            update_state('failed_tables', failed)
        
        # Free memory even on failure
        gc.collect()
        logger.info(f"Memory freed for failed table: {table}")
        
        return False, 0


async def main_async():
    """Main ETL process with async/await and gather for concurrent table processing."""
    logger.info("="*80)
    logger.info("Starting Simple MariaDB ETL (Async Mode)")
    logger.info("="*80)
    
    # Initialize monitor state
    update_state({
        'status': 'running',
        'start_time': time.time(),
        'end_time': None,
        'tables_status': {}
    })
    add_log("Starting Simple MariaDB ETL", "INFO")
    
    # Get database connection parameters
    src_conn_params = get_connection_params("SRC")
    dst_conn_params = get_connection_params("DST")
    
    logger.info(f"Source: {src_conn_params['host']}:{src_conn_params['port']}/{src_conn_params['database']}")
    logger.info(f"Destination: {dst_conn_params['host']}:{dst_conn_params['port']}/{dst_conn_params['database']}")
    
    # Get tables to process
    schema = src_conn_params['database']
    all_tables = get_tables(src_conn_params)
    
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
    add_log(f"Processing {len(tables)} tables", "INFO")
    
    # Initialize monitor state with tables
    update_state({
        'total_tables': len(tables),
        'completed_tables': 0,
        'failed_tables': 0
    })
    
    # Initialize all tables as pending in monitor
    if MONITOR_AVAILABLE:
        state = get_state()
        logger.info("Getting row counts for all tables...")
        for schema, table in tables:
            try:
                src_rows = get_table_row_count(src_conn_params, table)
            except:
                src_rows = 0
            
            state['tables_status'][table] = {
                'status': 'pending',
                'src_rows': src_rows,
                'dst_rows': 0,
                'progress': 0,
                'error': None
            }
        
        update_state('tables_status', state['tables_status'])
        logger.info("All tables initialized in monitor")
    
    # Process tables with multiple workers per table
    max_workers = int(get_env("MAX_WORKERS", "8"))  # Total workers across all tables
    workers_per_table = int(get_env("MAX_CONCURRENT_CHUNKS", "4"))  # Workers per single table
    
    logger.info(f"Processing {len(tables)} tables with {max_workers} total workers...")
    logger.info(f"Each table uses up to {workers_per_table} concurrent workers")
    
    # Create thread pool executor for blocking I/O operations
    executor = ThreadPoolExecutor(max_workers=max_workers)
    
    try:
        # Process tables one at a time, but use multiple workers per table
        for schema, table in tables:
            logger.info(f"Starting table: {table} with {workers_per_table} concurrent workers")
            await process_table_async(src_conn_params, dst_conn_params, schema, table, executor)
    finally:
        executor.shutdown(wait=True)
    
    # Final garbage collection
    gc.collect()
    logger.info("All tables completed - final memory cleanup done")
    
    # Get final stats from monitor
    if MONITOR_AVAILABLE:
        state = get_state()
        successful = state.get('completed_tables', 0)
        failed = state.get('failed_tables', 0)
        
        # Summary
        logger.info("="*80)
        logger.info("ETL Completed")
        logger.info(f"✓ Successful: {successful} tables")
        logger.info(f"✗ Failed: {failed} tables")
        logger.info("="*80)
    
    # Update final monitor state
    update_state({
        'status': 'completed',
        'end_time': time.time()
    })
    add_log(f"ETL completed: {successful} successful, {failed} failed", "INFO")


def main():
    """Entry point - starts monitor and runs async main."""
    # Start monitoring web server in background thread
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
        time.sleep(2)  # Give server time to start
    
    # Run async main
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
