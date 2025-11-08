"""
CDC (Change Data Capture) using MySQL Binlog Streaming
Real-time data synchronization by reading binary logs
"""

import logging
import asyncio
import time
from typing import Dict, List
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
import mysql.connector

logger = logging.getLogger(__name__)


class CDCStream:
    """Real-time CDC using MySQL Binlog"""
    
    def __init__(self, src_conn_params: Dict, dst_conn_params: Dict):
        self.src_params = src_conn_params
        self.dst_params = dst_conn_params
        self.stream = None
        self.running = False
        self.stats = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'errors': 0,
            'start_time': None
        }
    
    def connect_stream(self):
        """Connect to MySQL binlog stream"""
        logger.info("Connecting to binlog stream...")
        
        # Convert connection params for pymysqlreplication
        stream_settings = {
            'host': self.src_params['host'],
            'port': self.src_params['port'],
            'user': self.src_params['user'],
            'passwd': self.src_params['password']
        }
        
        try:
            self.stream = BinLogStreamReader(
                connection_settings=stream_settings,
                server_id=100,  # Unique ID for this replication client
                blocking=True,
                resume_stream=True,  # Resume from last position
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                only_schemas=[self.src_params['database']]  # Only watch our database
            )
            logger.info("✓ Connected to binlog stream")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to connect to binlog: {e}")
            return False
    
    def get_table_columns(self, table: str) -> List[str]:
        """Get column names for a table"""
        cnx = mysql.connector.connect(**self.dst_params)
        try:
            cur = cnx.cursor()
            cur.execute(f"SELECT * FROM `{table}` LIMIT 0")
            return [desc[0] for desc in cur.description]
        finally:
            cnx.close()
    
    def handle_insert(self, event: WriteRowsEvent):
        """Handle INSERT event from binlog"""
        table = event.table
        
        try:
            cnx = mysql.connector.connect(**self.dst_params)
            cur = cnx.cursor()
            
            for row in event.rows:
                values = row['values']
                
                # Build INSERT statement
                columns = list(values.keys())
                columns_str = ", ".join([f"`{col}`" for col in columns])
                placeholders = ", ".join(["%s"] * len(columns))
                
                sql = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"
                sql += " ON DUPLICATE KEY UPDATE "
                sql += ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns])
                
                cur.execute(sql, list(values.values()))
            
            cnx.commit()
            cnx.close()
            
            self.stats['inserts'] += len(event.rows)
            logger.info(f"[CDC] INSERT {len(event.rows)} rows into {table}")
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"[CDC] Error handling INSERT: {e}")
    
    def handle_update(self, event: UpdateRowsEvent):
        """Handle UPDATE event from binlog"""
        table = event.table
        
        try:
            cnx = mysql.connector.connect(**self.dst_params)
            cur = cnx.cursor()
            
            for row in event.rows:
                after_values = row['after_values']
                
                # Build UPDATE statement using all columns
                columns = list(after_values.keys())
                columns_str = ", ".join([f"`{col}`" for col in columns])
                placeholders = ", ".join(["%s"] * len(columns))
                
                # Use INSERT ... ON DUPLICATE KEY UPDATE for safety
                sql = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"
                sql += " ON DUPLICATE KEY UPDATE "
                sql += ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns])
                
                cur.execute(sql, list(after_values.values()))
            
            cnx.commit()
            cnx.close()
            
            self.stats['updates'] += len(event.rows)
            logger.info(f"[CDC] UPDATE {len(event.rows)} rows in {table}")
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"[CDC] Error handling UPDATE: {e}")
    
    def handle_delete(self, event: DeleteRowsEvent):
        """Handle DELETE event from binlog"""
        table = event.table
        
        try:
            cnx = mysql.connector.connect(**self.dst_params)
            cur = cnx.cursor()
            
            for row in event.rows:
                values = row['values']
                
                # Build DELETE statement
                # Use all columns to identify the row
                conditions = " AND ".join([f"`{col}` = %s" for col in values.keys()])
                sql = f"DELETE FROM `{table}` WHERE {conditions}"
                
                cur.execute(sql, list(values.values()))
            
            cnx.commit()
            cnx.close()
            
            self.stats['deletes'] += len(event.rows)
            logger.info(f"[CDC] DELETE {len(event.rows)} rows from {table}")
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"[CDC] Error handling DELETE: {e}")
    
    def start_streaming(self):
        """Start listening to binlog events (blocking)"""
        if not self.connect_stream():
            return
        
        self.running = True
        self.stats['start_time'] = time.time()
        
        logger.info("="*80)
        logger.info("🔴 CDC STREAMING MODE - Listening for real-time changes...")
        logger.info("="*80)
        
        try:
            for binlogevent in self.stream:
                if not self.running:
                    break
                
                # Handle different event types
                if isinstance(binlogevent, WriteRowsEvent):
                    self.handle_insert(binlogevent)
                
                elif isinstance(binlogevent, UpdateRowsEvent):
                    self.handle_update(binlogevent)
                
                elif isinstance(binlogevent, DeleteRowsEvent):
                    self.handle_delete(binlogevent)
                
                # Log stats every 100 events
                total_events = self.stats['inserts'] + self.stats['updates'] + self.stats['deletes']
                if total_events % 100 == 0 and total_events > 0:
                    elapsed = time.time() - self.stats['start_time']
                    logger.info(f"[CDC] Stats: {self.stats['inserts']} inserts, "
                              f"{self.stats['updates']} updates, "
                              f"{self.stats['deletes']} deletes, "
                              f"{self.stats['errors']} errors "
                              f"({elapsed:.1f}s)")
        
        except KeyboardInterrupt:
            logger.info("CDC streaming stopped by user")
        except Exception as e:
            logger.error(f"CDC streaming error: {e}")
        finally:
            self.stop_streaming()
    
    def stop_streaming(self):
        """Stop the binlog stream"""
        self.running = False
        if self.stream:
            self.stream.close()
            logger.info("CDC stream closed")
        
        # Final stats
        elapsed = time.time() - self.stats['start_time'] if self.stats['start_time'] else 0
        logger.info("="*80)
        logger.info("CDC STREAMING SUMMARY")
        logger.info(f"Duration: {elapsed:.1f}s")
        logger.info(f"Inserts: {self.stats['inserts']}")
        logger.info(f"Updates: {self.stats['updates']}")
        logger.info(f"Deletes: {self.stats['deletes']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("="*80)


async def start_cdc_async(src_conn_params: Dict, dst_conn_params: Dict):
    """Start CDC in async mode"""
    cdc = CDCStream(src_conn_params, dst_conn_params)
    
    # Run in thread pool to avoid blocking
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, cdc.start_streaming)
