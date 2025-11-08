"""
Hybrid ETL: Initial Load + CDC Streaming
1. Initial Load: Full table sync (one-time)
2. CDC Streaming: Real-time changes (continuous)
"""

import os
import sys
import time
import logging
import threading
import asyncio
from typing import Dict

# Import original ETL
from main import (
    get_env,
    get_connection_params,
    main_async as initial_load_async
)

# Import CDC
from cdc_stream import CDCStream

# Import monitor
try:
    from monitor import start_monitor_server, update_state, add_log
    MONITOR_AVAILABLE = True
except ImportError:
    MONITOR_AVAILABLE = False
    def update_state(*args, **kwargs): pass
    def add_log(*args, **kwargs): pass

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


async def hybrid_etl():
    """Hybrid ETL: Initial Load + CDC Streaming"""
    
    logger.info("="*80)
    logger.info("🚀 HYBRID ETL MODE")
    logger.info("="*80)
    
    # Get connection params
    src_conn_params = get_connection_params("SRC")
    dst_conn_params = get_connection_params("DST")
    
    # Check if we should do initial load
    do_initial_load = get_env("CDC_INITIAL_LOAD", "true").lower() == "true"
    
    if do_initial_load:
        logger.info("📊 PHASE 1: Initial Full Load")
        logger.info("="*80)
        
        # Run initial full load
        try:
            await initial_load_async()
            logger.info("✓ Initial load completed")
        except Exception as e:
            logger.error(f"✗ Initial load failed: {e}")
            return
        
        logger.info("")
        logger.info("="*80)
        logger.info("📊 PHASE 2: CDC Streaming (Real-time)")
        logger.info("="*80)
    else:
        logger.info("⏭️  Skipping initial load (CDC_INITIAL_LOAD=false)")
        logger.info("📊 Starting CDC Streaming...")
    
    # Start CDC streaming
    cdc = CDCStream(src_conn_params, dst_conn_params)
    
    # Update monitor
    if MONITOR_AVAILABLE:
        update_state({
            'mode': 'cdc_streaming',
            'status': 'streaming',
            'cdc_start_time': time.time()
        })
        add_log("CDC streaming started", "INFO")
    
    try:
        # Run CDC in blocking mode
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, cdc.start_streaming)
    except KeyboardInterrupt:
        logger.info("Stopping CDC streaming...")
        cdc.stop_streaming()
    except Exception as e:
        logger.error(f"CDC error: {e}")
        cdc.stop_streaming()


def main():
    """Entry point for Hybrid ETL"""
    
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
    
    # Run hybrid ETL
    asyncio.run(hybrid_etl())


if __name__ == "__main__":
    main()
