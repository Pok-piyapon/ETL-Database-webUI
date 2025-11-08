"""
Real-time ETL monitoring web dashboard with WebSocket support
"""
import os
import json
import time
import psutil
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import threading

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global state for monitoring
etl_state = {
    'status': 'idle',  # idle, running, completed, failed
    'start_time': None,
    'end_time': None,
    'current_table': None,
    'total_tables': 0,
    'completed_tables': 0,
    'failed_tables': 0,
    'tables_status': {},  # {table_name: {status, src_rows, dst_rows, error}}
    'current_progress': {
        'table': None,
        'rows_processed': 0,
        'total_rows': 0,
        'percentage': 0
    },
    'system_stats': {
        'cpu_percent': 0,
        'memory_percent': 0,
        'memory_used_mb': 0,
        'memory_total_mb': 0
    },
    'logs': [],  # Recent log messages (max 100)
    'next_run_time': None,  # Unix timestamp for next scheduled run
    'etl_interval': 0,  # Interval in seconds
    'max_workers': 0  # Number of workers
}

state_lock = threading.Lock()


def update_system_stats():
    """Update system resource usage stats"""
    while True:
        try:
            cpu = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            with state_lock:
                etl_state['system_stats'] = {
                    'cpu_percent': cpu,
                    'memory_percent': memory.percent,
                    'memory_used_mb': memory.used / (1024 * 1024),
                    'memory_total_mb': memory.total / (1024 * 1024)
                }
        except Exception as e:
            print(f"Error updating system stats: {e}")
        
        time.sleep(2)


def update_state(key, value=None):
    """Thread-safe state update with WebSocket broadcast"""
    with state_lock:
        if isinstance(key, dict):
            # Bulk update
            etl_state.update(key)
        else:
            etl_state[key] = value
    
    # Broadcast state change to all connected clients
    try:
        socketio.emit('state_update', {
            'key': key if not isinstance(key, dict) else 'bulk',
            'timestamp': time.time()
        }, namespace='/')
    except:
        pass  # Ignore if no clients connected


def get_state():
    """Thread-safe state read"""
    with state_lock:
        return etl_state.copy()


def add_log(message, level='INFO'):
    """Add a log message to the state"""
    with state_lock:
        timestamp = datetime.now().strftime('%H:%M:%S')
        etl_state['logs'].append({
            'time': timestamp,
            'level': level,
            'message': message
        })
        # Keep only last 100 logs
        if len(etl_state['logs']) > 100:
            etl_state['logs'] = etl_state['logs'][-100:]


@app.route('/')
def index():
    """Serve the dashboard HTML"""
    return render_template('dashboard.html')


# ========================================
# Helper Functions
# ========================================

def format_time(seconds):
    """Format seconds to HH:MM:SS"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


# ========================================
# Socket.IO Event Handlers (Replace REST APIs)
# ========================================

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('✅ Client connected')
    # Send initial state immediately
    emit('initial_state', get_state())


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('❌ Client disconnected')


@socketio.on('request_status')
def handle_request_status():
    """Handle status request via socket"""
    state = get_state()
    
    # Calculate elapsed time
    elapsed = 0
    if state.get('status') == 'running' and state.get('start_time'):
        elapsed = time.time() - state['start_time']
    
    emit('status_update', {
        'status': state.get('status', 'idle'),
        'elapsed_time': format_time(elapsed),
        'total_tables': state.get('total_tables', 0),
        'completed_tables': state.get('completed_tables', 0),
        'failed_tables': state.get('failed_tables', 0),
        'current_table': state.get('current_table', None),
        'system_stats': state.get('system_stats', {}),
        'workers': state.get('workers', {}),
        'max_workers': state.get('max_workers', 20),
        'etl_interval': state.get('etl_interval', 0),
        'next_run_time': state.get('next_run_time', None)
    })


@socketio.on('request_tables')
def handle_request_tables():
    """Handle tables request via socket"""
    state = get_state()
    tables_status = state.get('tables_status', {})
    
    tables = []
    for table_name, status in tables_status.items():
        tables.append({
            'name': table_name,
            'status': status.get('status', 'pending'),
            'src_rows': status.get('src_rows', 0),
            'dst_rows': status.get('dst_rows', 0),
            'error': status.get('error', None),
            'progress': status.get('progress', 0),
            'chunk_size': status.get('chunk_size', 0),
            'batch_size': status.get('batch_size', 0),
            'num_producers': status.get('num_producers', 0),
            'num_consumers': status.get('num_consumers', 0)
        })
    
    emit('tables_update', tables)


@socketio.on('request_logs')
def handle_request_logs():
    """Handle logs request via socket"""
    state = get_state()
    emit('logs_update', state.get('logs', []))


@socketio.on('request_config')
def handle_request_config():
    """Handle config request via socket"""
    env_file = '/app/.env'
    config = {}
    
    try:
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # Remove comments from value
                        value = value.split('#')[0].strip()
                        
                        # Map all .env variables
                        key_lower = key.lower()
                        if key == 'SRC_DB_HOST':
                            config['src_db_host'] = value
                        elif key == 'SRC_DB_PORT':
                            config['src_db_port'] = int(value)
                        elif key == 'SRC_DB_NAME':
                            config['src_db_name'] = value
                        elif key == 'SRC_DB_USER':
                            config['src_db_user'] = value
                        elif key == 'SRC_DB_PASSWORD':
                            config['src_db_password'] = value
                        elif key == 'DST_DB_HOST':
                            config['dst_db_host'] = value
                        elif key == 'DST_DB_PORT':
                            config['dst_db_port'] = int(value)
                        elif key == 'DST_DB_NAME':
                            config['dst_db_name'] = value
                        elif key == 'DST_DB_DYNAMIC':
                            config['dst_db_dynamic'] = value
                        elif key == 'DST_DB_USER':
                            config['dst_db_user'] = value
                        elif key == 'DST_DB_PASSWORD':
                            config['dst_db_password'] = value
                        elif key == 'INCLUDE_TABLES':
                            config['include_tables'] = value
                        elif key == 'EXCLUDE_TABLES':
                            config['exclude_tables'] = value
                        elif key == 'LOG_LEVEL':
                            config['log_level'] = value
                        elif key == 'MAX_WORKERS':
                            config['max_workers'] = int(value)
                        elif key == 'BATCH_SIZE':
                            config['batch_size'] = int(value)
                        elif key == 'MAX_TABLE_TIME_SECONDS':
                            config['max_table_time_seconds'] = int(value)
                        elif key == 'MIN_BATCH_SIZE':
                            config['min_batch_size'] = int(value)
                        elif key == 'MAX_BATCH_SIZE':
                            config['max_batch_size'] = int(value)
                        elif key == 'ETL_INTERVAL_SECONDS':
                            config['etl_interval_seconds'] = int(value)
    except Exception as e:
        print(f"Error reading config: {e}")
    
    emit('config_update', config)


@socketio.on('save_config')
def handle_save_config(data):
    """Handle config save via socket - Update ALL .env variables"""
    env_file = '/app/.env'
    
    try:
        # Read current .env file
        lines = []
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                lines = f.readlines()
        
        # Update all values
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('#') and '=' in stripped:
                key = stripped.split('=')[0]
                
                # Update each key with new value
                if key == 'SRC_DB_HOST' and 'src_db_host' in data:
                    lines[i] = f'SRC_DB_HOST={data["src_db_host"]}\n'
                elif key == 'SRC_DB_PORT' and 'src_db_port' in data:
                    lines[i] = f'SRC_DB_PORT={data["src_db_port"]}\n'
                elif key == 'SRC_DB_NAME' and 'src_db_name' in data:
                    lines[i] = f'SRC_DB_NAME={data["src_db_name"]}\n'
                elif key == 'SRC_DB_USER' and 'src_db_user' in data:
                    lines[i] = f'SRC_DB_USER={data["src_db_user"]}\n'
                elif key == 'SRC_DB_PASSWORD' and 'src_db_password' in data:
                    lines[i] = f'SRC_DB_PASSWORD={data["src_db_password"]}\n'
                elif key == 'DST_DB_HOST' and 'dst_db_host' in data:
                    lines[i] = f'DST_DB_HOST={data["dst_db_host"]}\n'
                elif key == 'DST_DB_PORT' and 'dst_db_port' in data:
                    lines[i] = f'DST_DB_PORT={data["dst_db_port"]}\n'
                elif key == 'DST_DB_NAME' and 'dst_db_name' in data:
                    lines[i] = f'DST_DB_NAME={data["dst_db_name"]}\n'
                elif key == 'DST_DB_DYNAMIC' and 'dst_db_dynamic' in data:
                    lines[i] = f'DST_DB_DYNAMIC={data["dst_db_dynamic"]}           # true = create new DB each run with date/time\n'
                elif key == 'DST_DB_USER' and 'dst_db_user' in data:
                    lines[i] = f'DST_DB_USER={data["dst_db_user"]}\n'
                elif key == 'DST_DB_PASSWORD' and 'dst_db_password' in data:
                    lines[i] = f'DST_DB_PASSWORD={data["dst_db_password"]}\n'
                elif key == 'INCLUDE_TABLES' and 'include_tables' in data:
                    lines[i] = f'INCLUDE_TABLES={data["include_tables"]}\n'
                elif key == 'EXCLUDE_TABLES' and 'exclude_tables' in data:
                    lines[i] = f'EXCLUDE_TABLES={data["exclude_tables"]}\n'
                elif key == 'LOG_LEVEL' and 'log_level' in data:
                    lines[i] = f'LOG_LEVEL={data["log_level"]}\n'
                elif key == 'MAX_WORKERS' and 'max_workers' in data:
                    lines[i] = f'MAX_WORKERS={data["max_workers"]}              # Conservative for stability\n'
                elif key == 'BATCH_SIZE' and 'batch_size' in data:
                    lines[i] = f'BATCH_SIZE={data["batch_size"]}             # Smaller batch for reliability\n'
                elif key == 'MAX_TABLE_TIME_SECONDS' and 'max_table_time_seconds' in data:
                    lines[i] = f'MAX_TABLE_TIME_SECONDS={data["max_table_time_seconds"]}    # 0 = no time limit, just complete all tables\n'
                elif key == 'MIN_BATCH_SIZE' and 'min_batch_size' in data:
                    lines[i] = f'MIN_BATCH_SIZE={data["min_batch_size"]}\n'
                elif key == 'MAX_BATCH_SIZE' and 'max_batch_size' in data:
                    lines[i] = f'MAX_BATCH_SIZE={data["max_batch_size"]}        # Smaller max for stability\n'
                elif key == 'ETL_INTERVAL_SECONDS' and 'etl_interval_seconds' in data:
                    lines[i] = f'ETL_INTERVAL_SECONDS={data["etl_interval_seconds"]}\n'
        
        # Write back to file
        with open(env_file, 'w') as f:
            f.writelines(lines)
        
        print(f"✅ Configuration updated successfully")
        emit('config_saved', {'success': True, 'message': 'All settings saved successfully!'})
    
    except Exception as e:
        print(f"❌ Error saving config: {e}")
        emit('config_saved', {'success': False, 'error': str(e)})


# All API endpoints removed - using Socket.IO only


def start_monitor_server(host='0.0.0.0', port=5000):
    """Start the monitoring web server"""
    import logging
    
    # Disable Flask's default logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    log.disabled = True
    
    # Disable Flask app logger
    app.logger.disabled = True
    
    # Start system stats updater in background
    stats_thread = threading.Thread(target=update_system_stats, daemon=True)
    stats_thread.start()
    
    print(f"Starting ETL Monitor Dashboard with WebSocket on http://{host}:{port}")
    socketio.run(app, host=host, port=port, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)


if __name__ == '__main__':
    start_monitor_server()
