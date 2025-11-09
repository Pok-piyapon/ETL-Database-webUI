# 📊 MySQL ETL Pipeline with Real-time Dashboard

A high-performance, production-ready ETL (Extract, Transform, Load) pipeline for MySQL/MariaDB databases with real-time monitoring dashboard and dynamic database management.

![ETL Pipeline](https://img.shields.io/badge/ETL-Pipeline-blue)
![Python](https://img.shields.io/badge/Python-3.11-green)
![Docker](https://img.shields.io/badge/Docker-Ready-blue)
![License](https://img.shields.io/badge/License-MIT-yellow)

## ✨ Key Features

### 🚀 **High-Performance ETL**
- **Parallel Processing**: Configurable producer-consumer architecture with async I/O
- **Smart Batching**: Dynamic batch sizing (1K-50K rows) based on table size
- **Connection Pooling**: Optimized aiomysql pools for maximum throughput
- **Retry Logic**: Automatic retry with exponential backoff for failed operations
- **Memory Efficient**: Streaming data processing to handle large datasets

### 📊 **Real-time Dashboard**
- **Live Monitoring**: WebSocket-based real-time updates (Socket.IO)
- **Worker Progress**: Track individual producer/consumer workers per table (auto-updates every 5s)
- **System Stats**: CPU, memory, and disk usage monitoring
- **Table Progress**: Visual progress bars with row counts and completion status
- **Live Logs**: Real-time log streaming with severity filtering

### 🗓️ **Dynamic Database Management**
- **Date-based Databases**: Auto-create databases with date/time patterns
- **Placeholders**: `{YYYY}`, `{MM}`, `{DD}`, `{HH}`, `{mm}`, `{ss}`
- **Example**: `medkku_{YYYY}_{MM}_{DD}` → `medkku_2025_11_08`
- **Versioning**: Keep historical snapshots without overwriting data

### ⚙️ **Flexible Configuration**
- **Web UI Settings**: Configure all parameters via dashboard
- **Environment Variables**: Full `.env` file management through UI
- **Table Filtering**: Include/exclude specific tables with patterns
- **Scheduled Runs**: Auto-run at configurable intervals (days/hours/minutes)
- **Performance Tuning**: Adjust workers, batch sizes, and timeouts on-the-fly

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  Source MySQL   │────▶│   ETL Pipeline       │────▶│  Dest MySQL     │
│  (Read-only)    │     │  ┌────────────────┐  │     │  (Dynamic DB)   │
│                 │     │  │ 10 Producers   │  │     │                 │
│  Tables: 54     │     │  │ (Extract)      │  │     │  Auto-created   │
│  Rows: Millions │     │  └────────┬───────┘  │     │  per run        │
└─────────────────┘     │           │          │     └─────────────────┘
                        │           ▼          │
                        │  ┌────────────────┐  │
                        │  │ Queue (async)  │  │
                        │  └────────┬───────┘  │
                        │           │          │
                        │           ▼          │
                        │  ┌────────────────┐  │
                        │  │ 10 Consumers   │  │
                        │  │ (Load)         │  │
                        │  └────────────────┘  │
                        └──────────┬───────────┘
                                   │
                                   ▼
                        ┌──────────────────────┐
                        │   Dashboard          │
                        │   (Socket.IO)        │
                        │   - Workers Progress │
                        │   - System Stats     │
                        │   - Live Logs        │
                        │   - Settings         │
                        └──────────────────────┘
```

### Components
- **Producers**: Extract data in parallel batches from source database
- **Consumers**: Transform and load data to destination database
- **Monitor**: Flask + Socket.IO server for real-time dashboard
- **Cache**: Optional Redis-based caching for table schemas and metadata

## 🐳 Quick Start

### Prerequisites
- Docker & Docker Compose
- Source MySQL/MariaDB database (accessible from Docker)

### Installation

1. **Clone the repository**
```bash
git clone <your-repo-url>
cd med
```

2. **Configure environment**
```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your database credentials
nano .env
```

3. **Start services**
```bash
docker-compose up -d
```

4. **Access dashboard**
```
http://localhost:5000
```

5. **Monitor logs**
```bash
docker-compose logs -f etl
```

## 📝 Configuration

### Environment Variables

| Variable | Description | Example | Default |
|----------|-------------|---------|---------|
| **Source Database** |
| `SRC_DB_HOST` | Source database host | `your-host` | `localhost` |
| `SRC_DB_PORT` | Source database port | `3306` | `3306` |
| `SRC_DB_NAME` | Source database name or pattern | `source_db` or `LATEST:backup_*` | `test` |
| `SRC_DB_USER` | Source database user | `username` | `root` |
| `SRC_DB_PASSWORD` | Source database password | `password` | - |
| **Destination Database** |
| `DST_DB_HOST` | Destination host | `host.docker.internal` | `localhost` |
| `DST_DB_PORT` | Destination port | `3306` | `3306` |
| `DST_DB_NAME` | Database name (supports placeholders) | `backup_{YYYY}_{MM}_{DD}` | `test` |
| `DST_DB_DYNAMIC` | Enable dynamic database creation | `true` / `false` | `false` |
| `DST_DB_USER` | Destination user | `username` | `root` |
| `DST_DB_PASSWORD` | Destination password | `password` | - |
| **Table Filtering** |
| `INCLUDE_TABLES` | Tables to include (comma-separated) | `tbl_user,tbl_order` | (all) |
| `EXCLUDE_TABLES` | Tables to exclude (comma-separated) | `temp_*,test_*` | (none) |
| **ETL Configuration** |
| `LOG_LEVEL` | Logging level | `DEBUG`/`INFO`/`WARNING`/`ERROR` | `INFO` |
| `MAX_WORKERS` | Total workers (producers + consumers) | `20` | `20` |
| `BATCH_SIZE` | Rows per batch | `5000` | `5000` |
| **Performance Tuning** |
| `MAX_TABLE_TIME_SECONDS` | Max time per table (0=unlimited) | `0` | `0` |
| `MIN_BATCH_SIZE` | Minimum batch size | `5000` | `5000` |
| `MAX_BATCH_SIZE` | Maximum batch size | `10000` | `10000` |
| **Scheduling** |
| `ETL_INTERVAL_SECONDS` | Auto-run interval (0=run once) | `300` | `300` |

### Auto-Select Latest Database

Use `LATEST:` prefix in `SRC_DB_NAME` to automatically select the latest database matching a pattern:

**Examples:**
```env
# Select latest database starting with "smartmedkku_"
SRC_DB_NAME=LATEST:smartmedkku_*
# Finds: smartmedkku_2025.11.09 (latest from all smartmedkku_* databases)

# Select latest backup
SRC_DB_NAME=LATEST:backup_*
# Finds: backup_2025_11_09 (latest from all backup_* databases)

# Select latest medkku database
SRC_DB_NAME=LATEST:medkku.*
# Finds: medkku.2025.11.09 (latest from all medkku.* databases)

# Or specify exact name
SRC_DB_NAME=smartmedkku_2025.10.11
# Uses: smartmedkku_2025.10.11 (exact match)
```

**How it works:**
1. Lists all databases on the source server
2. Filters databases matching the wildcard pattern (`*`)
3. Sorts databases alphabetically
4. Selects the last one (assumes date/time-based naming)

### Dynamic Database Naming

Use placeholders in `DST_DB_NAME` to create time-based databases:

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{YYYY}` | 4-digit year | `2025` |
| `{MM}` | 2-digit month | `11` |
| `{DD}` | 2-digit day | `08` |
| `{HH}` | 2-digit hour (24h) | `14` |
| `{mm}` | 2-digit minute | `30` |
| `{ss}` | 2-digit second | `45` |

**Examples:**
```env
# Daily backups
DST_DB_NAME=backup_{YYYY}_{MM}_{DD}
# Result: backup_2025_11_08

# Hourly snapshots
DST_DB_NAME=snapshot_{YYYY}{MM}{DD}_{HH}00
# Result: snapshot_20251108_1400

# Thai format
DST_DB_NAME=medkku.{YYYY}.{MM}.{DD}
# Result: medkku.2025.11.08
```

### Dashboard Settings

All settings can be configured via the web dashboard at `http://localhost:5000`:

1. **⚙️ Settings Modal**
   - Edit all `.env` variables
   - Source/Destination database credentials
   - Table filtering patterns
   - Performance tuning parameters
   - Scheduling intervals (Days/Hours/Minutes)

2. **📊 Workers Progress** (Auto-updates every 5s)
   - Active producers per table
   - Active consumers per table
   - Rows processed
   - Current status

3. **📈 System Stats**
   - CPU usage
   - Memory usage
   - Disk usage

4. **📋 Live Logs**
   - Real-time log streaming
   - Severity filtering
   - Auto-scroll

## 🎯 Use Cases

### 1. Daily Database Backups
```env
DST_DB_NAME=backup_{YYYY}_{MM}_{DD}
DST_DB_DYNAMIC=true
ETL_INTERVAL_SECONDS=86400  # 24 hours
```

### 2. Hourly Data Snapshots
```env
DST_DB_NAME=snapshot_{YYYY}{MM}{DD}_{HH}00
DST_DB_DYNAMIC=true
ETL_INTERVAL_SECONDS=3600  # 1 hour
```

### 3. One-time Migration
```env
DST_DB_NAME=production_db
DST_DB_DYNAMIC=false
ETL_INTERVAL_SECONDS=0  # Run once
```

### 4. Selective Table Migration
```env
INCLUDE_TABLES=tbl_user,tbl_order,tbl_product
EXCLUDE_TABLES=
```

### 5. Exclude Temporary Tables
```env
INCLUDE_TABLES=
EXCLUDE_TABLES=temp_,test_,backup_
```

## 📊 Performance

### Throughput
- **Small tables** (<10K rows): ~100K rows/second
- **Medium tables** (10K-100K): ~200K rows/second
- **Large tables** (>1M rows): ~300K-500K rows/second

*Performance depends on hardware, network, and database configuration*

### Optimization Tips

| Table Size | Workers | Batch Size | Strategy |
|------------|---------|------------|----------|
| < 10K rows | 2-3 | 1,000 | Minimal overhead |
| 10K-100K | 5 | 5,000 | Balanced |
| 100K-1M | 8 | 5,000 | Moderate parallel |
| > 1M rows | 10 | 5,000 | High parallel |

**Conservative Mode (Recommended for Stability):**
```env
MAX_WORKERS=20
BATCH_SIZE=5000
MAX_TABLE_TIME_SECONDS=0  # No time limit
```

**Aggressive Mode (Maximum Speed):**
```env
MAX_WORKERS=50
BATCH_SIZE=10000
MAX_TABLE_TIME_SECONDS=3600  # 1 hour limit
```

## 🛠️ Tech Stack

- **Backend**: Python 3.11, asyncio, aiomysql
- **Web Server**: Flask, Flask-SocketIO
- **Frontend**: Vanilla JavaScript, Socket.IO client
- **Database**: MySQL 8.0 / MariaDB 11.8
- **Cache**: Redis (optional)
- **Containerization**: Docker, Docker Compose

## 📦 Project Structure

```
med/
├── etl/
│   ├── app/
│   │   ├── main.py                  # ETL pipeline core
│   │   ├── monitor.py               # Dashboard server (Flask + Socket.IO)
│   │   ├── cache_storage.py         # Redis caching layer
│   │   ├── templates/
│   │   │   └── dashboard.html       # Dashboard UI
│   │   └── static/
│   │       └── dashboard_socket.js  # Real-time WebSocket client
│   └── Dockerfile
├── docker-compose.yml               # Docker services configuration
├── .env                             # Environment variables
├── requirements.txt                 # Python dependencies
└── README.md
```

## 🔍 Monitoring & Debugging

### Dashboard Features

1. **Status Overview**
   - Current status: Running/Idle/Completed/Failed
   - Elapsed time
   - Tables: Total/Completed/Failed
   - Current table being processed

2. **Table Progress**
   - Visual progress bars
   - Row counts (processed/total)
   - Completion percentage
   - ETA (estimated time)

3. **Workers Modal**
   - Real-time worker status
   - Producers: Active/Total
   - Consumers: Active/Total
   - Rows processed per worker
   - Auto-updates every 5 seconds

4. **System Resources**
   - CPU usage (%)
   - Memory usage (MB)
   - Disk usage (GB)

5. **Live Logs**
   - Real-time log streaming
   - Severity levels: DEBUG/INFO/WARNING/ERROR
   - Auto-scroll to latest
   - Filterable

### Socket.IO Events

All dashboard data is streamed via WebSocket:

| Client → Server | Server → Client | Description |
|-----------------|-----------------|-------------|
| `connect` | `initial_state` | Initial connection |
| `request_status` | `status_update` | ETL status update |
| `request_tables` | `tables_update` | Table progress |
| `request_logs` | `logs_update` | Log entries |
| `request_config` | `config_update` | Configuration |
| `save_config` | `config_saved` | Save result |

### Docker Commands

```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f etl

# Restart ETL
docker-compose restart etl

# Stop services
docker-compose down

# Rebuild after code changes
docker-compose up -d --build
```

## 🔒 Security

- **Credentials**: Stored in `.env` file (excluded from git)
- **Docker Isolation**: Services run in isolated containers
- **Read-only Source**: Source database accessed in read-only mode
- **Connection Limits**: Configurable pool sizes prevent resource exhaustion
- **Password Fields**: Hidden in dashboard UI

## 🐛 Troubleshooting

### Common Issues

**1. Database Connection Failed**
```bash
# Check if source database is accessible
docker exec -it mariadb-etl ping <SRC_DB_HOST>

# Verify credentials
docker exec -it mariadb-etl mysql -h <SRC_DB_HOST> -u <SRC_DB_USER> -p
```

**2. Dynamic Database Not Created**
```bash
# Check if DST_DB_DYNAMIC is enabled
grep DST_DB_DYNAMIC .env

# Verify destination database permissions
# User must have CREATE DATABASE privilege
```

**3. Workers Not Showing in Dashboard**
```bash
# Check if ETL is running
docker-compose logs etl | grep "Starting Parallel ETL"

# Verify Socket.IO connection
# Open browser console (F12) and check for WebSocket errors
```

**4. Slow Performance**
```bash
# Reduce batch size
BATCH_SIZE=1000

# Reduce workers
MAX_WORKERS=10

# Check network latency
docker exec -it mariadb-etl ping <SRC_DB_HOST>
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

MIT License - feel free to use this project for personal or commercial purposes.

## 🙏 Acknowledgments

- Built with Python asyncio for high-performance async I/O
- Inspired by AWS Glue ETL architecture
- Real-time dashboard powered by Socket.IO
- Connection pooling with aiomysql

## 📞 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Check the troubleshooting section
- Review Docker logs: `docker-compose logs -f etl`

---

**⭐ Star this repo if you find it useful!**

Made with ❤️ for efficient database migrations and backups
