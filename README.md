# 📊 MySQL ETL Pipeline พร้อม Dashboard แบบ Real-time

ระบบ ETL (Extract, Transform, Load) ประสิทธิภาพสูง พร้อมใช้งานจริงสำหรับฐานข้อมูล MySQL/MariaDB พร้อมด้วย Dashboard แบบ Real-time และการจัดการฐานข้อมูลแบบไดนามิก

![ETL Pipeline](https://img.shields.io/badge/ETL-Pipeline-blue)
![Python](https://img.shields.io/badge/Python-3.11-green)
![Docker](https://img.shields.io/badge/Docker-Ready-blue)
![License](https://img.shields.io/badge/License-MIT-yellow)

## ✨ ฟีเจอร์หลัก

### 🚀 **ETL ประสิทธิภาพสูง**
- **การประมวลผลแบบขนาน**: สถาปัตยกรรม producer-consumer ที่ปรับแต่งได้ด้วย async I/O
- **Smart Batching**: ปรับขนาด batch (1K-50K แถว) อัตโนมัติตามขนาดตาราง
- **Connection Pooling**: จัดการ connection pool ด้วย aiomysql เพื่อประสิทธิภาพสูงสุด
- **Retry Logic**: ลองใหม่อัตโนมัติพร้อม exponential backoff เมื่อเกิดข้อผิดพลาด
- **ประหยัดหน่วยความจำ**: ประมวลผลข้อมูลแบบ streaming รองรับข้อมูลขนาดใหญ่

### 📊 **Dashboard แบบ Real-time**
- **ติดตามสดๆ**: อัปเดตแบบ Real-time ผ่าน WebSocket (Socket.IO)
- **Worker Progress**: ติดตามสถานะ producer/consumer แต่ละตัวของแต่ละตาราง (อัปเดตทุก 5 วินาที)
- **System Stats**: ติดตามการใช้งาน CPU, หน่วยความจำ และดิสก์
- **Table Progress**: แสดงความคืบหน้าด้วย progress bar พร้อมจำนวนแถวและสถานะ
- **Live Logs**: ดู log แบบ Real-time พร้อมกรองตามระดับความสำคัญ

### 🗓️ **การจัดการฐานข้อมูลแบบไดนามิก**
- **ฐานข้อมูลตามวันที่**: สร้างฐานข้อมูลใหม่อัตโนมัติตามรูปแบบวันที่/เวลา
- **Placeholders**: `{YYYY}`, `{MM}`, `{DD}`, `{HH}`, `{mm}`, `{ss}`
- **ตัวอย่าง**: `medkku_{YYYY}_{MM}_{DD}` → `medkku_2025_11_08`
- **Versioning**: เก็บประวัติแยกตามวันโดยไม่เขียนทับข้อมูลเก่า

### ⚙️ **การตั้งค่าที่ยืดหยุ่น**
- **ตั้งค่าผ่าน Web UI**: ปรับแต่งค่าทั้งหมดผ่าน Dashboard
- **Environment Variables**: จัดการไฟล์ `.env` ได้ครบถ้วน
- **กรองตาราง**: เลือกหรือยกเว้นตารางที่ต้องการ
- **กำหนดเวลารัน**: รันอัตโนมัติตามช่วงเวลาที่กำหนด (วัน/ชั่วโมง/นาที)
- **ปรับแต่งประสิทธิภาพ**: ปรับจำนวน workers, ขนาด batch และ timeout ได้

## 🏗️ สถาปัตยกรรม

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  Source MySQL   │────▶│   ETL Pipeline       │────▶│  Dest MySQL     │
│  (อ่านอย่างเดียว)│     │  ┌────────────────┐  │     │  (สร้างอัตโนมัติ)│
│                 │     │  │ 10 Producers   │  │     │                 │
│  ตาราง: 54      │     │  │ (ดึงข้อมูล)    │  │     │  สร้างใหม่       │
│  แถว: หลายล้าน   │     │  └────────┬───────┘  │     │  ทุกครั้งที่รัน  │
└─────────────────┘     │           │          │     └─────────────────┘
                        │           ▼          │
                        │  ┌────────────────┐  │
                        │  │ Queue (async)  │  │
                        │  └────────┬───────┘  │
                        │           │          │
                        │           ▼          │
                        │  ┌────────────────┐  │
                        │  │ 10 Consumers   │  │
                        │  │ (โหลดข้อมูล)   │  │
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

### ส่วนประกอบ
- **Producers**: ดึงข้อมูลแบบขนานจากฐานข้อมูลต้นทาง
- **Consumers**: แปลงและโหลดข้อมูลไปยังฐานข้อมูลปลายทาง
- **Monitor**: เซิร์ฟเวอร์ Flask + Socket.IO สำหรับ Dashboard แบบ Real-time
- **Cache**: แคชข้อมูล schema และ metadata ด้วย Redis (ตัวเลือก)

## 🐳 เริ่มต้นใช้งาน

### ความต้องการ
- Docker & Docker Compose
- ฐานข้อมูล MySQL/MariaDB ต้นทาง (เข้าถึงได้จาก Docker)

### การติดตั้ง

1. **Clone repository**
```bash
git clone <your-repo-url>
cd med
```

2. **ตั้งค่า environment**
```bash
# คัดลอกไฟล์ตัวอย่าง
cp .env.example .env

# แก้ไขไฟล์ .env ใส่ข้อมูลฐานข้อมูลของคุณ
nano .env
```

3. **เริ่มต้นใช้งาน**
```bash
docker-compose up -d
```

4. **เข้าถึง Dashboard**
```
http://localhost:5000
```

5. **ดู logs**
```bash
docker-compose logs -f etl
```

## 📝 การตั้งค่า

### ตัวแปร Environment

| ตัวแปร | คำอธิบาย | ตัวอย่าง | ค่าเริ่มต้น |
|----------|-------------|---------|---------|
| **ฐานข้อมูลต้นทาง** |
| `SRC_DB_HOST` | โฮสต์ฐานข้อมูลต้นทาง | `your-host` | `localhost` |
| `SRC_DB_PORT` | พอร์ตฐานข้อมูลต้นทาง | `3306` | `3306` |
| `SRC_DB_NAME` | ชื่อฐานข้อมูลต้นทาง | `source_db` | `test` |
| `SRC_DB_USER` | ผู้ใช้ฐานข้อมูลต้นทาง | `username` | `root` |
| `SRC_DB_PASSWORD` | รหัสผ่านฐานข้อมูลต้นทาง | `password` | - |
| **ฐานข้อมูลปลายทาง** |
| `DST_DB_HOST` | โฮสต์ปลายทาง | `host.docker.internal` | `localhost` |
| `DST_DB_PORT` | พอร์ตปลายทาง | `3306` | `3306` |
| `DST_DB_NAME` | ชื่อฐานข้อมูล (รองรับ placeholders) | `backup_{YYYY}_{MM}_{DD}` | `test` |
| `DST_DB_DYNAMIC` | เปิดใช้งานการสร้างฐานข้อมูลแบบไดนามิก | `true` / `false` | `false` |
| `DST_DB_USER` | ผู้ใช้ปลายทาง | `username` | `root` |
| `DST_DB_PASSWORD` | รหัสผ่านปลายทาง | `password` | - |
| **การกรองตาราง** |
| `INCLUDE_TABLES` | ตารางที่ต้องการ (คั่นด้วยคอมม่า) | `tbl_user,tbl_order` | (ทั้งหมด) |
| `EXCLUDE_TABLES` | ตารางที่ไม่ต้องการ (คั่นด้วยคอมม่า) | `temp_*,test_*` | (ไม่มี) |
| **การตั้งค่า ETL** |
| `LOG_LEVEL` | ระดับการบันทึก log | `DEBUG`/`INFO`/`WARNING`/`ERROR` | `INFO` |
| `MAX_WORKERS` | จำนวน workers ทั้งหมด (producers + consumers) | `20` | `20` |
| `BATCH_SIZE` | จำนวนแถวต่อ batch | `5000` | `5000` |
| **การปรับแต่งประสิทธิภาพ** |
| `MAX_TABLE_TIME_SECONDS` | เวลาสูงสุดต่อตาราง (0=ไม่จำกัด) | `0` | `0` |
| `MIN_BATCH_SIZE` | ขนาด batch ขั้นต่ำ | `5000` | `5000` |
| `MAX_BATCH_SIZE` | ขนาด batch สูงสุด | `10000` | `10000` |
| **การกำหนดเวลา** |
| `ETL_INTERVAL_SECONDS` | ช่วงเวลารันอัตโนมัติ (0=รันครั้งเดียว) | `300` | `300` |

### การตั้งชื่อฐานข้อมูลแบบไดนามิก

ใช้ placeholders ใน `DST_DB_NAME` เพื่อสร้างฐานข้อมูลตามเวลา:

| Placeholder | คำอธิบาย | ตัวอย่าง |
|-------------|-------------|---------|
| `{YYYY}` | ปี 4 หลัก | `2025` |
| `{MM}` | เดือน 2 หลัก | `11` |
| `{DD}` | วัน 2 หลัก | `08` |
| `{HH}` | ชั่วโมง 2 หลัก (24 ชม.) | `14` |
| `{mm}` | นาที 2 หลัก | `30` |
| `{ss}` | วินาที 2 หลัก | `45` |

**ตัวอย่าง:**
```env
# สำรองข้อมูลรายวัน
DST_DB_NAME=backup_{YYYY}_{MM}_{DD}
# ผลลัพธ์: backup_2025_11_08

# Snapshot รายชั่วโมง
DST_DB_NAME=snapshot_{YYYY}{MM}{DD}_{HH}00
# ผลลัพธ์: snapshot_20251108_1400

# รูปแบบไทย
DST_DB_NAME=medkku.{YYYY}.{MM}.{DD}
# ผลลัพธ์: medkku.2025.11.08
```

### การตั้งค่าผ่าน Dashboard

ตั้งค่าทั้งหมดได้ผ่าน Dashboard ที่ `http://localhost:5000`:

1. **⚙️ Settings Modal**
   - แก้ไขตัวแปร `.env` ทั้งหมด
   - ข้อมูลฐานข้อมูลต้นทาง/ปลายทาง
   - รูปแบบการกรองตาราง
   - พารามิเตอร์ปรับแต่งประสิทธิภาพ
   - ช่วงเวลารัน (วัน/ชั่วโมง/นาที)

2. **📊 Workers Progress** (อัปเดตทุก 5 วินาที)
   - Producers ที่ทำงานอยู่ของแต่ละตาราง
   - Consumers ที่ทำงานอยู่ของแต่ละตาราง
   - จำนวนแถวที่ประมวลผล
   - สถานะปัจจุบัน

3. **📈 System Stats**
   - การใช้งาน CPU
   - การใช้งานหน่วยความจำ
   - การใช้งานดิสก์

4. **📋 Live Logs**
   - ดู log แบบ Real-time
   - กรองตามระดับความสำคัญ
   - เลื่อนอัตโนมัติ

## 🎯 กรณีการใช้งาน

### 1. สำรองข้อมูลรายวัน
```env
DST_DB_NAME=backup_{YYYY}_{MM}_{DD}
DST_DB_DYNAMIC=true
ETL_INTERVAL_SECONDS=86400  # 24 ชั่วโมง
```

### 2. Snapshot ข้อมูลรายชั่วโมง
```env
DST_DB_NAME=snapshot_{YYYY}{MM}{DD}_{HH}00
DST_DB_DYNAMIC=true
ETL_INTERVAL_SECONDS=3600  # 1 ชั่วโมง
```

### 3. ย้ายข้อมูลครั้งเดียว
```env
DST_DB_NAME=production_db
DST_DB_DYNAMIC=false
ETL_INTERVAL_SECONDS=0  # รันครั้งเดียว
```

### 4. ย้ายเฉพาะตารางที่เลือก
```env
INCLUDE_TABLES=tbl_user,tbl_order,tbl_product
EXCLUDE_TABLES=
```

### 5. ยกเว้นตารางชั่วคราว
```env
INCLUDE_TABLES=
EXCLUDE_TABLES=temp_,test_,backup_
```

## 📊 ประสิทธิภาพ

### ความเร็วในการประมวลผล
- **ตารางเล็ก** (<10K แถว): ~100K แถว/วินาที
- **ตารางกลาง** (10K-100K): ~200K แถว/วินาที
- **ตารางใหญ่** (>1M แถว): ~300K-500K แถว/วินาที

*ประสิทธิภาพขึ้นอยู่กับฮาร์ดแวร์ เครือข่าย และการตั้งค่าฐานข้อมูล*

### เคล็ดลับการปรับแต่ง

| ขนาดตาราง | Workers | Batch Size | กลยุทธ์ |
|------------|---------|------------|----------|
| < 10K แถว | 2-3 | 1,000 | ลด overhead |
| 10K-100K | 5 | 5,000 | สมดุล |
| 100K-1M | 8 | 5,000 | ขนานปานกลาง |
| > 1M แถว | 10 | 5,000 | ขนานสูง |

**โหมดอนุรักษ์นิยม (แนะนำเพื่อความเสถียร):**
```env
MAX_WORKERS=20
BATCH_SIZE=5000
MAX_TABLE_TIME_SECONDS=0  # ไม่จำกัดเวลา
```

**โหมดเร็วสูงสุด:**
```env
MAX_WORKERS=50
BATCH_SIZE=10000
MAX_TABLE_TIME_SECONDS=3600  # จำกัด 1 ชั่วโมง
```

## 🛠️ เทคโนโลยีที่ใช้

- **Backend**: Python 3.11, asyncio, aiomysql
- **Web Server**: Flask, Flask-SocketIO
- **Frontend**: Vanilla JavaScript, Socket.IO client
- **Database**: MySQL 8.0 / MariaDB 11.8
- **Cache**: Redis (ตัวเลือก)
- **Containerization**: Docker, Docker Compose

## 📦 โครงสร้างโปรเจค

```
med/
├── etl/
│   ├── app/
│   │   ├── main.py                  # ETL pipeline หลัก
│   │   ├── monitor.py               # Dashboard server (Flask + Socket.IO)
│   │   ├── cache_storage.py         # ชั้น Redis caching
│   │   ├── templates/
│   │   │   └── dashboard.html       # Dashboard UI
│   │   └── static/
│   │       └── dashboard_socket.js  # WebSocket client แบบ Real-time
│   └── Dockerfile
├── docker-compose.yml               # การตั้งค่า Docker services
├── .env                             # ตัวแปร Environment
├── .env.example                     # ตัวอย่างการตั้งค่า
├── requirements.txt                 # Python dependencies
└── README.md
```

## 🔍 การติดตามและแก้ไขปัญหา

### ฟีเจอร์ Dashboard

1. **ภาพรวมสถานะ**
   - สถานะปัจจุบัน: กำลังทำงาน/รอ/เสร็จสิ้น/ล้มเหลว
   - เวลาที่ใช้
   - ตาราง: ทั้งหมด/เสร็จสิ้น/ล้มเหลว
   - ตารางที่กำลังประมวลผล

2. **ความคืบหน้าตาราง**
   - Progress bars แบบ visual
   - จำนวนแถว (ประมวลผล/ทั้งหมด)
   - เปอร์เซ็นต์ที่เสร็จ
   - เวลาโดยประมาณ (ETA)

3. **Workers Modal**
   - สถานะ worker แบบ Real-time
   - Producers: ทำงาน/ทั้งหมด
   - Consumers: ทำงาน/ทั้งหมด
   - แถวที่ประมวลผลต่อ worker
   - อัปเดตอัตโนมัติทุก 5 วินาที

4. **ทรัพยากรระบบ**
   - การใช้งาน CPU (%)
   - การใช้งานหน่วยความจำ (MB)
   - การใช้งานดิสก์ (GB)

5. **Live Logs**
   - ดู log แบบ Real-time
   - ระดับความสำคัญ: DEBUG/INFO/WARNING/ERROR
   - เลื่อนอัตโนมัติไปที่ล่าสุด
   - กรองได้

### Socket.IO Events

ข้อมูล Dashboard ทั้งหมดส่งผ่าน WebSocket:

| Client → Server | Server → Client | คำอธิบาย |
|-----------------|-----------------|-------------|
| `connect` | `initial_state` | เชื่อมต่อครั้งแรก |
| `request_status` | `status_update` | อัปเดตสถานะ ETL |
| `request_tables` | `tables_update` | ความคืบหน้าตาราง |
| `request_logs` | `logs_update` | รายการ log |
| `request_config` | `config_update` | การตั้งค่า |
| `save_config` | `config_saved` | ผลการบันทึก |

### คำสั่ง Docker

```bash
# เริ่มต้น services
docker-compose up -d

# ดู logs
docker-compose logs -f etl

# รีสตาร์ท ETL
docker-compose restart etl

# หยุด services
docker-compose down

# สร้างใหม่หลังแก้ไขโค้ด
docker-compose up -d --build
```

## 🔒 ความปลอดภัย

- **ข้อมูลรับรอง**: เก็บในไฟล์ `.env` (ไม่อัปโหลด git)
- **Docker Isolation**: Services ทำงานใน container แยก
- **Read-only Source**: เข้าถึงฐานข้อมูลต้นทางแบบอ่านอย่างเดียว
- **Connection Limits**: จำกัดขนาด pool ป้องกันการใช้ทรัพยากรมากเกินไป
- **ฟิลด์รหัสผ่าน**: ซ่อนใน Dashboard UI

## 🐛 แก้ไขปัญหา

### ปัญหาที่พบบ่อย

**1. เชื่อมต่อฐานข้อมูลไม่ได้**
```bash
# ตรวจสอบว่าเข้าถึงฐานข้อมูลต้นทางได้หรือไม่
docker exec -it mariadb-etl ping <SRC_DB_HOST>

# ตรวจสอบข้อมูลรับรอง
docker exec -it mariadb-etl mysql -h <SRC_DB_HOST> -u <SRC_DB_USER> -p
```

**2. ฐานข้อมูลแบบไดนามิกไม่ถูกสร้าง**
```bash
# ตรวจสอบว่าเปิด DST_DB_DYNAMIC หรือไม่
grep DST_DB_DYNAMIC .env

# ตรวจสอบสิทธิ์ฐานข้อมูลปลายทาง
# ผู้ใช้ต้องมีสิทธิ์ CREATE DATABASE
```

**3. Workers ไม่แสดงใน Dashboard**
```bash
# ตรวจสอบว่า ETL กำลังทำงานหรือไม่
docker-compose logs etl | grep "Starting Parallel ETL"

# ตรวจสอบการเชื่อมต่อ Socket.IO
# เปิด browser console (F12) และตรวจสอบ WebSocket errors
```

**4. ประสิทธิภาพช้า**
```bash
# ลดขนาด batch
BATCH_SIZE=1000

# ลดจำนวน workers
MAX_WORKERS=10

# ตรวจสอบ network latency
docker exec -it mariadb-etl ping <SRC_DB_HOST>
```

## 🤝 การมีส่วนร่วม

ยินดีรับการมีส่วนร่วม! สามารถส่ง Pull Request ได้เลย

1. Fork repository
2. สร้าง feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit การเปลี่ยนแปลง (`git commit -m 'Add some AmazingFeature'`)
4. Push ไปยัง branch (`git push origin feature/AmazingFeature`)
5. เปิด Pull Request

## 📄 ใบอนุญาต

MIT License - ใช้งานได้ทั้งส่วนตัวและเชิงพาณิชย์

## 🙏 กิตติกรรมประกาศ

- สร้างด้วย Python asyncio เพื่อประสิทธิภาพ async I/O สูง
- แรงบันดาลใจจากสถาปัตยกรรม AWS Glue ETL
- Dashboard แบบ Real-time ขับเคลื่อนด้วย Socket.IO
- Connection pooling ด้วย aiomysql

## 📞 การสนับสนุน

สำหรับปัญหา คำถาม หรือข้อเสนอแนะ:
- เปิด issue บน GitHub
- ตรวจสอบส่วนแก้ไขปัญหา
- ดู Docker logs: `docker-compose logs -f etl`

---

**⭐ กด Star repo นี้ถ้าคุณชอบ!**

สร้างด้วย ❤️ เพื่อการย้ายข้อมูลและสำรองข้อมูลที่มีประสิทธิภาพ
