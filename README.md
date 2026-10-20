# Go Reconciliation Ingestion Engine

High-performance, concurrent ETL file ingestion and date-partitioning daemon designed to replace PHP's `FileGroupingService.php`.

---

## 🏗️ System Architecture

```mermaid
graph TD
    NAS[NAS Storage / Incoming CSVs] -->|Concurrent Scan| GoDaemon[Go Ingestion Engine]
    
    subgraph "Go Ingestion Engine"
        GoDaemon --> Scanner[Scanner Service]
        Scanner --> WorkerPool[Worker Pool: Goroutines + Semaphore]
        WorkerPool --> CSVParser[Streaming CSV Parser & Date Splitter]
        CSVParser --> ExtractStore[Storage: /app/extract/...]
    end

    GoDaemon -->|1. Register History & Details| MySQL[(MySQL DB: file_check_histories / details)]
    GoDaemon -->|2. Dispatch ReconcileJob| LaravelQueue[Laravel Queue / Worker]
    
    LaravelApp[Laravel Web / Admin UI] -->|Option A: MySQL jobs Table (every 5s)| MySQLQueue[(MySQL jobs table)]
    LaravelApp -->|Option B: Redis Queue (BLPOP)| RedisQueue[(Redis Queue)]
    LaravelApp -->|Option C: HTTP API Trigger| GoAPI[Internal REST API]

    MySQLQueue --> GoDaemon
    RedisQueue --> GoDaemon
    GoAPI --> GoDaemon
```

---

## 📂 Project Structure

```
go-reconciliation/
├── cmd/
│   └── server/
│       └── main.go                  # Main entry point & graceful shutdown
├── internal/
│   ├── config/                      # Environment & YAML configurations
│   ├── domain/                      # Domain models, constants & interfaces
│   ├── repository/mysql/            # MySQL connection pool & queries
│   ├── service/                     # Ingestion, grouping & job dispatcher
│   ├── worker/                      # Goroutine worker pool & queue consumers
│   │   ├── pool.go                  # Concurrency worker pool & semaphores
│   │   └── queue_consumer.go        # Dual Consumer: Redis (BLPOP) + MySQL jobs table (5s poll)
│   └── api/                         # REST API for healthcheck & on-demand rerun
├── pkg/
│   ├── formatter/                   # Date & amount parsers
│   └── stringhelper/                # Delimiter & filename cleaners
├── Dockerfile                       # Multi-stage production build
├── docker-compose.yml               # Container orchestration
└── .env.example                     # Environment template
```

---

## 🔌 Dual Queue Support: How Laravel Assigns Jobs to Go

You can trigger jobs from Laravel using any of the 3 supported methods:

### Method 1: Using Laravel's MySQL `database` Queue (Polling every 5 seconds)
If Laravel uses `QUEUE_CONNECTION=database`, Laravel dispatches a job to the `go_grouping` queue:

```php
use Illuminate\Support\Facades\DB;

// Push directly to MySQL jobs table
DB::table('jobs')->insert([
    'queue'        => 'go_grouping',
    'payload'      => json_encode([
        'action'     => 'RERUN_FILE',
        'file_id'    => $fch->id,
        'project_id' => $fch->project_id,
        'filename'   => $fch->filename,
    ]),
    'attempts'     => 0,
    'reserved_at'  => null,
    'available_at' => now()->timestamp,
    'created_at'   => now()->timestamp,
]);
```
*The Go daemon automatically polls `jobs` table every 5 seconds, processes the job, and removes it safely.*

---

### Method 2: Using Laravel's Redis Queue (`BLPOP` Instant Pickup)
If Laravel uses `QUEUE_CONNECTION=redis`, Laravel pushes the payload to Redis:

```php
use Illuminate\Support\Facades\Redis;

Redis::rpush('queues:go_grouping', json_encode([
    'action'     => 'RERUN_FILE',
    'file_id'    => $fch->id,
    'project_id' => $fch->project_id,
    'filename'   => $fch->filename,
]));
```
*The Go daemon uses `BLPOP` to instantly pick up and execute the job in 0ms without database load.*

---

### Method 3: HTTP API Endpoint (Direct Webhook)
```php
use Illuminate\Support\Facades\Http;

Http::timeout(3)->post('http://127.0.0.1:8080/api/v1/grouping/rerun', [
    'file_id'    => $fch->id,
    'project_id' => $fch->project_id,
    'filename'   => $fch->filename,
]);
```

---

## 🚀 Running the Service

### A. Using Docker (Recommended for Production)

```bash
# 1. Build and run container
docker compose up -d --build

# 2. Check logs
docker compose logs -f go-reconciliation
```

### B. Using PM2 (Native Binary)

```bash
# 1. Build binary
go build -ldflags="-w -s" -o bin/go-reconciliation ./cmd/server/main.go

# 2. Run via PM2
pm2 start ./bin/go-reconciliation --name "go-grouping" --interpreter none
```
