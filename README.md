# Clickstream Data for Online Shopping Setup

## 1. Start Docker Services

Pastikan berada di root folder project:

```bash
cd BDP_ALP
```

Jalankan seluruh services menggunakan Docker Compose:

```bash
docker compose up
```

---

## 2. Install Required Libraries

### Install dependencies untuk Spark jobs

Masuk ke folder `jobs`:

```bash
cd jobs
```

Install requirements:

```bash
pip install -r requirements.txt
```

---

### Install dependencies untuk Kafka producer

Masuk ke folder `producer`:

```bash
cd ../producer
```

Install requirements:

```bash
pip install -r requirements.txt
```

Required packages:

- kafka-python
- pandas

> Note: `pandas` kemungkinan sudah terinstall sebelumnya dari folder `jobs`, namun dapat ditambahkan kembali ke `requirements.txt` jika diperlukan.

---

## 3. Create Kafka Topic

Kembali ke root project:

```bash
cd ..
```

Create Kafka topic menggunakan command berikut:

```bash
docker exec -it alp-kafka bash /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic clickstream-events \
--partitions 3 \
--replication-factor 1
```

Jika berhasil, akan muncul output:

```text
Created topic clickstream-events
```

Jika topic sudah pernah dibuat sebelumnya, Kafka akan menampilkan:

```text
Topic 'clickstream-events' already exists
```

---

## 4. Run Kafka Producer

Masuk ke folder producer:

```bash
cd producer
```

Jalankan producer:

```bash
python producer.py
```

Jika berhasil, terminal akan menampilkan stream event seperti berikut:

```text
Sent: {
  'event_time': '2008-04-01',
  'session_id': 1,
  'country': 29,
  'main_category': 1,
  'clothing_model': 'A13',
  'colour': 1,
  'location': 5,
  'price': 28,
  'price_level': 2,
  'page': 1,
  'order_in_session': 1
}
```

Producer akan terus mengirim data ke Kafka secara streaming.

---

## 5. Kafka Consumer Testing

Buka terminal baru untuk memastikan Kafka menerima message.

Masuk ke Kafka container:

```bash
docker exec -it alp-kafka bash
```

Jalankan Kafka consumer:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic clickstream-events \
--from-beginning
```

Jika berhasil, consumer akan menampilkan JSON event yang dikirim producer:

```json
{
  "event_time":"2008-04-01",
  "session_id":1,
  "country":29,
  "main_category":1,
  "clothing_model":"A13",
  "colour":1,
  "location":5,
  "price":28,
  "price_level":2,
  "page":1,
  "order_in_session":1
}
```

---

# JSON Event Schema

Topic Name:

```text
clickstream-events
```

Example Event:

```json
{
  "event_time": "2008-04-01",
  "session_id": 1,
  "country": 29,
  "main_category": 1,
  "clothing_model": "A13",
  "colour": 1,
  "location": 5,
  "price": 28,
  "price_level": 2,
  "page": 1,
  "order_in_session": 1
}
```

| Field | Type | Description |
|---|---|---|
| event_time | string | event date |
| session_id | integer | shopping session ID |
| country | integer | visitor country code |
| main_category | integer | main clothing category |
| clothing_model | string | clothing model code |
| colour | integer | clothing color code |
| location | integer | product display location |
| price | integer | product price |
| price_level | integer | price category |
| page | integer | visited page |
| order_in_session | integer | click order in session |

---

## 6. Run Spark Raw Streaming Job

Setelah Kafka Producer dipastikan aktif mengalirkan data, jalankan Spark Structured Streaming untuk melakukan penyerapan data mentah (*raw data ingestion*), pemetaan skema, dan pengecekan toleransi kesalahan (*fault-tolerance*).

Buka terminal baru di root folder proyek, lalu jalankan perintah eksekusi *Spark Submit* absolut ke kontainer Master berikut:

```bash
docker exec -it alp-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/alp/jobs/streaming_raw.py

```

Jika berhasil dijalankan, konsol terminal Spark akan memunculkan representasi data tabular dari micro-batch yang ter-update secara berkala (append mode):
-------------------------------------------
Batch: 1
-------------------------------------------
+----------+----------+-------+-------------+--------------+------+--------+-----+-----------+----+----------------+
|event_time|session_id|country|main_category|clothing_model|colour|location|price|price_level|page|order_in_session|
+----------+----------+-------+-------------+--------------+------+--------+-----+-----------+----+----------------+
|2008-04-01|         2|     29|            4|            P1|     3|       1|   38|          1|   1|               8|
+----------+----------+-------+-------------+--------------+------+--------+-----+-----------+----+----------------+

## 7. Run Spark Aggregation Streaming Job

Setelah validasi *raw streaming* berhasil dilakukan, langkah berikutnya adalah menjalankan *Spark Structured Streaming Aggregation Job*. Job ini bertugas melakukan agregasi data clickstream secara real-time berdasarkan kategori produk utama (*main_category*) dan menghasilkan output yang dapat dibaca langsung oleh dashboard Streamlit.

Aggregation job akan:

- Membaca data streaming dari Kafka topic `clickstream-events`
- Melakukan parsing JSON sesuai skema dataset
- Mengelompokkan data berdasarkan `main_category`
- Menghitung jumlah view untuk setiap kategori produk
- Menyimpan snapshot agregasi terbaru ke `latest_snapshot.json`
- Menyimpan riwayat batch ke `history.jsonl`
- Menyediakan data real-time untuk visualisasi dashboard

Sebelum menjalankan Aggregation Job, pastikan proses `streaming_raw.py` telah dihentikan terlebih dahulu.

Jika terminal Raw Streaming masih berjalan, hentikan dengan:

```bash
Ctrl + C
```

Hal ini diperlukan karena Spark Worker memiliki resource yang terbatas. Menjalankan Raw Streaming dan Aggregation secara bersamaan dapat menyebabkan Aggregation Job gagal memperoleh executor.

Buka Spark Master UI untuk memastikan tidak ada lagi aplikasi Raw Streaming yang berjalan:

```text
http://localhost:8082
```

Setelah resource Spark tersedia, jalankan Aggregation Job menggunakan perintah berikut:

```bash
docker exec -it alp-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/alp/jobs/streaming_job.py
```

Jika berhasil dijalankan, Spark akan menampilkan hasil agregasi setiap micro-batch:

```text
------------------------------------------------------------
Batch: 0 | Total events: 706
------------------------------------------------------------

+-------------+-----+
|main_category|count|
+-------------+-----+
|1            |223  |
|2            |216  |
|3            |157  |
|4            |110  |
+-------------+-----+
```

Keterangan:

- `main_category` menunjukkan kategori utama produk fashion.
- `count` menunjukkan jumlah event yang telah diterima Spark untuk kategori tersebut.
- `Total events` merupakan jumlah kumulatif seluruh event yang telah diproses hingga batch saat ini.

Karena Spark menggunakan output mode `complete`, setiap batch akan menampilkan total agregasi terkini sejak offset awal Kafka.

Jika Aggregation Job berhasil berjalan, folder `dashboard_data` akan berisi file:

```text
dashboard_data/
├── latest_snapshot.json
└── history.jsonl
```

Verifikasi menggunakan:

```bash
ls dashboard_data
```

Untuk melihat snapshot agregasi terbaru, jalankan:

```bash
cat dashboard_data/latest_snapshot.json
```

Contoh output:

```json
{
  "batch_id": 28,
  "updated_at": "2026-05-29T13:38:33Z",
  "rows": [
    {
      "main_category": 1,
      "count": 110
    },
    {
      "main_category": 2,
      "count": 110
    },
    {
      "main_category": 3,
      "count": 76
    },
    {
      "main_category": 4,
      "count": 49
    }
  ]
}
```

File tersebut akan terus diperbarui setiap kali Spark menyelesaikan micro-batch baru.

---

## 8. Run Streamlit Dashboard

Setelah Aggregation Job berhasil menghasilkan data, dashboard dapat diakses melalui browser untuk memantau hasil agregasi secara real-time.

Buka browser dan akses alamat berikut:

```text
http://localhost:8501
```

Dashboard akan membaca file `latest_snapshot.json` dan `history.jsonl` yang dihasilkan oleh Spark Aggregation Job.

Jika dashboard berhasil berjalan, akan muncul beberapa komponen utama:

### Real-Time Metrics

Dashboard menampilkan informasi ringkas mengenai kondisi streaming saat ini, meliputi:

- Batch ID terbaru
- Timestamp pembaruan terakhir
- Total event yang telah diproses

Contoh:

```text
Batch ID: 28
Updated At: 2026-05-29T13:38:33Z
Total Events: 345
```

### Views by Main Category

Visualisasi bar chart yang menunjukkan jumlah view untuk masing-masing kategori produk fashion berdasarkan hasil agregasi Spark Structured Streaming.

Contoh:

```text
Main Category 1 : 110
Main Category 2 : 110
Main Category 3 : 76
Main Category 4 : 49
```

### Total Events per Batch

Grafik line chart yang menunjukkan perkembangan jumlah event yang berhasil diproses pada setiap micro-batch.

Karena aggregation menggunakan *complete output mode*, jumlah event akan terus bertambah selama producer masih mengirimkan data ke Kafka.

### Current Snapshot Rows

Tabel yang menampilkan hasil agregasi terbaru yang tersimpan pada file `latest_snapshot.json`.

Contoh:

| main_category | count |
|--------------|-------|
| 1 | 110 |
| 2 | 110 |
| 3 | 76 |
| 4 | 49 |

### Batch History

Tabel riwayat seluruh micro-batch yang telah diproses Spark.

Informasi yang ditampilkan meliputi:

- Batch ID
- Timestamp update
- Total events
- Distribusi count per kategori

Contoh:

| batch_id | total_events |
|----------|-------------|
| 28 | 345 |
| 27 | 334 |
| 26 | 323 |

---

### Dashboard Validation

Dashboard dianggap berhasil apabila:

- Batch ID terus bertambah

- Total event meningkat selama producer berjalan

- Grafik kategori berubah secara real-time

- Tabel snapshot menampilkan count untuk setiap `main_category`

- Riwayat batch terus bertambah seiring berjalannya streaming job

- Tidak terdapat error pada halaman dashboard

Jika seluruh komponen di atas berhasil ditampilkan, maka integrasi Kafka → Spark Structured Streaming → Aggregation → Streamlit Dashboard telah berjalan dengan baik.