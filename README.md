# Clickstream Analytics Pipeline for Online Fashion Shopping

 ## Table of Contents
 1. [Overview](#-overview)
 2. [Problem Statement](#-problem-statement)
 3. [Dataset](#-dataset)
 4. [Architecture](#-architecture)
 5. [Tech Stack](#-tech-stack)
 6. [Project Structure](#-project-structure)
 7. [Prerequisites](#-prerequisites)
 8. [How to Run](#-how-to-run)
 9. [Services & Ports](#-services--ports)
 10. [Pipeline Explanation](#-pipeline-explanation)
 11. [Dashboard](#-dashboard)
 12. [Team](#-team)

---
 ## Overview

Project ini membangun sebuah **end-to-end big data pipeline** yang mampu memproses data clickstream dari sebuah toko pakaian online secara **real-time (streaming)** maupun **batch**. Pipeline ini mensimulasikan alur kerja nyata di industri e-commerce, mulai dari pengiriman event klik pengguna, pemrosesan skala besar, hingga visualisasi insight di dashboard interaktif.

**Apa itu clickstream?**  
Setiap kali seseorang membuka halaman produk, mengklik item, atau berpindah halaman di sebuah toko online, aktivitas tersebut dicatat sebagai sebuah "event". Kumpulan event inilah yang disebut **clickstream data**. Dengan menganalisisnya, kita bisa memahami perilaku belanja, produk populer, hingga pola navigasi pengguna.

---
## Problem Statement

Toko fashion online menghasilkan ribuan event klik setiap harinya. Tantangan utamanya:

- **Bagaimana memproses data volume besar secara efisien?** Data clickstream bisa mencapai ribuan baris per hari.
- **Bagaimana mendapatkan insight secara real-time?** Bisnis perlu tahu produk mana yang sedang tren *saat ini*, bukan kemarin.
- **Bagaimana menggabungkan analisis historis dan live?** Keputusan bisnis membutuhkan konteks dari masa lalu dan kondisi terkini.

Project ini menjawab ketiga tantangan tersebut dengan membangun pipeline yang memisahkan jalur **streaming** (untuk analisis real-time) dan **batch** (untuk analisis historis).

---
 ## Project Description

Proyek ini mengimplementasikan pipeline analitik clickstream secara real-time dan batch untuk toko pakaian online untuk wanita hamil asal Polandia yang memiliki lebih dari 165.000 data. Setiap kali pengguna mengklik atau membuka halaman produk, aktivitas tersebut dicatat dan dikirim ke sistem untuk dianalisis secara langsung maupun secara keseluruhan.

Sistem ini menggunakan Apache Kafka untuk mengalirkan data, Apache Spark untuk memprosesnya, dan Streamlit untuk menampilkan hasilnya dalam bentuk grafik dan tabel yang mudah dipahami. Semua komponen dijalankan sekaligus hanya dalam satu perintah menggunakan Docker Compose.

---
## Dataset

**Sumber:** [Clickstream Data for Online Shopping – Kaggle](https://www.kaggle.com/datasets/tunguz/clickstream-data-for-online-shopping)

**File:** `data/e-shop-clothing-2008.csv`

Dataset ini berisi data clickstream dari sebuah toko pakaian online untuk wanita hamil di Polandia selama tahun 2008, dengan sekitar **165.000+ baris** data. Dataset ini mengambil data 5 bulan di 2008 untuk mengetahui dan membandingkan perilaku belanja (konsumsi) di Polandia dan negara sekitar di Eropa.

### Schema Dataset

| Kolom | Tipe | Deskripsi |
|---|---|---|
| `year` | integer | Tahun event |
| `month` | integer | Bulan event |
| `day` | integer | Hari event |
| `order` | integer | Urutan klik dalam satu sesi |
| `country` | integer | Kode negara pengunjung |
| `session ID` | integer | ID unik sesi belanja |
| `page 1 (main category)` | integer | Kategori utama pakaian yang dikunjungi |
| `page 2 (clothing model)` | string | Kode model pakaian spesifik |
| `colour` | integer | Kode warna pakaian |
| `location` | integer | Lokasi produk di halaman |
| `model photography` | integer | Tipe foto produk |
| `price` | integer | Harga produk |
| `price 2` | integer | Level/kategori harga |
| `page` | integer | Nomor halaman yang dikunjungi |


---
### Event Schema (Kafka)

Setelah diproses oleh producer, setiap baris CSV dikirim ke Kafka dalam format JSON:

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

| Kolom | Tipe | Deskripsi |
|---|---|---|
| `event_time` | string | tanggal kegiatan |
| `session_id` | integer | ID unik sesi belanja |
| `country` | integer | Kode negara pengunjung |
| `main_category` | integer | kategori pakaian utama |
| `clothing_model` | string | Kode model pakaian |
| `colour` | integer | kode warna pakaian |
| `location` | integer | Lokasi produk di halaman |
| `price` | integer | Harga produk |
| `price_level` | integer | Kategori harga |
| `page` | integer | Nomor halaman yang dikunjungi |
| `order_in_session` | integer | Urutan klik dalam sesi |

---
## Architecture

![Architecture Diagram](assets/architecture.png)


### Alur Data (Data Flow)

1. **CSV → Producer:** `producer.py` membaca file CSV baris per baris dan mengirimkannya sebagai JSON event ke Kafka secara streaming.
2. **Kafka:** Bertindak sebagai message broker terpusat. Menyimpan event di topic `clickstream-fashion-events` dengan 3 partisi agar bisa diproses paralel.
3. **Spark Streaming:** Dua Spark job secara bersamaan mengkonsumsi data dari Kafka:
   - `streaming_raw.py` — menyerap data mentah dan memvalidasi skema
   - `streaming_aggregation.py` — melakukan agregasi real-time (hitungan per kategori, per negara, dll.)
4. **Spark Batch:** `batch_analysis.py` memproses keseluruhan dataset CSV untuk insight historis mendalam.
5. **Dashboard:** Streamlit membaca output dari Spark (disimpan di `dashboard_data/`) dan menampilkannya sebagai grafik dan tabel interaktif.


## Tech Stack

| Tech | Versi | Fungsi |
|---|---|---|
| **Apache Kafka** | 4.2.0 (KRaft) | Message streaming broker |
| **Apache Spark** | 4.0.0 | Distributed data processing |
| **PySpark** | 4.0.0 | Python API untuk Spark |
| **Streamlit** | latest | Interactive dashboard |
| **Docker & Docker Compose** | latest | Container orchestration |
| **Python** | 3.x | Bahasa pemrograman utama |
| **Kafka UI** | latest | Visual monitoring Kafka |
| **Jupyter Notebook** | PySpark | Data exploration |
| **Strimzi Kafka Bridge** | 0.33.1 | REST API untuk Kafka |

## Project Structure

```
project/
├── docker-compose.yml          # Definisi semua services (Kafka, Spark, Streamlit, dll)
├── README.md                   # Dokumentasi ini
│
├── config/                     # Konfigurasi Kafka (server.properties, env, bridge)
│
├── data/
│   └── e-shop-clothing-2008.csv   # Dataset clickstream utama
│
├── producer/
│   ├── producer.py             # Membaca CSV & mengirim event ke Kafka
│   └── requirements.txt        # Dependencies: kafka-python, pandas
│
├── jobs/
│   ├── streaming_raw.py        # Spark: konsumsi raw data dari Kafka
│   ├── streaming_aggregation.py # Spark: agregasi real-time dari Kafka
│   └── batch_analysis.py       # Spark: analisis batch dari CSV
│
├── dashboard/
│   ├── app.py                  # Aplikasi Streamlit (dashboard visualisasi)
│   ├── Dockerfile              # Container image untuk Streamlit
│   └── requirements.txt        # Dependencies Streamlit
│
├── checkpoints/                # Spark Structured Streaming checkpoints (fault-tolerance)
├── dashboard_data/             # Output Spark → dibaca oleh Streamlit
└── assets/
    └── architecture.png        # Gambar arsitektur pipeline
```

**Penjelasan folder penting:**
- `producer/` — "pintu masuk" data. Semua kode pengiriman event ada di sini.
- `jobs/` — "otak" pipeline. Semua logika pemrosesan Spark ada di sini.
- `dashboard/` — "wajah" pipeline. Semua kode visualisasi ada di sini.
- `checkpoints/` — folder otomatis diisi Spark untuk menyimpan progress streaming. Jangan dihapus saat job berjalan!
- `dashboard_data/` — "jembatan" antara Spark dan Streamlit. Spark menulis ke sini, Streamlit membaca dari sini.

---
## Prerequisites

Pastikan sudah terinstall:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (versi terbaru)
- [Docker Compose](https://docs.docker.com/compose/install/) (sudah termasuk di Docker Desktop)
- Python 3.8+ (untuk menjalankan producer & jobs di luar container, opsional)


---


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
--topic clickstream-fashion-events \
--partitions 3 \
--replication-factor 1
```

Jika berhasil, akan muncul output:

```text
Created topic clickstream-fashion-events
```

Jika topic sudah pernah dibuat sebelumnya, Kafka akan menampilkan:

```text
Topic 'clickstream-fashion-events' already exists
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
--topic clickstream-fashion-events \
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
clickstream-fashion-events
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