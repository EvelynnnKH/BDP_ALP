# Membuka Ulang Dashboard (Setelah Setup Awal Selesai)
Berikut adalah instruksi atau tahapan untuk membuka ulang dashboard tanpa mengulang seluruh step dari awal yang berada di readme.md

---
 ## 1. Buka Docker Desktop

Pastikan aplikasi Docker Desktop sudah dibuka sebelum masuk ke tahap berikutnya.

---
## 2. Jalankan Container

```
cd BDP_ALP              # masuk ke folder
docker compose up -d
```
Tunggu hingga semua container berstatus **running**.

---
 ## 3. Jalankan Kafka Producer (terminal baru)

```
cd producer
python producer.py
```

---
 ## 4. Jalankan Spark Streaming Job (terminal baru)

```
docker exec -it alp-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/alp/jobs/streaming_job.py
```

---
 ## 5. Buka Dashboard di Browser

```
https://localhost:8501
```
Dashboard akan update otomatis sesuai interval yang dipilih di slide sidebar. Pastikan ketiga terminal (Docker, producer, dan Spark) tetap terbuka agar data dapat berjalan secara live.

>>>Catatan: Kafka topic clickstream-events tidak perlu dibuat ulang karena sudah tersimpan dari setup sebeelumnya. Jika muncul error topic not found, jalankan kembali perintah create topic di tahapan 3 pada readme.md.
