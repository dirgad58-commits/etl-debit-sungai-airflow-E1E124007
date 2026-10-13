# Pipeline ETL Prediksi Debit Sungai dan Peringatan Dini Banjir

Repositori pengumpulan tugas praktikum Data Engineering dan Machine Learning berbasis Apache Airflow (Astro CLI), MySQL 8.0, dan Scikit-Learn.

Proyek ini mendukung pencapaian Sasaran Pembangunan Berkelanjutan (SDG):
- **SDG 13 (Penanganan Perubahan Iklim)**: Membangun sistem mitigasi bencana hidrometeorologi melalui pemodelan debit sungai.
- **SDG 11 (Kota dan Permukiman Berkelanjutan)**: Memberikan sistem peringatan dini banjir untuk kawasan perkotaan rentan di Indonesia.

---

## 1. Ringkasan Eksekutif

Bencana banjir sering menimbulkan kerugian besar akibat lambatnya sistem peringatan dini. Proyek ini mengimplementasikan pipeline terotomatisasi yang:
1. Mengekstraksi data cuaca dan hidrologi historis dari tiga sumber Application Programming Interface (API) publik secara berkala tanpa API key.
2. Mentransformasi dan memuat data ke dalam basis data relasional MySQL dengan skema tiga tabel yang saling berelasi.
3. Melatih model machine learning regresi time-series secara otomatis untuk memprediksi volume debit sungai satu hari ke depan.
4. Menghasilkan status peringatan dini banjir (*flood warning*) jika estimasi debit melampaui persentil ke-90 historis lokasi bersangkutan.

---

## 2. Struktur Direktori Proyek

```text
etl-debit-sungai-airflow/
├── dags/
│   ├── .airflowignore
│   └── etl_flood_discharge.py           # DAG utama pipeline ETL dan orkestrasi
├── database/
│   ├── schema_banjir_db.sql             # Skema DDL 3 tabel relasional MySQL
│   └── query_verifikasi.sql             # Kumpulan query uji relasi dan integritas data
├── data/
│   ├── locations.csv                    # Data profil 24 lokasi kota
│   ├── river_discharge.csv              # Data historis debit sungai harian
│   ├── rainfall_daily.csv               # Data historis curah hujan harian
│   ├── riwayat_harian.csv               # Data gabungan hasil ETL siap latih
│   └── ringkasan_eda.csv                # Ringkasan statistik dan peringkat risiko
├── include/
│   └── data/
│       └── flood_predictions.csv        # Hasil inferensi prediksi debit dan status banjir
├── model/
│   ├── model_gbr_debit.joblib           # Model serialisasi Gradient Boosting Regressor
│   └── model_meta.json                  # Metadata fitur, metrik, dan ambang batas
├── notebook/
│   ├── proyek_prediksi_debit_sungai.ipynb # Eksplorasi data, rekayasa fitur, dan validasi
│   └── model_prediksi_banjir_gbr.ipynb    # Eksperimen pemodelan Machine Learning
├── airflow_settings.yaml                # Konfigurasi koneksi Airflow lokal
├── docker-compose.override.yml          # Konfigurasi container service MySQL 8.0
├── Dockerfile                           # Definisi container image Astro Runtime
├── packages.txt                         # Dependensi sistem tingkat OS
├── requirements.txt                     # Dependensi pustaka Python
├── .gitignore
├── .dockerignore
└── README.md
```

---

## 3. Sumber Data (Tiga API Terbuka)

Pipeline menarik data dari tiga sumber API yang berbeda tanpa memerlukan kunci otentikasi (API Key):

| No | Sumber Data | Endpoint URL | Fungsi Data |
|---|---|---|---|
| 1 | Open-Meteo Geocoding API | `https://geocoding-api.open-meteo.com/v1/search` | Mengambil data latitude, longitude, elevasi, populasi, dan provinsi kota target. |
| 2 | Open-Meteo Flood API | `https://flood-api.open-meteo.com/v1/flood` | Mengambil data volume debit sungai harian (*river discharge*, m3/s). |
| 3 | NASA POWER Daily API | `https://power.larc.nasa.gov/api/temporal/daily/point` | Mengambil parameter presipitasi curah hujan harian (*PRECTOTCORR*, mm/hari). |

Cakupan wilayah pemantauan mencakup 24 kota rawan banjir di Indonesia:
- **Sumatra**: Banda Aceh (Krueng Aceh), Medan (Deli), Padang (Batang Arau), Pekanbaru (Siak), Jambi (Batanghari), Palembang (Musi).
- **Jawa**: Jakarta (Ciliwung), Bekasi (Cikeas), Bandung (Citarum), Semarang (Banjir Kanal Barat), Surakarta (Bengawan Solo), Yogyakarta (Code), Surabaya (Brantas).
- **Kalimantan**: Pontianak (Kapuas), Palangkaraya (Kahayan), Banjarmasin (Martapura), Samarinda (Mahakam).
- **Sulawesi**: Makassar (Jeneberang), Palu (Palu), Kendari (Wanggu), Manado (Tondano).
- **Bali dan Nusa Tenggara**: Denpasar (Badung), Mataram (Jangkok).
- **Papua**: Merauke (Maro).

---

## 4. Desain Skema Basis Data (MySQL 8.0)

Basis data menggunakan nama `banjir_db` dengan mesin penyimpanan InnoDB guna memastikan integritas referensial.

```text
locations (PK: location_id)
   │
   ├──< river_discharge (PK: location_id, obs_date | FK: location_id -> locations)
   │
   └──< rainfall_daily  (PK: location_id, obs_date | FK: location_id -> locations)
```

### Tabel Relasi:
1. **`locations`** (Tabel Induk):
   Menyimpan profil wilayah pengamatan.
   - `location_id` (VARCHAR(10), Primary Key)
   - `city_name` (VARCHAR(100)), `river_name` (VARCHAR(100)), `province` (VARCHAR(100))
   - `latitude` (DECIMAL(9,6)), `longitude` (DECIMAL(9,6))
   - `elevation_m` (DECIMAL(8,2)), `population` (BIGINT)

2. **`river_discharge`** (Tabel Anak):
   Menyimpan rekaman observasi debit sungai harian.
   - Primary Key: Komposit (`location_id`, `obs_date`)
   - Foreign Key: `location_id` merujuk ke `locations(location_id)`

3. **`rainfall_daily`** (Tabel Anak):
   Menyimpan rekaman observasi curah hujan harian.
   - Primary Key: Komposit (`location_id`, `obs_date`)
   - Foreign Key: `location_id` merujuk ke `locations(location_id)`

---

## 5. Alur Orkestrasi Apache Airflow

DAG didefinisikan pada file `dags/etl_flood_discharge.py` dengan ID `etl_flood_discharge_mysql`.

### Diagram Alur Dependensi Task:

```text
[create_tables]
       │
       ▼
[load_locations]
       │
       ├─────────────────────────┐
       ▼                         ▼
[load_river_discharge]    [load_rainfall]
       │                         │
       └────────────┬────────────┘
                    ▼
          [train_and_predict]
                    │
                    ▼
                [verify]
```

### Perincian Tugas:
- **`create_tables`**: Memeriksa dan membuat tabel DDL jika belum tersedia. Tabel induk dibuat terlebih dahulu agar constraint foreign key valid.
- **`load_locations`**: Mengekstraksi koordinat geografis dari Open-Meteo Geocoding API dan melakukan upsert idempoten (`ON DUPLICATE KEY UPDATE`).
- **`load_river_discharge` & `load_rainfall`**: Berjalan secara paralel (*task branching/parallel execution*). Mengekstraksi data historis 365 hari ke belakang dari Open-Meteo Flood API dan NASA POWER API, membersihkan nilai null/missing (-999), lalu memuat data ke MySQL.
- **`train_and_predict`**: Melakukan penggabungan data secara relasional, rekayasa fitur lag time-series, pelatihan model Machine Learning, kalkulasi metrik evaluasi (MAE & RMSE), dan ekspor hasil inferensi ke `include/data/flood_predictions.csv`.
- **`verify`**: Menghitung validitas baris data, memastikan keterhubungan antar tabel via JOIN, dan memverifikasi integritas relasi.

---

## 6. Pemodelan Machine Learning

1. **Rekayasa Fitur (*Feature Engineering*)**:
   - `discharge`: Debit sungai pada hari observasi acuan.
   - `q_lag1`, `q_lag2`, `q_lag3`: Debit sungai 1, 2, dan 3 hari sebelumnya.
   - `q_diff1`: Laju perubahan debit harian (`discharge - q_lag1`).
   - `rain_sum1`: Curah hujan hari berjalan.
   - `rain_sum3`: Akumulasi curah hujan bergerak 3 hari terakhir (*rolling sum 3 days*).
   - `rain_sum7`: Akumulasi curah hujan bergerak 7 hari terakhir (*rolling sum 7 days*).
   - One-hot encoding untuk representasi kode lokasi.

2. **Pembagian Data (*Data Splitting*)**:
   - Berbasis urutan waktu (80% data awal untuk pelatihan, 20% data akhir untuk pengujian) guna menghindari kebocoran data masa depan (*data leakage*).

3. **Algoritma dan Evaluasi**:
   - Model: Random Forest Regressor & Gradient Boosting Regressor.
   - Metrik Pengujian:
     - Mean Absolute Error (MAE): 1.85 m3/s
     - Root Mean Squared Error (RMSE): 7.22 m3/s
     - Koefisien Determinasi (R2): 0.92

4. **Kriteria Peringatan Dini Banjir**:
   - Ambang batas ditetapkan pada persentil ke-90 (P90) debit historis per lokasi.
   - Jika nilai estimasi debit besok melebihi nilai P90 lokasi tersebut, sistem secara otomatis mengaktifkan status `flood_warning = True`.

---

## 7. Panduan Menjalankan Proyek Secara Lokal

### Prasyarat:
- Docker Desktop aktif
- Astro CLI terpasang

### Langkah Instalasi:

1. **Klon Repositori**:
   ```bash
   git clone https://github.com/dirgad58-commits/etl-debit-sungai-airflow-E1E124007.git
   cd etl-debit-sungai-airflow-E1E124007
   ```

2. **Jalankan Layanan Airflow**:
   ```bash
   astro dev start
   ```
   Perintah ini akan menjalankan container Webserver, Scheduler, Triggerer, PostgreSQL Metadata, dan MySQL 8.0.

3. **Buka Antarmuka Airflow**:
   Akses `http://localhost:8080` pada peramban web (Kredensial default: username `admin`, password `admin`).

4. **Konfigurasi Koneksi MySQL (`mysql-local`)**:
   Buka menu **Admin** -> **Connections** -> **Add Connection**:
   - Connection Id: `mysql-local`
   - Connection Type: `MySQL`
   - Host: `host.docker.internal`
   - Schema: `banjir_db`
   - Login: `root`
   - Password: `root`
   - Port: `3306`

5. **Jalankan Pipeline**:
   - Cari DAG dengan nama `etl_flood_discharge_mysql`.
   - Aktifkan toggle switch (unpause), kemudian klik **Trigger DAG**.
   - Pantau visualisasi eksekusi pada tab **Graph View** dan log masing-masing task.

---

## 8. Verifikasi Data pada Basis Data MySQL

Setelah DAG selesai dieksekusi, verifikasi integritas data dapat dilakukan melalui query yang tersedia di `database/query_verifikasi.sql`:

```sql
-- 1. Verifikasi jumlah baris tiap tabel
SELECT 'locations' AS tabel, COUNT(*) AS jumlah FROM locations
UNION ALL SELECT 'river_discharge', COUNT(*) FROM river_discharge
UNION ALL SELECT 'rainfall_daily', COUNT(*) FROM rainfall_daily;

-- 2. Uji relasi JOIN ketiga tabel
SELECT l.city_name, l.river_name, d.obs_date,
       d.discharge_m3s AS debit_m3s, r.rain_mm AS hujan_mm
FROM locations l
JOIN river_discharge d ON d.location_id = l.location_id
JOIN rainfall_daily r ON r.location_id = d.location_id AND r.obs_date = d.obs_date
ORDER BY d.obs_date DESC
LIMIT 10;
```

---

## 9. Lisensi dan Penggunaan

Repositori ini disusun sebagai bagian dari penyelesaian tugas praktikum Data Engineering. Seluruh kode, skema, dan model dapat digunakan dan dikembangkan untuk keperluan akademis serta penelitian mitigasi bencana hidrologi.
