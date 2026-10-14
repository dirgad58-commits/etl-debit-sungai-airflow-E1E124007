-- ============================================================================
-- PANDUAN QUERY SQL UNTUK DEMO DAN UJIAN PRAKTIKUM DI DEPAN DOSEN
-- Kasus Contoh: Kota Kendari (Kode: KDI, Sungai: Wanggu, Sulawesi Tenggara)
-- ============================================================================

USE banjir_db;

-- ----------------------------------------------------------------------------
-- 1. MENAMPILKAN PROFIL SUATU KOTA TERTENTU
-- Pertanyaan Dosen: "Coba tunjukkan profil kota Kendari di database!"
-- ----------------------------------------------------------------------------
SELECT 
    location_id AS kode_kota,
    city_name   AS nama_kota,
    river_name  AS nama_sungai,
    province    AS provinsi,
    latitude    AS garis_lintang,
    longitude   AS garis_bujur,
    elevation_m AS elevasi_meter,
    population  AS jumlah_penduduk
FROM locations
WHERE location_id = 'KDI';


-- ----------------------------------------------------------------------------
-- 2. MENAMPILKAN 10 DATA DEBIT SUNGAI TERAKHIR DARI KOTA TERSEBUT
-- Pertanyaan Dosen: "Coba tunjukkan catatan debit air sungai Wanggu beberapa hari terakhir!"
-- ----------------------------------------------------------------------------
SELECT 
    obs_date      AS tanggal_observasi,
    discharge_m3s AS debit_sungai_m3s
FROM river_discharge
WHERE location_id = 'KDI'
ORDER BY obs_date DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- 3. MENAMPILKAN 10 DATA CURAH HUJAN TERAKHIR DARI KOTA TERSEBUT
-- Pertanyaan Dosen: "Bagaimana dengan catatan curah hujan di kota itu?"
-- ----------------------------------------------------------------------------
SELECT 
    obs_date AS tanggal_observasi,
    rain_mm  AS curah_hujan_mm
FROM rainfall_daily
WHERE location_id = 'KDI'
ORDER BY obs_date DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- 4. MENGGABUNGKAN (JOIN) PROFIL KOTA, DEBIT SUNGAI, DAN CURAH HUJAN
-- Pertanyaan Dosen (FAVORIT): "Tunjukkan bagaimana kamu menggabungkan data debit dan hujan kota itu dalam 1 tabel!"
-- Penjelasan: Menggunakan INNER JOIN antara tabel induk locations dan 2 tabel anak (river_discharge & rainfall_daily).
-- ----------------------------------------------------------------------------
SELECT 
    l.city_name     AS kota,
    l.river_name    AS sungai,
    d.obs_date      AS tanggal,
    r.rain_mm       AS curah_hujan_mm,
    d.discharge_m3s AS debit_air_m3s
FROM locations l
JOIN river_discharge d 
    ON l.location_id = d.location_id
JOIN rainfall_daily r 
    ON d.location_id = r.location_id 
    AND d.obs_date   = r.obs_date
WHERE l.location_id = 'KDI'
ORDER BY d.obs_date DESC
LIMIT 15;


-- ----------------------------------------------------------------------------
-- 5. FILTER: MELIHAT DEBIT SAAT CURAH HUJAN TINGGI / EKSTREM
-- Pertanyaan Dosen: "Coba filter hanya hari-hari saat hujan lebat (> 20 mm), apakah debitnya naik?"
-- ----------------------------------------------------------------------------
SELECT 
    l.city_name     AS kota,
    d.obs_date      AS tanggal,
    r.rain_mm       AS hujan_lebat_mm,
    d.discharge_m3s AS debit_m3s
FROM locations l
JOIN river_discharge d 
    ON l.location_id = d.location_id
JOIN rainfall_daily r 
    ON d.location_id = r.location_id 
    AND d.obs_date   = r.obs_date
WHERE l.location_id = 'KDI' 
  AND r.rain_mm > 20.0
ORDER BY r.rain_mm DESC;


-- ----------------------------------------------------------------------------
-- 6. STATISTIK AGREGASI (MIN, MAX, AVG) SATU KOTA
-- Pertanyaan Dosen: "Berapa rata-rata debit air dan curah hujan tertinggi di sungai Wanggu?"
-- ----------------------------------------------------------------------------
SELECT 
    l.city_name                    AS kota,
    l.river_name                   AS sungai,
    COUNT(*)                       AS total_hari_observasi,
    ROUND(AVG(d.discharge_m3s), 2) AS debit_rata_rata,
    ROUND(MAX(d.discharge_m3s), 2) AS debit_puncak_tertinggi,
    ROUND(MIN(d.discharge_m3s), 2) AS debit_terendah,
    ROUND(AVG(r.rain_mm), 2)       AS hujan_rata_rata,
    ROUND(MAX(r.rain_mm), 2)       AS hujan_tertinggi
FROM locations l
JOIN river_discharge d 
    ON l.location_id = d.location_id
JOIN rainfall_daily r 
    ON d.location_id = r.location_id 
    AND d.obs_date   = r.obs_date
WHERE l.location_id = 'KDI'
GROUP BY l.city_name, l.river_name;


-- ----------------------------------------------------------------------------
-- 7. PEMBUKTIAN BAHWA FOREIGN KEY BERFUNGSI (PENTING!)
-- Pertanyaan Dosen: "Buktikan ke saya kalau Foreign Key di database ini benar-benar aktif!"
-- Penjelasan Jawaban: "Jika saya mencoba memasukkan data ke tabel anak river_discharge dengan
-- kode lokasi 'XXX' yang tidak pernah terdaftar di tabel induk locations, MySQL otomatis
-- akan menolaknya dengan error: Cannot add or update a child row: a foreign key constraint fails."
-- ----------------------------------------------------------------------------
-- Jalankan baris berikut di depan dosen:
INSERT INTO river_discharge (location_id, obs_date, discharge_m3s)
VALUES ('XXX', '2026-10-14', 99.9);
-- HASIL YANG MUNCUL: Error Code: 1452. Cannot add or update a child row: a foreign key constraint fails...


-- ----------------------------------------------------------------------------
-- 8. MEMBANDINGKAN KENDARI DENGAN KOTA LAIN (PERINGKAT DEBIT NASIONAL)
-- Pertanyaan Dosen: "Bagaimana debit sungai Kendari dibanding kota-kota lain?"
-- ----------------------------------------------------------------------------
SELECT 
    l.location_id,
    l.city_name,
    l.river_name,
    l.province,
    ROUND(AVG(d.discharge_m3s), 2) AS rata_rata_debit,
    ROUND(MAX(d.discharge_m3s), 2) AS debit_maksimum
FROM locations l
JOIN river_discharge d ON l.location_id = d.location_id
GROUP BY l.location_id, l.city_name, l.river_name, l.province
ORDER BY rata_rata_debit DESC;
