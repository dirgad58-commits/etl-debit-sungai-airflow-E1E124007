-- ============================================================================
-- QUERY VERIFIKASI BASIS DATA DAN RELASI PIPELINE ETL DEBIT SUNGAI
-- ============================================================================

-- 1. Jumlah baris pada setiap tabel
SELECT 'locations' AS nama_tabel, COUNT(*) AS total_baris FROM locations
UNION ALL
SELECT 'river_discharge', COUNT(*) FROM river_discharge
UNION ALL
SELECT 'rainfall_daily', COUNT(*) FROM rainfall_daily;

-- 2. Bukti relasi: JOIN ketiga tabel (locations, river_discharge, rainfall_daily)
SELECT 
    l.location_id,
    l.city_name,
    l.river_name,
    d.obs_date,
    d.discharge_m3s AS debit_sungai_m3s,
    r.rain_mm AS curah_hujan_mm
FROM locations l
JOIN river_discharge d ON d.location_id = l.location_id
JOIN rainfall_daily r ON r.location_id = d.location_id AND r.obs_date = d.obs_date
ORDER BY d.obs_date DESC, l.city_name ASC
LIMIT 20;

-- 3. Agregasi statistik per kota (jumlah observasi, rata-rata debit, rata-rata curah hujan)
SELECT 
    l.location_id,
    l.city_name,
    l.province,
    COUNT(d.obs_date) AS jumlah_hari,
    ROUND(AVG(d.discharge_m3s), 2) AS rata_rata_debit,
    ROUND(MAX(d.discharge_m3s), 2) AS debit_maksimum,
    ROUND(AVG(r.rain_mm), 2) AS rata_rata_hujan,
    ROUND(MAX(r.rain_mm), 2) AS hujan_maksimum
FROM locations l
JOIN river_discharge d ON d.location_id = l.location_id
JOIN rainfall_daily r ON r.location_id = d.location_id AND r.obs_date = d.obs_date
GROUP BY l.location_id, l.city_name, l.province
ORDER BY rata_rata_debit DESC;

-- 4. Uji integritas referensial (Foreign Key Constraint)
-- Pernyataan berikut sengaja menggunakan location_id 'INVALID' yang tidak ada di tabel induk locations.
-- Eksekusi harus menghasilkan error: Cannot add or update a child row: a foreign key constraint fails.
-- INSERT INTO river_discharge (location_id, obs_date, discharge_m3s) VALUES ('INVALID', '2026-10-10', 15.5);
