-- ============================================================================
-- SKEMA BASIS DATA MYSQL: banjir_db
-- Pipeline ETL Apache Airflow & Prediksi Debit Sungai (SDG 13 & SDG 11)
-- ============================================================================

CREATE DATABASE IF NOT EXISTS banjir_db
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE banjir_db;

-- Tabel Induk: Profil Lokasi Pemantauan (locations)
CREATE TABLE IF NOT EXISTS locations (
    location_id  VARCHAR(10) PRIMARY KEY,
    city_name    VARCHAR(100) NOT NULL,
    river_name   VARCHAR(100),
    province     VARCHAR(100),
    latitude     DECIMAL(9,6) NOT NULL,
    longitude    DECIMAL(9,6) NOT NULL,
    elevation_m  DECIMAL(8,2),
    population   BIGINT
) ENGINE=InnoDB;

-- Tabel Anak: Debit Sungai Harian (river_discharge)
CREATE TABLE IF NOT EXISTS river_discharge (
    location_id    VARCHAR(10) NOT NULL,
    obs_date       DATE NOT NULL,
    discharge_m3s  DECIMAL(14,3) NOT NULL,
    PRIMARY KEY (location_id, obs_date),
    CONSTRAINT fk_discharge_location
        FOREIGN KEY (location_id) REFERENCES locations(location_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

-- Tabel Anak: Curah Hujan Harian (rainfall_daily)
CREATE TABLE IF NOT EXISTS rainfall_daily (
    location_id  VARCHAR(10) NOT NULL,
    obs_date     DATE NOT NULL,
    rain_mm      DECIMAL(8,2) NOT NULL,
    PRIMARY KEY (location_id, obs_date),
    CONSTRAINT fk_rain_location
        FOREIGN KEY (location_id) REFERENCES locations(location_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

-- Tabel Log Prediksi dan Peringatan Banjir
CREATE TABLE IF NOT EXISTS prediksi_log (
    id                          INT AUTO_INCREMENT PRIMARY KEY,
    location_id                 VARCHAR(10) NOT NULL,
    obs_date                    DATE NOT NULL,
    forecast_for                DATE NOT NULL,
    predicted_discharge_next_day DECIMAL(14,3) NOT NULL,
    threshold_p90               DECIMAL(14,3) NOT NULL,
    flood_warning               BOOLEAN NOT NULL,
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_prediksi_location
        FOREIGN KEY (location_id) REFERENCES locations(location_id)
) ENGINE=InnoDB;
