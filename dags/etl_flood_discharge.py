"""
DAG: etl_flood_discharge_mysql
Topik: Pipeline ETL debit sungai dan curah hujan untuk peringatan dini banjir (SDG 13 dan SDG 11)

Sumber data (3 API):
  1. Open-Meteo Geocoding API -> tabel locations
  2. Open-Meteo Flood API     -> tabel river_discharge
  3. NASA POWER API           -> tabel rainfall_daily

Alur orkestrasi:
  create_tables >> load_locations >> [load_river_discharge, load_rainfall]
                >> train_and_predict >> verify
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import requests

try:
    from airflow.sdk import BaseHook, dag, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

# Konfigurasi koneksi dan direktori
MYSQL_CONN_ID = "mysql-local"
HISTORY_DAYS = 365
NASA_LAG_DAYS = 3
OUTPUT_DIR = "/usr/local/airflow/include/data"

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FLOOD_URL = "https://flood-api.open-meteo.com/v1/flood"
NASA_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

MAX_HINT_DEG = 0.6

# Daftar 24 lokasi rawan banjir di Indonesia
LOCATIONS = [
    # Sumatra
    {"code": "BNA", "query": "Banda Aceh",   "river": "Krueng Aceh",        "lat": 5.55,  "lon": 95.32},
    {"code": "MDN", "query": "Medan",        "river": "Deli",               "lat": 3.59,  "lon": 98.67},
    {"code": "PDG", "query": "Padang",       "river": "Batang Arau",        "lat": -0.95, "lon": 100.35},
    {"code": "PKU", "query": "Pekanbaru",    "river": "Siak",               "lat": 0.51,  "lon": 101.45},
    {"code": "JMB", "query": "Jambi",        "river": "Batanghari",         "lat": -1.61, "lon": 103.61},
    {"code": "PLG", "query": "Palembang",    "river": "Musi",               "lat": -2.98, "lon": 104.75},
    # Jawa
    {"code": "JKT", "query": "Jakarta",      "river": "Ciliwung",           "lat": -6.21, "lon": 106.85},
    {"code": "BKS", "query": "Bekasi",       "river": "Cikeas",             "lat": -6.24, "lon": 106.99},
    {"code": "BDG", "query": "Bandung",      "river": "Citarum",            "lat": -6.91, "lon": 107.61},
    {"code": "SMG", "query": "Semarang",     "river": "Banjir Kanal Barat", "lat": -6.97, "lon": 110.42},
    {"code": "SLO", "query": "Surakarta",    "river": "Bengawan Solo",      "lat": -7.57, "lon": 110.82},
    {"code": "YOG", "query": "Yogyakarta",   "river": "Code",               "lat": -7.80, "lon": 110.36},
    {"code": "SBY", "query": "Surabaya",     "river": "Brantas",            "lat": -7.25, "lon": 112.75},
    # Kalimantan
    {"code": "PTK", "query": "Pontianak",    "river": "Kapuas",             "lat": -0.03, "lon": 109.33},
    {"code": "PLK", "query": "Palangkaraya", "river": "Kahayan",            "lat": -2.21, "lon": 113.92},
    {"code": "BJM", "query": "Banjarmasin",  "river": "Martapura",          "lat": -3.32, "lon": 114.59},
    {"code": "SMD", "query": "Samarinda",    "river": "Mahakam",            "lat": -0.50, "lon": 117.15},
    # Sulawesi
    {"code": "MKS", "query": "Makassar",     "river": "Jeneberang",         "lat": -5.15, "lon": 119.43},
    {"code": "PAL", "query": "Palu",         "river": "Palu",               "lat": -0.90, "lon": 119.87},
    {"code": "KDI", "query": "Kendari",      "river": "Wanggu",             "lat": -3.97, "lon": 122.51},
    {"code": "MDO", "query": "Manado",       "river": "Tondano",            "lat": 1.47,  "lon": 124.84},
    # Bali dan Nusa Tenggara
    {"code": "DPS", "query": "Denpasar",     "river": "Badung",             "lat": -8.65, "lon": 115.22},
    {"code": "MTR", "query": "Mataram",      "river": "Jangkok",            "lat": -8.58, "lon": 116.12},
    # Papua
    {"code": "MRK", "query": "Merauke",      "river": "Maro",               "lat": -8.49, "lon": 140.40},
]


def get_conn():
    """Buka koneksi MySQL dari Airflow Connection ID."""
    import mysql.connector
    c = BaseHook.get_connection(MYSQL_CONN_ID)
    return mysql.connector.connect(
        host=c.host,
        port=c.port or 3306,
        user=c.login,
        password=c.password,
        database=c.schema,
    )


def get_json(url: str, params: dict, retries: int = 3, timeout: int = 60) -> dict:
    """Melakukan HTTP GET JSON dengan mekanisme coba ulang."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as err:
            last_err = err
            log.warning("Percobaan %s/%s gagal untuk %s: %s", attempt, retries, url, err)
            time.sleep(3 * attempt)
    raise RuntimeError(f"Gagal mengambil data dari {url}: {last_err}")


def fetch_locations() -> list[tuple]:
    """Mengambil id lokasi dan koordinat dari tabel locations."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT location_id, latitude, longitude FROM locations ORDER BY location_id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        raise RuntimeError("Tabel locations kosong. Jalankan task load_locations terlebih dahulu.")
    return rows


def pick_result(results: list[dict], hint_lat: float, hint_lon: float):
    """Memilih hasil geocoding terdekat dari koordinat acuan."""
    best, best_d = None, None
    for r in results:
        d = ((r["latitude"] - hint_lat) ** 2 + (r["longitude"] - hint_lon) ** 2) ** 0.5
        if best is None or d < best_d:
            best, best_d = r, d
    return best, best_d


def upsert_many(sql: str, rows: list[tuple], batch: int = 1000) -> int:
    """Melakukan insert bertahap secara idempoten dan commit."""
    if not rows:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    for i in range(0, len(rows), batch):
        cur.executemany(sql, rows[i:i + batch])
    conn.commit()
    cur.close()
    conn.close()
    return len(rows)


@dag(
    dag_id="etl_flood_discharge_mysql",
    description="Pipeline ETL debit sungai dan curah hujan ke MySQL serta prediksi risiko banjir",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "praktikum", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "flood", "sdg13", "sdg11", "mysql"],
)
def etl_flood_discharge_mysql():

    @task
    def create_tables():
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS locations (
                location_id  VARCHAR(10) PRIMARY KEY,
                city_name    VARCHAR(100) NOT NULL,
                river_name   VARCHAR(100),
                province     VARCHAR(100),
                latitude     DECIMAL(9,6) NOT NULL,
                longitude    DECIMAL(9,6) NOT NULL,
                elevation_m  DECIMAL(8,2),
                population   BIGINT
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS river_discharge (
                location_id    VARCHAR(10) NOT NULL,
                obs_date       DATE NOT NULL,
                discharge_m3s  DECIMAL(14,3) NOT NULL,
                PRIMARY KEY (location_id, obs_date),
                CONSTRAINT fk_discharge_location
                    FOREIGN KEY (location_id) REFERENCES locations(location_id)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS rainfall_daily (
                location_id  VARCHAR(10) NOT NULL,
                obs_date     DATE NOT NULL,
                rain_mm      DECIMAL(8,2) NOT NULL,
                PRIMARY KEY (location_id, obs_date),
                CONSTRAINT fk_rain_location
                    FOREIGN KEY (location_id) REFERENCES locations(location_id)
            ) ENGINE=InnoDB
            """,
        ]
        conn = get_conn()
        cur = conn.cursor()
        for stmt in ddl:
            cur.execute(stmt)
        conn.commit()
        cur.close()
        conn.close()
        log.info("Skema tabel locations, river_discharge, dan rainfall_daily berhasil diverifikasi.")

    @task
    def load_locations():
        rows = []
        for loc in LOCATIONS:
            try:
                data = get_json(
                    GEOCODING_URL,
                    {"name": loc["query"], "count": 10, "language": "id",
                     "format": "json", "countryCode": "ID"},
                )
            except RuntimeError as err:
                log.error("Geocoding gagal untuk %s: %s", loc["query"], err)
                data = {}
            results = data.get("results") or []
            r, dist = pick_result(results, loc["lat"], loc["lon"]) if results else (None, None)

            if r is not None and dist <= MAX_HINT_DEG:
                rows.append((
                    loc["code"], r.get("name", loc["query"]), loc["river"], r.get("admin1"),
                    r["latitude"], r["longitude"], r.get("elevation"), r.get("population"),
                ))
            else:
                log.warning("Geocoding %s memakai koordinat acuan (jarak=%s).",
                            loc["query"], None if dist is None else round(dist, 2))
                rows.append((loc["code"], loc["query"], loc["river"], None,
                             loc["lat"], loc["lon"], None, None))
            time.sleep(0.3)

        sql = """
            INSERT INTO locations
                (location_id, city_name, river_name, province,
                 latitude, longitude, elevation_m, population)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                city_name = VALUES(city_name), river_name = VALUES(river_name),
                province = VALUES(province), latitude = VALUES(latitude),
                longitude = VALUES(longitude), elevation_m = VALUES(elevation_m),
                population = VALUES(population)
        """
        n = upsert_many(sql, rows)
        log.info("%s lokasi berhasil disimpan ke tabel locations.", n)

    @task
    def load_river_discharge():
        end = date.today()
        start = end - timedelta(days=HISTORY_DAYS)
        sql = """
            INSERT INTO river_discharge (location_id, obs_date, discharge_m3s)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE discharge_m3s = VALUES(discharge_m3s)
        """
        total, kosong, gagal = 0, [], []
        for loc_id, lat, lon in fetch_locations():
            try:
                data = get_json(
                    FLOOD_URL,
                    {"latitude": float(lat), "longitude": float(lon),
                     "daily": "river_discharge",
                     "start_date": start.isoformat(), "end_date": end.isoformat()},
                )
                daily = data.get("daily", {})
                df = pd.DataFrame({
                    "obs_date": daily.get("time", []),
                    "discharge": daily.get("river_discharge", []),
                }).dropna()
                if df.empty:
                    kosong.append(loc_id)
                    log.warning("Data debit kosong untuk lokasi %s.", loc_id)
                    continue
                df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
                rows = [(loc_id, d, float(q)) for d, q in zip(df["obs_date"], df["discharge"])]
                total += upsert_many(sql, rows)
            except Exception as err:
                gagal.append(loc_id)
                log.error("Gagal memuat data debit untuk %s: %s", loc_id, err)
            time.sleep(1)
        log.info("Total %s baris debit dimuat. Kosong: %s | Gagal: %s", total, kosong, gagal)
        if total == 0:
            raise RuntimeError("Tidak ada data debit yang berhasil dimuat.")

    @task
    def load_rainfall():
        end = date.today() - timedelta(days=NASA_LAG_DAYS)
        start = end - timedelta(days=HISTORY_DAYS)
        sql = """
            INSERT INTO rainfall_daily (location_id, obs_date, rain_mm)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE rain_mm = VALUES(rain_mm)
        """
        total, gagal = 0, []
        for loc_id, lat, lon in fetch_locations():
            try:
                data = get_json(
                    NASA_URL,
                    {"parameters": "PRECTOTCORR", "community": "AG",
                     "latitude": float(lat), "longitude": float(lon),
                     "start": start.strftime("%Y%m%d"), "end": end.strftime("%Y%m%d"),
                     "format": "JSON"},
                    timeout=120,
                )
                values = data["properties"]["parameter"]["PRECTOTCORR"]
                df = pd.DataFrame(list(values.items()), columns=["tgl", "rain"])
                df = df[df["rain"] >= 0]
                df["obs_date"] = pd.to_datetime(df["tgl"], format="%Y%m%d").dt.date
                rows = [(loc_id, d, float(r)) for d, r in zip(df["obs_date"], df["rain"])]
                total += upsert_many(sql, rows)
            except Exception as err:
                gagal.append(loc_id)
                log.error("Gagal memuat data hujan untuk %s: %s", loc_id, err)
            time.sleep(1)
        log.info("Total %s baris curah hujan dimuat. Gagal: %s", total, gagal)
        if total == 0:
            raise RuntimeError("Tidak ada data curah hujan yang berhasil dimuat.")

    @task
    def train_and_predict():
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_absolute_error, mean_squared_error

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT d.location_id, d.obs_date, d.discharge_m3s, r.rain_mm
            FROM river_discharge d
            JOIN rainfall_daily r
              ON r.location_id = d.location_id AND r.obs_date = d.obs_date
            JOIN locations l ON l.location_id = d.location_id
            ORDER BY d.location_id, d.obs_date
        """)
        df = pd.DataFrame(cur.fetchall(),
                          columns=["location_id", "obs_date", "discharge", "rain"])
        cur.close()
        conn.close()

        df["discharge"] = df["discharge"].astype(float)
        df["rain"] = df["rain"].astype(float)
        df["obs_date"] = pd.to_datetime(df["obs_date"])
        if len(df) < 100:
            raise RuntimeError(f"Data tidak memadai untuk pelatihan: {len(df)} baris.")

        # Pembentukan fitur berbasis urutan waktu
        g = df.groupby("location_id")
        for k in (1, 2, 3):
            df[f"q_lag{k}"] = g["discharge"].shift(k)
        df["q_diff1"] = df["discharge"] - df["q_lag1"]
        df["rain_sum1"] = df["rain"]
        df["rain_sum3"] = g["rain"].transform(lambda s: s.rolling(3).sum())
        df["rain_sum7"] = g["rain"].transform(lambda s: s.rolling(7).sum())
        df["target"] = g["discharge"].shift(-1)

        feat = ["discharge", "q_lag1", "q_lag2", "q_lag3", "q_diff1",
                "rain_sum1", "rain_sum3", "rain_sum7"]
        df = pd.concat([df, pd.get_dummies(df["location_id"], prefix="loc", dtype=float)], axis=1)
        feat += [c for c in df.columns if c.startswith("loc_")]

        usable = df.dropna(subset=feat)
        labeled = usable.dropna(subset=["target"])
        latest = usable[usable["target"].isna()]

        # Pemisahan data latih dan data uji berbasis waktu
        cutoff = labeled["obs_date"].quantile(0.8)
        train = labeled[labeled["obs_date"] <= cutoff]
        test = labeled[labeled["obs_date"] > cutoff]

        model = RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)
        model.fit(train[feat], train["target"])

        pred_test = model.predict(test[feat])
        mae = mean_absolute_error(test["target"], pred_test)
        rmse = float(np.sqrt(mean_squared_error(test["target"], pred_test)))
        log.info("Evaluasi model | Data latih=%s Data uji=%s | MAE=%.3f | RMSE=%.3f",
                 len(train), len(test), mae, rmse)

        threshold = train.groupby("location_id")["discharge"].quantile(0.9)

        out = latest[["location_id", "obs_date"]].copy()
        out["predicted_discharge_next_day"] = model.predict(latest[feat])
        out["threshold_p90"] = out["location_id"].map(threshold)
        out["flood_warning"] = out["predicted_discharge_next_day"] > out["threshold_p90"]
        out["forecast_for"] = out["obs_date"] + pd.Timedelta(days=1)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        path = os.path.join(OUTPUT_DIR, "flood_predictions.csv")
        out.to_csv(path, index=False)
        log.info("Prediksi debit tersimpan di %s", path)

    @task
    def verify():
        conn = get_conn()
        cur = conn.cursor()
        counts = {}
        for t in ("locations", "river_discharge", "rainfall_daily"):
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            counts[t] = cur.fetchone()[0]
        log.info("Jumlah baris basis data: %s", counts)
        if any(v == 0 for v in counts.values()):
            raise RuntimeError(f"Terdapat tabel tanpa data: {counts}")

        cur.execute("""
            SELECT l.city_name, COUNT(*) AS hari_data,
                   ROUND(AVG(d.discharge_m3s), 2) AS rata_debit,
                   ROUND(AVG(r.rain_mm), 2)       AS rata_hujan
            FROM locations l
            JOIN river_discharge d ON d.location_id = l.location_id
            JOIN rainfall_daily  r ON r.location_id = d.location_id
                                  AND r.obs_date    = d.obs_date
            GROUP BY l.city_name
        """)
        for row in cur.fetchall():
            log.info("Validasi relasi JOIN: %s", row)

        cur.execute("""
            SELECT l.location_id, l.city_name,
                   (SELECT COUNT(*) FROM river_discharge d WHERE d.location_id = l.location_id) AS n_debit,
                   (SELECT COUNT(*) FROM rainfall_daily  r WHERE r.location_id = l.location_id) AS n_hujan
            FROM locations l
            ORDER BY l.location_id
        """)
        lemah = [row for row in cur.fetchall() if row[2] == 0 or row[3] == 0]
        if lemah:
            log.warning("Lokasi tanpa data lengkap: %s", lemah)
        else:
            log.info("Semua lokasi tervalidasi memiliki data debit dan hujan.")
        cur.close()
        conn.close()

    t_create = create_tables()
    t_loc = load_locations()
    t_dis = load_river_discharge()
    t_rain = load_rainfall()
    t_ml = train_and_predict()
    t_ver = verify()

    t_create >> t_loc >> [t_dis, t_rain] >> t_ml >> t_ver


etl_flood_discharge_mysql()
