#!/usr/bin/env python3
# -*- coding: utf-8 -*-



from __future__ import annotations
import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import pymysql

# -----------------------------
# MySQL connection
# -----------------------------
MYSQL_HOST = "192.140.56.40"
MYSQL_USER = "padrocl1_gs"
MYSQL_PASSWORD = "]I@kyc4sgwEqk}hH"
MYSQL_DB = "padrocl1_bd"
MYSQL_PORT = 3306

FILENAME_RE = re.compile(r"^taxi_trips_(\d{4}-\d{2}-\d{2})\.jsonl$")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso_like_dt(value: Any) -> Optional[datetime]:
    if value is None: return None
    s = str(value).strip()
    if not s: return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def safe_float(v): 
    try: return float(v) if v is not None else 0.0
    except: return 0.0

def safe_int(v): 
    try: return int(float(v)) if v is not None else 0
    except: return 0

@dataclass
class Settings:
    output_dir: Path
    update_window_days: int

def read_settings_from_config(cfg: Dict[str, Any]) -> Settings:
    s = cfg.get("settings", {})
    out_dir = Path(s.get("output_dir", "./data/raw")).resolve()
    update_window_days = int(s.get("update_window_days", 1))
    return Settings(output_dir=out_dir, update_window_days=max(1, update_window_days))

def list_available_days(output_dir: Path) -> List[date]:
    days: List[date] = []
    if not output_dir.exists(): return days
    for p in output_dir.iterdir():
        m = FILENAME_RE.match(p.name)
        if m:
            days.append(datetime.strptime(m.group(1), "%Y-%m-%d").date())
    return sorted(set(days))

def choose_days_to_upload(available_days: List[date], rolling_days: int) -> List[date]:
    if not available_days: return []
    anchor = max(available_days)
    wanted = {anchor - timedelta(days=i) for i in range(rolling_days)}
    return [d for d in available_days if d in wanted]

def mysql_connect():
    return pymysql.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB, port=MYSQL_PORT, charset="utf8mb4",
        autocommit=False, cursorclass=pymysql.cursors.Cursor,
    )

def ensure_table_exists(conn):
    """Crea la tabla o añade columnas faltantes verificando su existencia previa."""
    ddl_base = """
    CREATE TABLE IF NOT EXISTS taxi_trips_raw (
      trip_day DATE NOT NULL,
      trip_id VARCHAR(128) NOT NULL,
      payload JSON NOT NULL,
      ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (trip_day, trip_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    
    new_columns = [
        ("taxi_id", "VARCHAR(255) AFTER trip_id"),
        ("trip_start_timestamp", "DATETIME AFTER taxi_id"),
        ("trip_end_timestamp", "DATETIME AFTER trip_start_timestamp"),
        ("trip_seconds", "INT AFTER trip_end_timestamp"),
        ("trip_miles", "DECIMAL(10,2) AFTER trip_seconds"),
        ("pickup_community_area", "INT AFTER trip_miles"),
        ("dropoff_community_area", "INT AFTER pickup_community_area"),
        ("fare", "DECIMAL(10,2) AFTER dropoff_community_area"),
        ("tips", "DECIMAL(10,2) AFTER fare"),
        ("tolls", "DECIMAL(10,2) AFTER tips"),
        ("extras", "DECIMAL(10,2) AFTER tolls"),
        ("trip_total", "DECIMAL(10,2) AFTER extras"),
        ("payment_type", "VARCHAR(50) AFTER trip_total"),
        ("company", "VARCHAR(100) AFTER payment_type"),
        ("pickup_latitude", "DOUBLE AFTER company"),
        ("pickup_longitude", "DOUBLE AFTER pickup_latitude"),
        ("dropoff_latitude", "DOUBLE AFTER pickup_longitude"),
        ("dropoff_longitude", "DOUBLE AFTER dropoff_latitude")
    ]

    with conn.cursor() as cur:
        cur.execute(ddl_base)
        # Obtener columnas que ya existen para no intentar duplicarlas
        cur.execute("SHOW COLUMNS FROM taxi_trips_raw")
        existing_cols = {row[0] for row in cur.fetchall()}
        
        for col_name, col_def in new_columns:
            if col_name not in existing_cols:
                try:
                    cur.execute(f"ALTER TABLE taxi_trips_raw ADD COLUMN {col_name} {col_def}")
                except Exception as e:
                    print(f"[WARN] No se pudo agregar {col_name}: {e}")
        conn.commit()

def iter_jsonl_records(file_path: Path) -> Iterable[Tuple]:
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                data = json.loads(line)
                trip_id = data.get("trip_id", "hash_" + hashlib.md5(line.encode()).hexdigest()[:12])
                ts_start = parse_iso_like_dt(data.get("trip_start_timestamp"))
                ts_end = parse_iso_like_dt(data.get("trip_end_timestamp"))
                
                yield (
                    trip_id,
                    data.get("taxi_id"),
                    ts_start.strftime("%Y-%m-%d %H:%M:%S") if ts_start else None,
                    ts_end.strftime("%Y-%m-%d %H:%M:%S") if ts_end else None,
                    safe_int(data.get("trip_seconds")),
                    safe_float(data.get("trip_miles")),
                    safe_int(data.get("pickup_community_area")),
                    safe_int(data.get("dropoff_community_area")),
                    safe_float(data.get("fare")),
                    safe_float(data.get("tips")),
                    safe_float(data.get("tolls")),
                    safe_float(data.get("extras")),
                    safe_float(data.get("trip_total")),
                    data.get("payment_type"),
                    data.get("company"),
                    safe_float(data.get("pickup_centroid_latitude")),
                    safe_float(data.get("pickup_centroid_longitude")),
                    safe_float(data.get("dropoff_centroid_latitude")),
                    safe_float(data.get("dropoff_centroid_longitude")),
                    json.dumps(data, ensure_ascii=False)
                )
            except: continue

def batch_insert_day(conn, day: date, rows_iter: Iterable[Tuple], batch_size: int = 2000) -> int:
    sql = """
    INSERT INTO taxi_trips_raw (
        trip_day, trip_id, taxi_id, trip_start_timestamp, trip_end_timestamp,
        trip_seconds, trip_miles, pickup_community_area, dropoff_community_area,
        fare, tips, tolls, extras, trip_total, payment_type, company,
        pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude, payload
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        trip_total=VALUES(trip_total),
        payload=VALUES(payload);
    """
    day_str = day.strftime("%Y-%m-%d")
    inserted_total = 0
    batch = []
    with conn.cursor() as cur:
        cur.execute("DELETE FROM taxi_trips_raw WHERE trip_day=%s", (day_str,))
        for row in rows_iter:
            batch.append((day_str,) + row)
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                inserted_total += len(batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)
            inserted_total += len(batch)
    conn.commit()
    return inserted_total

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=str, default="etl_config.json")
    ap.add_argument("--days", type=int, default=None)
    ap.add_argument("--day", type=str, default=None)
    args = ap.parse_known_args()[0]

    config_path = Path(args.config).resolve()
    if not config_path.exists(): return
    with open(config_path, "r") as f: cfg = json.load(f)
    settings = read_settings_from_config(cfg)
    available_days = list_available_days(settings.output_dir)

    if args.day:
        d_obj = datetime.strptime(args.day, "%Y-%m-%d").date()
        days_to_upload = [d_obj] if d_obj in available_days else []
    else:
        rolling = args.days if args.days is not None else settings.update_window_days
        days_to_upload = choose_days_to_upload(available_days, rolling)

    conn = mysql_connect()
    try:
        ensure_table_exists(conn)
        for d in days_to_upload:
            file_path = settings.output_dir / f"taxi_trips_{d.strftime('%Y-%m-%d')}.jsonl"
            if file_path.exists():
                print(f"Subiendo {d}...")
                count = batch_insert_day(conn, d, iter_jsonl_records(file_path))
                print(f" [OK] {count} filas.")
    finally: conn.close()

if __name__ == "__main__":
    main()