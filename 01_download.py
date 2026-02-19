#!/usr/bin/env python
# coding: utf-8

# In[8]:



from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# -----------------------------
# Config model
# -----------------------------

@dataclass
class StaticSettings:
    domain: str = "data.cityofchicago.org"
    dataset_id: str = "ajtu-isnz"
    timestamp_field: str = "trip_start_timestamp"  # usado en el WHERE
    order_by: str = "trip_start_timestamp, trip_id"  # estable para paginar
    page_size: int = 50000
    timeout_s: int = 60
    max_retries: int = 6
    retry_base_s: float = 1.3
    output_dir: str = "./data/raw"

    # Primera corrida (carga inicial)
    backfill_days: int = 45

    # Corridas siguientes: ventana rolling a recalcular (re-escribir) por corrida
    update_window_days: int = 1

    # Opcional: mejora rate limits en Socrata
    app_token: Optional[str] = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def today_utc_date() -> date:
    return utc_now().date()


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def parse_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp.replace(path)


def load_or_init_config(config_path: Path) -> Dict[str, Any]:
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    settings = StaticSettings()
    cfg: Dict[str, Any] = {
        "settings": {
            "domain": settings.domain,
            "dataset_id": settings.dataset_id,
            "timestamp_field": settings.timestamp_field,
            "order_by": settings.order_by,
            "page_size": settings.page_size,
            "timeout_s": settings.timeout_s,
            "max_retries": settings.max_retries,
            "retry_base_s": settings.retry_base_s,
            "output_dir": settings.output_dir,
            "backfill_days": settings.backfill_days,
            "update_window_days": settings.update_window_days,
            "app_token": os.getenv("SOCRATA_APP_TOKEN", "") or None,
        },
        "state": {
            "initialized": False,
            "last_day_downloaded": None,  # "YYYY-MM-DD"
            "last_run_utc": None,
        },
        "log": {
            "runs": []
        },
    }
    atomic_write_json(config_path, cfg)
    return cfg


def get_settings(cfg: Dict[str, Any]) -> StaticSettings:
    s = cfg.get("settings", {})
    return StaticSettings(
        domain=s.get("domain", "data.cityofchicago.org"),
        dataset_id=s.get("dataset_id", "ajtu-isnz"),
        timestamp_field=s.get("timestamp_field", "trip_start_timestamp"),
        order_by=s.get("order_by", "trip_start_timestamp, trip_id"),
        page_size=int(s.get("page_size", 50000)),
        timeout_s=int(s.get("timeout_s", 60)),
        max_retries=int(s.get("max_retries", 6)),
        retry_base_s=float(s.get("retry_base_s", 1.3)),
        output_dir=s.get("output_dir", "./data/raw"),
        backfill_days=int(s.get("backfill_days", 45)),
        update_window_days=int(s.get("update_window_days", 1)),
        app_token=(s.get("app_token") or os.getenv("SOCRATA_APP_TOKEN") or None),
    )


# -----------------------------
# Socrata client
# -----------------------------

class SocrataClient:
    def __init__(self, settings: StaticSettings):
        self.settings = settings
        self.base_url = f"https://{settings.domain}/resource/{settings.dataset_id}.json"
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if settings.app_token:
            self.session.headers.update({"X-App-Token": settings.app_token})

    def fetch_page(self, where: str, limit: int, offset: int, order_by: str) -> List[Dict[str, Any]]:
        params = {
            "$where": where,
            "$order": order_by,
            "$limit": str(limit),
            "$offset": str(offset),
        }

        last_err: Optional[Exception] = None
        for attempt in range(self.settings.max_retries):
            try:
                r = self.session.get(self.base_url, params=params, timeout=self.settings.timeout_s)

                if r.status_code == 429 or 500 <= r.status_code <= 599:
                    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:250]}")

                r.raise_for_status()
                data = r.json()
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected JSON type: {type(data)}")
                return data

            except Exception as e:
                last_err = e
                sleep_s = self.settings.retry_base_s ** attempt
                print(
                    f"[WARN] fetch failed (attempt {attempt+1}/{self.settings.max_retries}): {e} -> sleep {sleep_s:.2f}s",
                    file=sys.stderr,
                )
                time.sleep(sleep_s)

        raise RuntimeError(f"Failed after retries. Last error: {last_err}") from last_err


# -----------------------------
# Daily download
# -----------------------------

def build_where_for_day(timestamp_field: str, day: date) -> str:
    start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return f"{timestamp_field} >= '{iso_utc(start)}' AND {timestamp_field} < '{iso_utc(end)}'"


def daily_output_path(output_dir: Path, day: date) -> Path:
    return output_dir / f"taxi_trips_{day.strftime('%Y-%m-%d')}.jsonl"


def download_one_day(
    client: SocrataClient,
    settings: StaticSettings,
    day: date,
) -> Tuple[Path, int, int]:

    out_dir = Path(settings.output_dir).resolve()
    ensure_dir(out_dir)

    final_path = daily_output_path(out_dir, day)
    tmp_path = final_path.with_suffix(final_path.suffix + ".part")

    where = build_where_for_day(settings.timestamp_field, day)

    total = 0
    pages = 0
    offset = 0

    with tmp_path.open("w", encoding="utf-8") as f:
        while True:
            rows = client.fetch_page(where=where, limit=settings.page_size, offset=offset, order_by=settings.order_by)
            if not rows:
                break

            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False))
                f.write("\n")

            total += len(rows)
            pages += 1
            offset += settings.page_size

            if len(rows) < settings.page_size:
                break

    tmp_path.replace(final_path)
    return final_path, total, pages


def log_run(
    cfg: Dict[str, Any],
    day: date,
    file_path: Path,
    rows: int,
    pages: int,
    status: str,
    err: Optional[str],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    run = {
        "day": day.strftime("%Y-%m-%d"),
        "file": str(file_path),
        "rows_written": rows,
        "pages": pages,
        "status": status,
        "error": err,
        "started_at_utc": started_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "finished_at_utc": finished_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
    }
    cfg.setdefault("log", {}).setdefault("runs", []).append(run)
    cfg.setdefault("state", {})["last_run_utc"] = utc_now().isoformat().replace("+00:00", "Z")


def choose_days_to_download(cfg: Dict[str, Any], settings: StaticSettings) -> List[date]:
    """
    Regla:
      - Si no está inicializado: backfill últimos settings.backfill_days (incluye hoy)
      - Si está inicializado: actualiza ventana rolling de N días (settings.update_window_days) anclada en el día más nuevo:
          anchor = max(hoy, last_day_downloaded)
          days = [anchor, anchor-1, ..., anchor-(N-1)]
    """
    state = cfg.get("state", {})
    initialized = bool(state.get("initialized"))
    last_day_s = state.get("last_day_downloaded")
    today = today_utc_date()

    if not initialized or not last_day_s:
        days = [today - timedelta(days=i) for i in range(max(1, settings.backfill_days))]
        return sorted(days)

    last_day = parse_yyyy_mm_dd(last_day_s)
    anchor = today if today >= last_day else last_day

    n = max(1, int(settings.update_window_days))
    days = [anchor - timedelta(days=i) for i in range(n)]
    return sorted(days)


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=str, default="etl_config.json", help="Ruta del archivo config/log JSON.")
    ap.add_argument("--backfill", type=int, default=None, help="Fuerza backfill de N días (sobrescribe settings.backfill_days).")
    ap.add_argument("--update-window", type=int, default=None, help="Cuántos días (rolling window) actualizar en cada corrida. Ej: 10, 15.")
    ap.add_argument("--day", type=str, default=None, help="Descarga solo 1 día específico (YYYY-MM-DD).")

    # Notebook/Jupyter-safe
    args, _unknown = ap.parse_known_args()
    return args


def main() -> None:
    args = parse_args()
    config_path = Path(args.config).resolve()

    cfg = load_or_init_config(config_path)
    settings = get_settings(cfg)

    # overrides por CLI
    if args.backfill is not None:
        settings.backfill_days = int(args.backfill)

    if args.update_window is not None:
        settings.update_window_days = int(args.update_window)

    client = SocrataClient(settings)

    if args.day:
        days = [parse_yyyy_mm_dd(args.day)]
    else:
        days = choose_days_to_download(cfg, settings)

    forced_backfill = args.backfill is not None and args.day is None

    out_dir = Path(settings.output_dir).resolve()

    print(f"[INFO] Configuracion: {config_path}")
    print(f"[INFO] Salida: {out_dir}")
    print(f"[INFO] Link descarga: https://{settings.domain}/resource/{settings.dataset_id}.json")
    print(f"[INFO] Cantiad de días (initial): {settings.backfill_days}")
    print(f"[INFO] Ventana de días (rolling): {settings.update_window_days}")
    print(f"[INFO] Dias a descargar: {len(days)} -> {days[0]} .. {days[-1]}")

    for d in days:
        started = utc_now()
        out_file = daily_output_path(out_dir, d)
        rows = 0
        pages = 0
        status = "success"
        err = None

        try:
            file_path, rows, pages = download_one_day(client, settings, d)
            finished = utc_now()
            log_run(cfg, d, file_path, rows, pages, status, err, started, finished)

            cfg.setdefault("state", {})
            cfg["state"]["last_day_downloaded"] = d.strftime("%Y-%m-%d")

            print(f"[OK] {d} -> rows={rows} pages={pages} file={file_path.name}")

        except Exception as e:
            finished = utc_now()
            status = "failed"
            err = str(e)
            log_run(cfg, d, out_file, rows, pages, status, err, started, finished)
            print(f"[FAIL] {d} -> {e}", file=sys.stderr)

        atomic_write_json(config_path, cfg)

    # Marca initialized cuando corresponde (no en modo --day)
    if args.day is None:
        cfg.setdefault("state", {})
        if not cfg["state"].get("initialized") or forced_backfill:
            cfg["state"]["initialized"] = True
            atomic_write_json(config_path, cfg)

    print("[INFO] Done.")


if __name__ == "__main__":
    main()


# In[ ]:




