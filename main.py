from fastapi import FastAPI
import threading
from datetime import datetime

from dotenv import load_dotenv
import os

from sparql import ingest_imdbIds_by_year
from logger import send_log_async, service_wake_up
from db import get_checkpoint
from psycopg2 import pool

app = FastAPI()


can_run = True
lock = threading.Lock()


@app.get("/health")
def health_check():
    global can_run

    with lock:
        if not can_run:
            return {"status": "ok", "message": "already running"}

        can_run = False
        threading.Thread(target=run_ingestion, daemon=True).start()

    return {"status": "ok", "message": "started"}


def run_ingestion():
    global can_run
    db_pool = None
    try:
        load_dotenv()
        DATABASE_URL = os.getenv("DATABASE_URL")
        service_wake_up()
        db_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL
        )

        checkpoint = get_checkpoint(db_pool)

        start_year = checkpoint.get("year", 2000)
        last_imdb = checkpoint.get("imdb_id", "")
        current_year = datetime.now().year

        ingest(start_year, current_year, last_imdb, db_pool)

    finally:
        if db_pool:
            db_pool.closeall()
        with lock:
            can_run = True


def ingest(start_year, current_year, last_imdb, db_pool):
    year = start_year

    while year <= current_year:
        try:
            ingest_imdbIds_by_year(year, last_imdb, db_pool)

            send_log_async(
                "info",
                "main.py -> ingest()",
                year,
                f"ingested all imdb ids for year {year}"
            )

        except Exception as e:
            send_log_async(
                "error",
                "main.py -> ingest()",
                year,
                str(e)
            )

        year += 1