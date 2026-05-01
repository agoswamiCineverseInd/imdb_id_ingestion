import json

from logger import send_log_async

def insert_imdb_batch(pool, imdb_ids):
    conn = pool.getconn()
    cur = conn.cursor()

    try:
        cur.executemany("""
            INSERT INTO public.dl_imdb_table (imdb, processed)
            VALUES (%s, 'pending')
            ON CONFLICT (imdb) DO NOTHING
        """, [(imdb,) for imdb in imdb_ids])

        conn.commit()

    except Exception as e:
        conn.rollback()
        send_log_async(
            "error",
            "db.py -> insert_imdb_batch()",
            {"imdb_ids_count": len(imdb_ids)},
            str(e)
        )

    finally:
        cur.close()
        pool.putconn(conn)


def get_checkpoint(db_pool):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT data FROM ingestion_checkpoint WHERE id = 1;
            """)
            row = cur.fetchone()
            return row[0] if row else {}
    finally:
        db_pool.putconn(conn)

def update_checkpoint(imdb_id, year, db_pool):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ingestion_checkpoint (id, data, updated_at)
                VALUES (1, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (id)
                DO UPDATE SET
                    data = ingestion_checkpoint.data || EXCLUDED.data,
                    updated_at = CURRENT_TIMESTAMP;
            """, [json.dumps({
                "imdb_id": imdb_id,
                "year": year
            })])

        conn.commit()

    except Exception as e:
        conn.rollback()
        send_log_async("error","db.py -> update_checkpoint()",f"imdb={imdb_id},year={year}",str(e))

    finally:
        db_pool.putconn(conn)