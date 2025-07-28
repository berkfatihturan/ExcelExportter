import mysql.connector
from v06.config import DB_CONFIG  # config.py içindeki DB_CONFIG burada olmalı

# === GET PENDING JOB ===
def get_pending_export_job():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT * FROM export_jobs
        WHERE table_name IN ('orders', 'orders_logs') AND status = 'pending'
        ORDER BY id ASC
        LIMIT 1
    """)
    job = cursor.fetchone()
    cursor.close()
    conn.close()
    return job


# === UPDATE JOB STATUS ===
def update_job_status(job_id, status, percent=0):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE export_jobs SET status=%s, percent=%s WHERE id=%s", (status, percent, job_id))
    conn.commit()
    cursor.close()
    conn.close()