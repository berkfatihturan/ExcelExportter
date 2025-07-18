import mysql.connector
import pandas as pd
import json
import ast
import os

DB_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'root',
    'password': '',
    'database': 'SmartEmkWarehouseDB',
}

EXPORT_FOLDER = "/home/smartemk221/Desktop/wms/exports"

def get_pending_order_jobs():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT * FROM export_jobs
        WHERE table_name = 'orders' AND status = 'pending'
        ORDER BY id ASC
        LIMIT 1
    """)
    job = cursor.fetchone()
    cursor.close()
    conn.close()
    return job


def update_job_status(job_id, status, percent=0):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE export_jobs SET status=%s, percent=%s WHERE id=%s", (status, percent, job_id))
    conn.commit()
    cursor.close()
    conn.close()


def export_order_items_to_excel(job):
    job_id = job["id"]

    # Geçerli Python söz dizimine çevirmek için ast.literal_eval kullan
    try:
        search_values = ast.literal_eval(job["search_values"])
    except Exception as e:
        print(f"search_values hatası: {e}")
        update_job_status(job_id, 'failed', 0)
        return

    order_id = search_values.get("order_id")
    if not order_id:
        update_job_status(job_id, 'failed', 0)
        print("order_id bulunamadı.")
        return

    file_name = job["file_name"]
    file_path = os.path.join(EXPORT_FOLDER, f"{file_name}.xlsx")

    try:
        update_job_status(job_id, 'processing', 0)

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        cursor.execute(f"""
            SELECT 
                o.id AS OrderItemId,
                o.order_id AS OrderId,
                o.order_sort_num AS OrderItemOrderNumber,
                s.code AS ItemCode,
                s.name AS ItemName,
                s.feature AS ItemDescription,
                s.production_date AS ItemProductionDate,
                s.weight AS ItemWeight,
                s.volume AS ItemVolume,
                GROUP_CONCAT(b.barcode) AS Barcode,
                o.orderQty AS OrderQty,
                o.pickingQty AS PickingQty,
                w.name AS PickPlace_W,
                l.name AS PickPlace_L,
                b2.name AS PickPlace_B,
                o.putawayQty AS PutawayQty,
                o.putaway_pin AS PutawayLocId,
                o.shipping_number AS ShippingNumber,
                c.id AS CurrCustomerId,
                c.name AS CurrCustomerName,
                c.post_code AS CurrCustomerPostCode,
                c.phone AS CurrCustomerPhone,
                c.email AS CurrCustomerEmail
            FROM order_items o
            LEFT JOIN current_stocks cs ON cs.id = o.curr_stk_id
            LEFT JOIN stocks s ON s.id = cs.stock_id
            LEFT JOIN barcodes b ON b.curr_stk_id = cs.id
            LEFT JOIN boxes b2 ON b2.id = cs.box_id
            LEFT JOIN locations l ON l.id = b2.location_id
            LEFT JOIN warehouses w ON w.id = l.warehouse_id
            LEFT JOIN customers c ON c.id = o.customer_id
            WHERE o.order_id = %s
            GROUP BY o.id
        """, (order_id,))

        rows = cursor.fetchall()
        df = pd.DataFrame(rows)

        os.makedirs(EXPORT_FOLDER, exist_ok=True)
        df.to_excel(file_path, index=False)

        update_job_status(job_id, 'done', 100)

        cursor.execute("UPDATE export_jobs SET file_path = %s WHERE id = %s", (file_path, job_id))
        conn.commit()

        cursor.close()
        conn.close()

        print(f"[✓] Export completed: {file_path}")
    except Exception as e:
        update_job_status(job_id, 'failed', 0)
        print(f"[!] Export failed: {e}")


if __name__ == "__main__":
    job = get_pending_order_jobs()
    if job:
        export_order_items_to_excel(job)
    else:
        print("[✓] No pending export_jobs found.")