import mysql.connector
import pandas as pd
from datetime import datetime
import os
import ast

from v06.job_utils import update_job_status
from v06.config import EXPORT_FOLDER_LOCAL, EXPORT_FOLDER, DB_CONFIG

class ExportOrderItemsJob:
    def __init__(self, job):
        self.job = job
        self.job_id = job["id"]
        self.search_values = {}
        self.order_id = None
        self.file_name = ""
        self.file_path = ""
        self.df = pd.DataFrame()

    def run(self):
        try:
            self._parse_search_values()
            self._validate_order_id()
            self._prepare_paths()
            update_job_status(self.job_id, 'processing', 0)
            rows = self._fetch_data()
            self._create_dataframe(rows)
            self._export_to_excel()
            self._mark_done()
            print(f"[✓] Export tamamlandı: {self.file_path}")
        except Exception as e:
            update_job_status(self.job_id, 'failed', 0)
            print(f"[!] Export işlemi başarısız: {e}")

    def _parse_search_values(self):
        try:
            self.search_values = ast.literal_eval(self.job["search_values"])
        except Exception as e:
            raise ValueError(f"search_values ayrıştırma hatası: {e}")

    def _validate_order_id(self):
        self.order_id = self.search_values.get("order_id")
        if not self.order_id:
            raise ValueError("order_id bulunamadı.")

    def _prepare_paths(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.file_name = f"orders_{self.order_id}_{timestamp}.xlsx"
        base_folder = EXPORT_FOLDER_LOCAL if self.search_values.get("local_host") else EXPORT_FOLDER
        self.file_path = os.path.join(base_folder, f"orderList/{self.job['file_name']}")
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

    def _fetch_data(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
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
        """, (self.order_id,))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows

    def _create_dataframe(self, rows):
        self.df = pd.DataFrame(rows)

    def _export_to_excel(self):
        self.df.to_excel(self.file_path, index=False)

    def _mark_done(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE export_jobs 
            SET status=%s, percent=%s, file_name=%s, file_path=%s 
            WHERE id = %s
        """, ('done', 100, self.file_name, self.file_path, self.job_id))
        conn.commit()
        cursor.close()
        conn.close()