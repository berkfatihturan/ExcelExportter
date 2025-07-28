from time import sleep
from v06.job_utils import get_pending_export_job
from v06.ExportOrdersLogsJob import ExportOrdersLogsJob
from v06.ExportOrderItemsJob import ExportOrderItemsJob


def run():
    while True:
        job = get_pending_export_job()
        if job:
            table = job['table_name']
            if table == 'orders':
                ExportOrderItemsJob(job).run()
            elif table == 'orders_logs':
                ExportOrdersLogsJob(job).run()
            else:
                print(f"[!] Desteklenmeyen tablo: {table}")
        else:
            print("[✓] Bekleyen iş yok.")

        sleep(10)