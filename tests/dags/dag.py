from airflow.decorators import dag, task
from datetime import datetime
import time
import logging

log = logging.getLogger(__name__)

@dag(start_date=datetime(2023, 1, 1), schedule=None, catchup=False)
def simple_timer_dag():
    @task
    def wait_and_log():
        log.info("⏱ Таймер запущен (2 мин)...")
        time.sleep(120)
        log.info("✅ Таймер завершён.")
    
    wait_and_log()

simple_timer_dag()
