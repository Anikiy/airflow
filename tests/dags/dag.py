import json
import time
import random
import os
from datetime import datetime

from airflow.sdk import dag, task, get_current_context

# ---------- ЭКЗЕКУТОРЫ ----------
# Алиасы Airflow 3: "celery" и "kubernetes"
# (можно и полные пути: airflow.providers.celery.executors.celery_executor.CeleryExecutor и т.д.)
CELERY = "celery"
KUBERNETES = "kubernetes"

# (Опционально) override пода для задач на KubernetesExecutor — задаём ресурсы
try:
    from kubernetes.client import models as k8s

    K8S_POD_OVERRIDE = {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"cpu": "500m", "memory": "512Mi"},
                            limits={"cpu": "2", "memory": "1Gi"},
                        ),
                    )
                ]
            )
        ),
    }
except ImportError:
    K8S_POD_OVERRIDE = None


# ---------- ЗАДАЧИ (чередуем executor) ----------

@task(executor=CELERY)
def extract_params():
    params = {
        "cpu_duration": random.randint(15, 40),
        "cpu_iterations": random.randint(100_000, 1_000_000),
        "ram_target_mb": random.randint(30, 150),
        "ram_hold_time": random.randint(5, 20),
        "io_duration": random.randint(10, 30),
    }
    return params


@task(executor=KUBERNETES, executor_config=K8S_POD_OVERRIDE)
def cpu_stress(task_params: dict):
    duration = task_params.get("cpu_duration", 20)
    iterations = task_params.get("cpu_iterations", 500_000)

    start_time = time.time()
    while time.time() - start_time < duration:
        x = 0
        for i in range(iterations):
            x += i * i
        time.sleep(0.1)
    return {"cpu_work_done": True, "duration": duration}


@task(executor=CELERY)
def ram_stress(task_params: dict):
    target_size_mb = task_params.get("ram_target_mb", 100)
    hold_time = task_params.get("ram_hold_time", 15)
    target_size = target_size_mb * 1024 * 1024

    data = bytearray()
    try:
        while len(data) < target_size:
            chunk_size = 10 * 1024 * 1024
            data.extend(bytearray(chunk_size))
            time.sleep(0.5)
        time.sleep(hold_time)
    except MemoryError:
        pass
    finally:
        del data
    return {"ram_work_done": True, "allocated_mb": target_size_mb}


@task(executor=KUBERNETES, executor_config=K8S_POD_OVERRIDE)
def io_stress(task_params: dict):
    duration = task_params.get("io_duration", 15)

    ctx = get_current_context()
    dag_id = ctx["dag"].dag_id
    task_id = ctx["ti"].task_id

    dummy_file = f"/tmp/airflow_io_test_{dag_id}_{task_id}.bin"
    start_time = time.time()

    try:
        while time.time() - start_time < duration:
            with open(dummy_file, "wb") as f:
                f.write(os.urandom(10 * 1024 * 1024))
            time.sleep(0.5)
    finally:
        if os.path.exists(dummy_file):
            os.remove(dummy_file)
    return {"io_work_done": True, "duration": duration}


@task(executor=CELERY)
def validate_results(cpu_result: dict, ram_result: dict, io_result: dict):
    time.sleep(random.uniform(1, 3))
    return {"validated": True}


@task(executor=KUBERNETES, executor_config=K8S_POD_OVERRIDE)
def summarize_results(validation: dict):
    ctx = get_current_context()
    dag_id = ctx["dag"].dag_id
    summary = {"dag_id": dag_id, "status": "completed", "timestamp": datetime.now().isoformat()}
    print(f"SUMMARY [{dag_id}]: {json.dumps(summary, indent=2)}")
    return summary


# ---------- ФАБРИКА DAG ----------
def create_stress_pipeline(dag_id: str):
    @dag(
        dag_id=dag_id,
        schedule="*/1 * * * *",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=["CPU", "RAM", "IO", "LoadTest", "MultiDag", "MultiExecutor"],
        default_args={"retries": 1, "retry_delay": 30},
    )
    def stress_pipeline():
        params = extract_params()                   # celery
        cpu_res = cpu_stress(task_params=params)    # kubernetes
        ram_res = ram_stress(task_params=params)    # celery
        io_res = io_stress(task_params=params)      # kubernetes
        validation = validate_results(cpu_res, ram_res, io_res)  # celery
        summarize_results(validation)               # kubernetes
    return stress_pipeline()


# Регистрируем 9 DAG-ов
for i in range(1, 10):
    dag_name = f"stress_test_dag_{i:02d}"
    globals()[dag_name] = create_stress_pipeline(dag_name)