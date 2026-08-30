"""
Семейства celery_dag_NN (10 шт.) и kubernetes_dag_NN (10 шт.) — сплит 10/10
+ управляемая нагрузка на CPU и RAM в каждом DAG'е.

Стенд: Airflow 3.2.2 (Helm chart 1.22.0, kind), мульти-executor:
    AIRFLOW__CORE__EXECUTOR = "KubernetesExecutor,CeleryExecutor"
Дефолтный executor — ПЕРВЫЙ в списке, т.е. KubernetesExecutor.

Структура каждого DAG'а (4 задачи):
    probe_1..3   -> параллельные зонды места выполнения (как раньше)
    cpu_ram_load -> НАГРУЗКА: CPU (N процессов × X секунд, 100% ядро)
                    + RAM (Y МБ, затрагиваются страницы, держится всю нагрузку)
    report       -> сводная таблица проб + VERDICT: OK/MISMATCH
                    (запускается после нагрузки)

Профиль нагрузки масштабируется по номеру DAG'а (idx = 1..10):
    cpu_seconds = LOAD_CPU_BASE_SEC + idx        -> 11..20 сек
    ram_mb      = LOAD_RAM_BASE_MB + idx*STEP    -> 80..224 МБ
    процессов всегда LOAD_CPU_PROCS (2)

Где нагрузка живёт:
    - celery_dag_*: ВНУТРИ подов celery-воркеров (airflow-worker-N) —
      нагружаются сами воркеры, pod_override игнорируется;
    - kubernetes_dag_*: отдельные ephemeral-поды с pod_override:
      requests 250m/256Mi, limits 1 CPU/512Mi, лейбл airflow-load=cpu-ram.
      Имя контейнера ОБЯЗАНО быть "base" (strategic merge с pod_template).

Куда смотреть:
    - лог задачи cpu_ram_load: elapsed, exitcodes процессов, ru_maxrss;
    - Grafana: node exporter (CPU/RAM нод), cAdvisor/kubelet (контейнеры),
      Flower (длительность celery-задач);
    - kubectl -n airflow get pods -w: поды нагрузки k8s живут ~40-60 сек.
      (kubectl top может не работать — в kind нет metrics-server).

Безопасность:
    - RAM ограничена (<=224 МБ на задачу), CPU-процессы daemon=True
      (умрут вместе с родителем), execution_timeout=3 мин, retries=0
      (нагрузка не перезапускается);
    - worst case celery: 10 задач/мин на 5 воркеров = до 2 нагрузок
      на воркер (4 ядра на 11-20 сек, ~450 МБ пиково);
    - если kind давится: LOAD_CPU_PROCS=1, LOAD_RAM_STEP_MB=8, cron "*/2".

Служебное (чтобы не регрессировать):
    .override() есть только у объекта, возвращаемого декоратором @task
    (airflow.sdk.bases.task.Task). У обычной def-функции его нет — именно это
    давало "AttributeError: 'function' object has no attribute 'override'"
    при парсинге DAG.
"""

import json
import os
import platform
import socket
from datetime import timedelta

import pendulum
from kubernetes.client import models as k8s

try:
    from airflow.sdk import dag, get_current_context, task
except ImportError:  # pragma: no cover — fallback для Airflow 2.x
    from airflow.decorators import dag, task  # type: ignore
    from airflow.operators.python import get_current_context  # type: ignore

NUM_DAGS_PER_EXECUTOR = 10
CRON = "* * * * *"  # каждую минуту

CELERY = "CeleryExecutor"
K8S = "KubernetesExecutor"

# --- ручки нагрузки -------------------------------------------------------
LOAD_CPU_PROCS = 2        # процессов, жгущих ядро на 100%
LOAD_CPU_BASE_SEC = 10    # секунд горения: + idx -> 11..20
LOAD_RAM_BASE_MB = 64     # МБ RAM: + idx * LOAD_RAM_STEP_MB -> 80..224
LOAD_RAM_STEP_MB = 16
K8S_LOAD_REQUESTS = {"cpu": "250m", "memory": "256Mi"}
K8S_LOAD_LIMITS = {"cpu": "1", "memory": "512Mi"}
# --------------------------------------------------------------------------


def _collect_facts(label: str) -> dict:
    """Факты о месте выполнения.

    Обычная функция (БЕЗ @task): вызывается на рантайме ВНУТРИ задачи,
    а не при парсинге DAG.
    """
    context = get_current_context()
    ti = context["ti"]

    configured = ""
    try:
        from airflow.sdk import conf

        configured = conf.get("core", "executor")
    except Exception:
        configured = os.environ.get("AIRFLOW__CORE__EXECUTOR", "?")

    return {
        "label": label,
        "hostname": socket.gethostname(),
        # Маркер KubernetesExecutor: выставляется True в pod-шаблоне задачи.
        "is_k8s_executor_pod": os.environ.get("AIRFLOW_IS_K8S_EXECUTOR_POD", ""),
        # celery-воркеры чарта имеют переменные брокера из секрета.
        "has_celery_broker_env": bool(os.environ.get("AIRFLOW__CELERY__BROKER_URL")),
        "configured_executors": configured,
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["dag_run"].run_id,
        "try_number": getattr(ti, "try_number", None),
        "queue": getattr(ti, "queue", None),
        "pool": getattr(ti, "pool", None),
        "python": platform.python_version(),
        "pid": os.getpid(),
    }


@task
def _where_am_i(label: str) -> dict:
    """Задача-зонд: печатает и возвращает факты о месте выполнения.

    @task обязателен — только декорированный объект имеет .override().
    """
    facts = _collect_facts(label)
    print(json.dumps(facts, ensure_ascii=False, indent=2, default=str))
    return facts


# --- нагрузка CPU/RAM ------------------------------------------------------

def _generate_cpu_load(seconds: int, procs: int) -> dict:
    """Жжёт CPU: `procs` дочерних процессов по 100% ядра `seconds` секунд.

    Обычная функция (не @task) — удобно вынести и тестировать отдельно.
    """
    import multiprocessing as mp
    import time

    def _spin(stop_ts: float) -> None:
        x = 1.000001
        while time.monotonic() < stop_ts:
            for _ in range(50_000):
                x = (x * 1.0000001) % 999_983

    started = time.monotonic()
    stop_ts = started + seconds
    ctx = (
        mp.get_context("fork")
        if "fork" in mp.get_all_start_methods()
        else mp.get_context("spawn")
    )
    workers = [
        ctx.Process(target=_spin, args=(stop_ts,), daemon=True)
        for _ in range(max(1, procs))
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=seconds + 60)
    return {
        "procs": len(workers),
        "seconds": seconds,
        "elapsed_sec": round(time.monotonic() - started, 1),
        "exitcodes": [w.exitcode for w in workers],
    }


def _touch_and_hold_ram(mb: int) -> bytearray:
    """Аллоцирует mb МБ и затрагивает каждую страницу ( resident set)."""
    block = bytearray(mb * 1024 * 1024)
    for i in range(0, len(block), 4096):
        block[i] = 1
    return block


def _k8s_load_pod_override() -> dict:
    """executor_config для подов нагрузки в kubernetes_dag_*.

    Имя контейнера ОБЯЗАНО совпадать с базовым ("base") — стратегический
    merge с pod_template (git-sync init сохраняется).
    """
    return {
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels={"airflow-load": "cpu-ram"}),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests=K8S_LOAD_REQUESTS,
                            limits=K8S_LOAD_LIMITS,
                        ),
                    )
                ]
            ),
        )
    }


@task
def _cpu_ram_load(idx: int, cpu_seconds: int, cpu_procs: int, ram_mb: int) -> dict:
    """Нагрузка: RAM держится ВСЁ время горения CPU (параллельно)."""
    import resource
    import time

    print(
        f"[load] DAG #{idx}: CPU {cpu_procs} x {cpu_seconds}s, "
        f"RAM {ram_mb} MB (держится всё время нагрузки)"
    )
    started = time.monotonic()
    block = _touch_and_hold_ram(ram_mb)
    cpu_result = _generate_cpu_load(cpu_seconds, cpu_procs)
    # ru_maxrss на Linux в КБ -> МБ; включает пик самого процесса
    maxrss_mb = round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1
    )
    del block
    result = {
        "dag_index": idx,
        "ram_mb": ram_mb,
        "maxrss_mb": maxrss_mb,
        "total_elapsed_sec": round(time.monotonic() - started, 1),
        **cpu_result,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


# --------------------------------------------------------------------------


def _observed_kind(facts: dict) -> str:
    """Классификация места выполнения по надёжным маркерам.

    1) маркер пода KubernetesExecutor — авторитетный для k8s;
    2) иначе celery, если есть env брокера или hostname воркера;
    3) иначе unknown.
    """
    if str(facts.get("is_k8s_executor_pod", "")).strip().lower() == "true":
        return "kubernetes"
    hostname = str(facts.get("hostname", ""))
    if facts.get("has_celery_broker_env") or hostname.startswith("airflow-worker"):
        return "celery"
    return "unknown"


@task
def _report(expected_kind: str, *results: dict) -> dict:
    """Сводная таблица проб + вердикт: все ли задачи на ожидаемом executor'е."""
    print(f"=== EXPECTED executor: {expected_kind} ===")
    width = max(len(r["label"]) for r in results)
    for r in results:
        kind = _observed_kind(r)
        marker = "OK" if kind == expected_kind else "MISMATCH"
        print(
            f'{r["label"].ljust(width)} | host={r["hostname"]} '
            f'| try#{r["try_number"]} | observed={kind} | {marker}'
        )

    observed_kinds = sorted({_observed_kind(r) for r in results})
    mismatches = [r["task_id"] for r in results if _observed_kind(r) != expected_kind]
    verdict = "OK" if not mismatches else "MISMATCH"
    hosts = sorted({r["hostname"] for r in results})

    print(f"configured executors (env core.executor): {results[0]['configured_executors']}")
    print(f"observed kinds: {observed_kinds}")
    print(f"уникальных хостов: {len(hosts)} из {len(results)} задач -> {hosts}")
    print(f"VERDICT: {verdict} (expected={expected_kind}, mismatched={mismatches})")

    return {
        "expected": expected_kind,
        "observed": observed_kinds,
        "verdict": verdict,
        "hosts": hosts,
        "mismatched_tasks": mismatches,
    }


def _build_split_dag(
    dag_id: str, executor: str, expected_kind: str, group_tag: str, idx: int
) -> None:
    """Фабрика одного DAG'а; цикл в конце файла создаёт 10 celery + 10 k8s."""
    cpu_seconds = LOAD_CPU_BASE_SEC + idx
    ram_mb = LOAD_RAM_BASE_MB + idx * LOAD_RAM_STEP_MB
    # pod_override имеет смысл только для KubernetesExecutor
    extra_load_kwargs = (
        {"executor_config": _k8s_load_pod_override()} if executor == K8S else {}
    )

    @dag(
        dag_id=dag_id,
        schedule=CRON,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,                          # не догонять пропущенные интервалы
        max_active_runs=1,                      # не копить очередь, если запуск > 1 мин
        dagrun_timeout=timedelta(minutes=5),    # самоубийство зависшего запуска
        default_args={
            "executor": executor,               # страховка на уровне всего DAG'а
            "retries": 1,
            "retry_delay": timedelta(seconds=15),
        },
        tags=["executor-split", group_tag, "every-minute", "load"],
        doc_md=(
            f"Все задачи на **{executor}**. "
            f"Нагрузка (#{idx}): CPU {LOAD_CPU_PROCS}×{cpu_seconds}с, "
            f"RAM {ram_mb} МБ держится всю нагрузку"
            + (
                f"; pod requests {K8S_LOAD_REQUESTS['cpu']}/{K8S_LOAD_REQUESTS['memory']}, "
                f"limits {K8S_LOAD_LIMITS['cpu']}/{K8S_LOAD_LIMITS['memory']}"
                if executor == K8S
                else " (внутри пода воркера)"
            )
            + ".\n\n"
            "Проверка: лог `cpu_ram_load` (elapsed/maxrss) и `report` (вердикт OK/MISMATCH)."
        ),
    )
    def _split_dag():
        # 3 независимых пробы; каждая ЯВНО пинается на executor DAG'а.
        probe_1 = _where_am_i.override(
            task_id="probe_1", executor=executor,
        )("probe 1: executor задан явно (.override)")
        probe_2 = _where_am_i.override(
            task_id="probe_2", executor=executor,
        )("probe 2: executor задан явно (.override)")
        probe_3 = _where_am_i.override(
            task_id="probe_3", executor=executor,
        )("probe 3: executor задан явно (.override)")

        # Нагрузка после проб; для k8s — с pod_override (requests/limits).
        load = _cpu_ram_load.override(
            task_id="cpu_ram_load",
            executor=executor,
            retries=0,
            execution_timeout=timedelta(minutes=3),
            **extra_load_kwargs,
        )(idx, cpu_seconds, LOAD_CPU_PROCS, ram_mb)
        [probe_1, probe_2, probe_3] >> load

        # report получит результаты всех проб по XCom; зависимости строятся
        # автоматически из переданных XComArg.
        report = _report.override(
            task_id="report", executor=executor,
        )(expected_kind, probe_1, probe_2, probe_3)
        load >> report

    _split_dag()


for _i in range(1, NUM_DAGS_PER_EXECUTOR + 1):
    _build_split_dag(
        dag_id=f"celery_dag_{_i:02d}",
        executor=CELERY,
        expected_kind="celery",
        group_tag="celery",
        idx=_i,
    )
    _build_split_dag(
        dag_id=f"kubernetes_dag_{_i:02d}",
        executor=K8S,
        expected_kind="kubernetes",
        group_tag="kubernetes",
        idx=_i,
    )