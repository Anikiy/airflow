"""
Семейства celery_dag_NN (10 шт.) и kubernetes_dag_NN (10 шт.) — сплит 10/10.

Стенд: Airflow 3.2.2 (Helm chart 1.22.0, kind), мульти-executor:
    AIRFLOW__CORE__EXECUTOR = "KubernetesExecutor,CeleryExecutor"
Дефолтный executor — ПЕРВЫЙ в списке, т.е. KubernetesExecutor.

Отличие от прошлой версии (executor_matrix_test_NN):
    - старые DAG'и ИСЧЕЗНУТ из UI после синка этого файла (файл заменён);
    - теперь 20 DAG'ов с ЧИСТЫМ сплитом: 10 строго celery + 10 строго k8s;
    - НЕТ перекрёстных задач (никаких celery_explicit внутри k8s-DAG'а);
    - КАЖДАЯ задача пинается ЯВНО через .override(executor=...) И ПЛЮС
      default_args={"executor": ...} на весь DAG. Маршрутизация больше не
      зависит от того, какой executor дефолтный в кластере.

Имена DAG'ов = их executor (удобно фильтровать в UI по тегам
"celery" / "kubernetes" и смотреть в Flower: celery_dag_* там есть,
kubernetes_dag_* там быть НЕ должно — это норма).

Как проверить, что сплит работает (минимум ручной работы):
    1) открой любой запуск -> задача `report` -> лог: таблица проб и
       вердикт "VERDICT: OK" / "VERDICT: MISMATCH" + XCom с summary;
    2) celery_dag_*: hostname задач = airflow-worker-N (StatefulSet воркеров);
    3) kubernetes_dag_*: hostname = имя ephemeral-пода, маркер
       AIRFLOW_IS_K8S_EXECUTOR_POD=True; в `kubectl get pods -n airflow` при
       каждом запуске мелькают поды kubernetes-dag-*-*;
    4) UI: Task Instance Details -> Hostname; теги celеry/kubernetes.

Нагрузка (20 DAG'ов, cron раз в минуту, по 4 задачи):
    ~30 ephemeral-подов k8s + ~30 celery-задач в минуту.
    Если kind не справляется: CRON = "*/2 * * * *" или пауза части DAG'ов.

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

try:
    from airflow.sdk import dag, get_current_context, task
except ImportError:  # pragma: no cover — fallback для Airflow 2.x
    from airflow.decorators import dag, task  # type: ignore
    from airflow.operators.python import get_current_context  # type: ignore

NUM_DAGS_PER_EXECUTOR = 10
CRON = "* * * * *"  # каждую минуту

CELERY = "CeleryExecutor"
K8S = "KubernetesExecutor"


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


def _build_split_dag(dag_id: str, executor: str, expected_kind: str, group_tag: str) -> None:
    """Фабрика одного DAG'а; цикл в конце файла создаёт 10 celery + 10 k8s."""

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
        tags=["executor-split", group_tag, "every-minute"],
        doc_md=(
            f"Все задачи этого DAG выполняются строго на **{executor}**.\n\n"
            f"Проверка: лог задачи `report` (вердикт OK/MISMATCH), для Celery — "
            f"наличие задач в Flower; для Kubernetes — ephemeral-поды и hostname."
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

        # report получит результаты всех проб по XCom; зависимости строятся
        # автоматически из переданных XComArg.
        _report.override(
            task_id="report", executor=executor,
        )(expected_kind, probe_1, probe_2, probe_3)

    _split_dag()


for _i in range(1, NUM_DAGS_PER_EXECUTOR + 1):
    _build_split_dag(
        dag_id=f"celery_dag_{_i:02d}",
        executor=CELERY,
        expected_kind="celery",
        group_tag="celery",
    )
    _build_split_dag(
        dag_id=f"kubernetes_dag_{_i:02d}",
        executor=K8S,
        expected_kind="kubernetes",
        group_tag="kubernetes",
    )