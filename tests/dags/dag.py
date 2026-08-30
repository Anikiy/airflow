"""
Семейство executor_matrix_test_NN — 10 DAG'ов, расписание каждую минуту.

Стенд: Airflow 3.2.2 (Helm chart 1.22.0, kind), мульти-executor:
    AIRFLOW__CORE__EXECUTOR = "KubernetesExecutor,CeleryExecutor"
Дефолтный executor — ПЕРВЫЙ в списке, т.е. KubernetesExecutor.

Что делает семейство:
    - executor_matrix_test_01 ... executor_matrix_test_10 (сгенерированы циклом);
    - CRON = "* * * * *" — новый запуск каждую минуту, catchup=False;
    - НЕЧЁТНЫЕ DAG'и: дефолтный executor = KubernetesExecutor (как в кластере);
    - ЧЁТНЫЕ DAG'и: default_args={"executor": "CeleryExecutor"} — весь DAG по
      умолчанию на celery (для дашбордов airflow_executor_* по лейблу executor);
    - в каждом DAG'е матрица из 5 задач:
        default_executor         -> дефолтный executor DAG'а
        kubernetes_explicit      -> KubernetesExecutor явно
        celery_explicit          -> CeleryExecutor явно
        kubernetes_pod_override  -> KubernetesExecutor + executor_config
        report                   -> сводка по XCom (смотреть лог задачи report)

Нагрузка на кластер (10 DAG'ов, cron раз в минуту):
    ~30 ephemeral-подов KubernetesExecutor + ~20 celery-задач в минуту;
    каждый k8s-под делает one-time git clone (git-sync init из pod_template).
    Если kind не справляется: уменьшите NUM_DAGS, поставьте "*/2 * * * *"
    в CRON или поставьте часть DAG'ов на паузу в UI.

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

NUM_DAGS = 10
CRON = "* * * * *"  # каждую минуту


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


@task
def _report(*results: dict) -> None:
    """Агрегирует пробы DAG'а и печатает сводную таблицу."""
    print("=== EXECUTOR MATRIX: сводка ===")
    width = max(len(r["label"]) for r in results)
    for r in results:
        if r["is_k8s_executor_pod"]:
            hint = "KubernetesExecutor pod"
        elif r["has_celery_broker_env"]:
            hint = "celery worker (по env брокера)"
        else:
            hint = "неизвестно (нет маркеров)"
        print(
            f'{r["label"].ljust(width)} | host={r["hostname"]} '
            f'| try#{r["try_number"]} | {hint}'
        )
    hosts = {r["hostname"] for r in results}
    print(f"уникальных хостов: {len(hosts)} из {len(results)} задач")


def _build_matrix(dag_id: str, default_to_celery: bool) -> None:
    """Фабрика одного DAG'а семейства; цикл в конце файла создаёт 10 штук."""

    @dag(
        dag_id=dag_id,
        schedule=CRON,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,                          # не догонять пропущенные интервалы
        max_active_runs=1,                      # не копить очередь, если запуск > 1 мин
        dagrun_timeout=timedelta(minutes=5),    # самоубийство зависшего запуска
        default_args=(
            {"executor": "CeleryExecutor"} if default_to_celery else {}
        ),
        tags=["test", "executors", "every-minute"],
        doc_md=__doc__,
    )
    def _matrix_dag():
        default_run = _where_am_i.override(task_id="default_executor")(
            "default: executor не указан -> "
            + (
                "CeleryExecutor (через default_args)"
                if default_to_celery
                else "первый из списка (KubernetesExecutor)"
            )
        )

        # Явное указание всегда побеждает default_args.
        k8s_run = _where_am_i.override(
            task_id="kubernetes_explicit",
            executor="KubernetesExecutor",
        )("kubernetes: указан явно")

        celery_run = _where_am_i.override(
            task_id="celery_explicit",
            executor="CeleryExecutor",
        )("celery: указан явно")

        # V1Pod стратегически мержится с pod_template (git-sync init сохраняется).
        # Имя контейнера ОБЯЗАНО совпадать с базовым контейнером шаблона ("base").
        k8s_pod_override = _where_am_i.override(
            task_id="kubernetes_pod_override",
            executor="KubernetesExecutor",
            executor_config={
                "pod_override": k8s.V1Pod(
                    metadata=k8s.V1ObjectMeta(labels={"where": "pod-override"}),
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(
                                    requests={"cpu": "100m", "memory": "256Mi"},
                                    limits={"cpu": "500m", "memory": "512Mi"},
                                ),
                            )
                        ]
                    ),
                )
            },
        )("kubernetes + pod_override")

        # report получит результаты всех проб по XCom; зависимости строятся
        # автоматически из переданных XComArg.
        _report(default_run, k8s_run, celery_run, k8s_pod_override)

    _matrix_dag()


for _i in range(1, NUM_DAGS + 1):
    _build_matrix(
        dag_id=f"executor_matrix_test_{_i:02d}",
        default_to_celery=(_i % 2 == 0),
    )