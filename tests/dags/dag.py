"""
executor_matrix_test — тест-зонд маршрутизации задач между executor'ами.

Стенд: Airflow 3.2.2 (Helm chart 1.22.0, kind), мульти-executor:
    AIRFLOW__CORE__EXECUTOR = "KubernetesExecutor,CeleryExecutor"
Дефолтный executor — ПЕРВЫЙ в списке, т.е. KubernetesExecutor.

Матрица проб:
    1. default_executor        - executor не указан            -> KubernetesExecutor
    2. kubernetes_explicit     - executor указан явно          -> KubernetesExecutor
    3. celery_explicit         - executor указан явно          -> CeleryExecutor (воркер)
    4. kubernetes_pod_override - KubernetesExecutor + pod_override
                                 (доп. ресурсы и лейблы пода)
    5. report                  - агрегирует результаты и печатает сводку

Где смотреть результат:
    - лог задачи report (сводная таблица + XCom каждой пробы);
    - kubectl -n airflow get pods -w  (видно, как KubernetesExecutor
      создаёт ephemeral-поды для задач во время запуска);
    - celery-проба выполняется на постоянном поде airflow-worker-N.

ИСТОРИЯ БАГА, который чинит этот файл:
    .override() — метод объекта, который возвращает декоратор @task
    (класс airflow.sdk.bases.task.Task). У обычной def-функции такого
    атрибута нет, поэтому строка вида

        celery_explicit = _where_am_i.override(executor="CeleryExecutor")

    падала на этапе ПАРСИНГА DAG (dag-processor выполняет тело
    @dag-функции при импорте файла) с ошибкой:
        AttributeError: 'function' object has no attribute 'override'
    Лечение: над _where_am_i должен стоять декоратор @task.
"""

import json
import os
import platform
import socket

import pendulum
from kubernetes.client import models as k8s

# Airflow 3.x: канонические импорты из airflow.sdk.
# (в Airflow 2.x те же сущности живут в airflow.decorators и
#  airflow.operators.python — fallback оставлен на всякий случай)
try:
    from airflow.sdk import dag, get_current_context, task
except ImportError:  # pragma: no cover
    from airflow.decorators import dag, task  # type: ignore
    from airflow.operators.python import get_current_context  # type: ignore


def _collect_facts(label: str) -> dict:
    """Собирает факты о том, где именно выполняется задача.

    Обычная функция (БЕЗ @task): вызывается ВНУТРИ задачи на рантайме,
    а не при парсинге DAG. Если её по ошибке вызвать снаружи задачи,
    она выполнится один раз на dag-processor'е — это классическая
    ошибка TaskFlow, которую мы здесь не повторяем.
    """
    context = get_current_context()
    ti = context["ti"]

    # Какой executor-конфиг активен (через SDK-конфиг или env).
    configured = ""
    try:
        from airflow.sdk import conf

        configured = conf.get("core", "executor")
    except Exception:
        configured = os.environ.get("AIRFLOW__CORE__EXECUTOR", "?")

    return {
        "label": label,
        "hostname": socket.gethostname(),
        # KubernetesExecutor выставляет AIRFLOW_IS_K8S_EXECUTOR_POD=True
        # в pod-шаблоне задачи — самый надёжный маркер k8s-пода.
        "is_k8s_executor_pod": os.environ.get("AIRFLOW_IS_K8S_EXECUTOR_POD", ""),
        # celery-воркеры чарта получают переменные брокера из секрета.
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

    Декоратор @task превращает функцию в объект airflow.sdk.bases.task.Task,
    у которого ЕСТЬ метод .override(). Без декоратора .override() не
    существует — именно это было причиной ImportError вашего DAG.
    """
    facts = _collect_facts(label)
    print(json.dumps(facts, ensure_ascii=False, indent=2, default=str))
    return facts


@task
def _report(*results: dict) -> None:
    """Агрегирует пробы всех задач и печатает сводную таблицу."""
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
    if len(hosts) < len(results):
        print(
            "WARNING: несколько задач выполнились на одном хосте. "
            "Для celery это нормально (несколько задач на одном воркере), "
            "для k8s-проб — нет."
        )


@dag(
    dag_id="executor_matrix_test",
    schedule=None,  # запуск только вручную
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["test", "executors"],
    doc_md=__doc__,
)
def executor_matrix_test():
    # 1. Executor не указан -> дефолтный (первый в AIRFLOW__CORE__EXECUTOR).
    default_run = _where_am_i.override(task_id="default_executor")(
        "default: executor не указан -> первый из списка"
    )

    # 2. KubernetesExecutor указан явно (полное имя класса, как в конфиге).
    #    Короткие алиасы ("kubernetes") тоже работают.
    k8s_run = _where_am_i.override(
        task_id="kubernetes_explicit",
        executor="KubernetesExecutor",
    )("kubernetes: указан явно")

    # 3. CeleryExecutor: задача уйдёт в очередь default к celery-воркерам
    #    чарта (они слушают очередь default из коробки). Если задача
    #    зависнет в queued — проверьте, что поды airflow-worker-N живы.
    celery_run = _where_am_i.override(
        task_id="celery_explicit",
        executor="CeleryExecutor",
    )("celery: указан явно")

    # 4. KubernetesExecutor + executor_config: наш V1Pod стратегически
    #    мержится с pod_template чарта (включая ваш git-sync init).
    #    Имя контейнера ОБЯЗАНО совпадать с базовым контейнером шаблона
    #    ("base"), иначе под не стартует. Init-контейнеры не трогаем —
    #    они останутся из шаблона.
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
    )("kubernetes + pod_override: ресурсы и лейблы пода")

    # report получит результаты всех проб по XCom; зависимости строятся
    # автоматически из переданных XComArg (все 4 пробы -> report).
    _report(default_run, k8s_run, celery_run, k8s_pod_override)


executor_matrix_test()