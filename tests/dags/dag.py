"""Тестовые DAG'и для проверки multi-executor конфигурации (Airflow 3).

Ваш values задан как:
    executor: "KubernetesExecutor,CeleryExecutor"   # = AIRFLOW__CORE__EXECUTOR

Правила (официальная дока 3.2.2, "Using Multiple Executors Concurrently"):
  1. ПЕРВЫЙ executor в списке — дефолтный для всего окружения. У вас это
     KubernetesExecutor: любая задача БЕЗ явного executor= пойдёт туда.
  2. Остальные executor'ы списка инициализируются и готовы принимать задачи,
     если указать их на задаче или DAG'е.
  3. Per-task: параметр ``executor=`` у оператора / ``@task(executor=...)``.
  4. Per-DAG: ``default_args={"executor": "..."}`` — все задачи DAG'а
     наследуют его, если у задачи не задано своё переопределение.
  5. Если указать executor, которого НЕТ в конфиге — DAG упадёт при парсинге
     (warning в Airflow UI).

Что в файле:
  - executor_matrix_test  — один DAG, задачи выполняются на ОБОИХ executor'ах;
  - executor_only__kubernetesexecutor / executor_only__celeryexecutor —
    два одинаковых DAG'а, целиком приколотых к разным executor'ам
    (удобно сравнивать side-by-side).

Как посмотреть, ГДЕ выполнилась задача:
  - логи задачи: строка "Задача выполнилась в поде: <hostname>"
    (Celery -> постоянный под <release>-worker-...; K8s -> ephemeral-под задачи);
  - kubectl get pods -n airflow -l tier=airflow --watch  (во время запуска
    k8s-задач появляются короткоживущие поды);
  - Airflow UI: Grid -> задача -> Details -> атрибут Executor.
"""

from __future__ import annotations

import socket
import time
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

K8S_EXECUTOR = "KubernetesExecutor"
CELERY_EXECUTOR = "CeleryExecutor"

# Специально подольше, чтобы успеть увидеть ephemeral-под KubernetesExecutor
# через `kubectl get pods -n airflow --watch`
TASK_SLEEP_SECONDS = 10


def _where_am_i() -> str:
    """Печатает имя пода, в котором реально выполнилась задача."""
    host = socket.gethostname()
    print(f"Задача выполнилась в поде: {host}")
    print(f"sleep {TASK_SLEEP_SECONDS}s — можно смотреть kubectl get pods -n airflow")
    time.sleep(TASK_SLEEP_SECONDS)
    return host


# =============================================================================
# DAG 1: матрица — один DAG, задачи на РАЗНЫХ executor'ах
# =============================================================================
@dag(
    dag_id="executor_matrix_test",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "multi-executor"],
    doc_md=__doc__,
    # Per-DAG executor: все задачи этого DAG'а по умолчанию — CeleryExecutor.
    # (Дефолт окружения у вас KubernetesExecutor — первый в списке values.)
    default_args={"executor": CELERY_EXECUTOR},
)
def executor_matrix_test():
    # 1) Явный CeleryExecutor на задаче (эквивалент BashOperator(executor=...))
    celery_explicit = _where_am_i.override(
        task_id="celery__explicit", executor=CELERY_EXECUTOR
    )()

    # 2) Явный KubernetesExecutor — переопределяет default_args DAG'а
    k8s_explicit = _where_am_i.override(
        task_id="k8s__explicit", executor=K8S_EXECUTOR
    )()

    # 3) Bash на KubernetesExecutor — классический синтаксис из доки
    k8s_bash = BashOperator(
        task_id="k8s__bash",
        executor=K8S_EXECUTOR,
        bash_command="echo 'pod:' $(hostname); date; sleep 10",
    )

    # celery первым, затем две k8s-задачи ПАРАЛЛЕЛЬНО:
    # в этот момент в кластере одновременно работают оба executor'а
    celery_explicit >> [k8s_explicit, k8s_bash]


executor_matrix_test_dag = executor_matrix_test()


# =============================================================================
# DAG 2 и 3: одинаковый тест, каждый целиком на своём executor'е
# =============================================================================
for _executor in (K8S_EXECUTOR, CELERY_EXECUTOR):
    _dag_id = f"executor_only__{_executor.lower()}"

    @dag(
        dag_id=_dag_id,
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["test", "multi-executor"],
        default_args={"executor": _executor},  # весь DAG на конкретном executor'е
        doc_md=f"Клон теста, целиком выполняющийся на **{_executor}**.",
    )
    def _make_executor_only_dag():
        step_1 = _where_am_i.override(task_id="step_1")()
        step_2 = _where_am_i.override(task_id="step_2")()
        step_1 >> step_2

    globals()[_dag_id] = _make_executor_only_dag()