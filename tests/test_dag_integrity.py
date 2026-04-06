"""
DAG integrity tests — validate all DAGs load without errors.
Run: pytest tests/test_dag_integrity.py
"""
import importlib
import os
import sys
from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    """All DAGs must load without import errors."""
    assert dagbag.import_errors == {}, (
        f"DAG import errors: {dagbag.import_errors}"
    )


def test_dag_count(dagbag):
    """Expect exactly 5 DAGs."""
    assert len(dagbag.dags) == 5, f"Expected 5 DAGs, found {len(dagbag.dags)}"


@pytest.mark.parametrize(
    "dag_id",
    [
        "01_taskflow_api",
        "02_dynamic_task_mapping",
        "03_sensors_and_hooks",
        "04_branching_and_xcoms",
        "05_data_quality_checks",
    ],
)
def test_dag_exists(dagbag, dag_id):
    """Each expected DAG must be present."""
    assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found"


@pytest.mark.parametrize(
    "dag_id",
    [
        "01_taskflow_api",
        "02_dynamic_task_mapping",
        "03_sensors_and_hooks",
        "04_branching_and_xcoms",
        "05_data_quality_checks",
    ],
)
def test_dag_has_no_cycles(dagbag, dag_id):
    """DAGs must be acyclic (no cycles in task dependencies)."""
    dag = dagbag.get_dag(dag_id)
    assert dag.test_cycle() is False, f"Cycle detected in DAG '{dag_id}'"


@pytest.mark.parametrize(
    "dag_id",
    [
        "01_taskflow_api",
        "02_dynamic_task_mapping",
        "03_sensors_and_hooks",
        "04_branching_and_xcoms",
        "05_data_quality_checks",
    ],
)
def test_dag_has_tags(dagbag, dag_id):
    """All DAGs must have at least one tag for discoverability."""
    dag = dagbag.get_dag(dag_id)
    assert dag.tags, f"DAG '{dag_id}' has no tags"


@pytest.mark.parametrize(
    "dag_id",
    [
        "01_taskflow_api",
        "02_dynamic_task_mapping",
        "03_sensors_and_hooks",
        "04_branching_and_xcoms",
        "05_data_quality_checks",
    ],
)
def test_dag_schedule_interval_is_set(dagbag, dag_id):
    """All DAGs must have a schedule interval defined."""
    dag = dagbag.get_dag(dag_id)
    assert dag.schedule_interval is not None, f"DAG '{dag_id}' has no schedule_interval"
