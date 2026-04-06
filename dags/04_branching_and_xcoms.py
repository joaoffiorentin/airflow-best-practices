"""
DAG 04: Branching and XComs
Demonstrates BranchPythonOperator and XCom for inter-task communication.
Pipeline: check data quality → branch on result → send alert or proceed.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {"owner": "airflow", "retries": 0}

QUALITY_THRESHOLD = 0.95  # 95% pass rate required


@dag(
    dag_id="04_branching_and_xcoms",
    default_args=DEFAULT_ARGS,
    description="Branching + XComs: data quality gate with conditional routing",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["demo", "branching", "xcoms"],
)
def branching_and_xcoms_demo():

    @task()
    def fetch_data() -> dict:
        """Simulates fetching a dataset from an external source."""
        import random
        total_records = random.randint(800, 1000)
        null_count = random.randint(0, 80)
        duplicate_count = random.randint(0, 30)
        valid_count = total_records - null_count - duplicate_count

        return {
            "total_records": total_records,
            "valid_records": valid_count,
            "null_count": null_count,
            "duplicate_count": duplicate_count,
            "source": "simulated_api",
        }

    @task()
    def run_quality_checks(data: dict) -> dict:
        """Computes quality metrics and pushes to XCom."""
        total = data["total_records"]
        valid = data["valid_records"]
        pass_rate = valid / total if total > 0 else 0

        quality_report = {
            **data,
            "pass_rate": round(pass_rate, 4),
            "pass_rate_pct": f"{pass_rate * 100:.1f}%",
            "quality_status": "PASS" if pass_rate >= QUALITY_THRESHOLD else "FAIL",
        }
        print(f"Quality check: {quality_report['quality_status']} ({quality_report['pass_rate_pct']})")
        return quality_report

    def route_on_quality(**context):
        """Branching function: routes to alert or process based on quality."""
        report = context["ti"].xcom_pull(task_ids="run_quality_checks")
        if report["quality_status"] == "FAIL":
            return "send_quality_alert"
        return "process_data"

    branch = BranchPythonOperator(
        task_id="branch_on_quality",
        python_callable=route_on_quality,
    )

    @task(task_id="send_quality_alert")
    def send_quality_alert(**context) -> None:
        """Handles the FAIL branch: sends an alert."""
        report = context["ti"].xcom_pull(task_ids="run_quality_checks")
        print(f"ALERT: Data quality check FAILED!")
        print(f"  Pass rate: {report['pass_rate_pct']} (threshold: {QUALITY_THRESHOLD * 100}%)")
        print(f"  Nulls: {report['null_count']} | Duplicates: {report['duplicate_count']}")

    @task(task_id="process_data")
    def process_data(**context) -> None:
        """Handles the PASS branch: processes the data."""
        report = context["ti"].xcom_pull(task_ids="run_quality_checks")
        print(f"Processing {report['valid_records']} valid records...")
        print(f"Quality: {report['pass_rate_pct']} — proceeding.")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    fetched = fetch_data()
    quality_report = run_quality_checks(fetched)
    quality_report >> branch
    branch >> [send_quality_alert(), process_data()] >> end


branching_and_xcoms_demo()
