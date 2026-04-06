"""
DAG 02: Dynamic Task Mapping
Demonstrates .expand() to process a variable list of items in parallel.
"""
from __future__ import annotations

from datetime import timedelta

import httpx
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

STATES = ["SP", "RJ", "SC", "RS", "MG", "BA", "PR", "GO"]


@dag(
    dag_id="02_dynamic_task_mapping",
    default_args=DEFAULT_ARGS,
    description="Dynamic task mapping: fetch IBGE data for multiple states in parallel",
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    tags=["demo", "dynamic-mapping"],
)
def dynamic_task_mapping_demo():

    @task()
    def get_state_codes() -> list[str]:
        """Returns the list of state codes to process. Could come from a DB or config."""
        return STATES

    @task()
    def fetch_municipalities(state_code: str) -> dict:
        """Fetches municipality count for a given state from IBGE API."""
        response = httpx.get(
            f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{state_code}/municipios",
            timeout=15,
        )
        response.raise_for_status()
        municipalities = response.json()
        return {
            "state_code": state_code,
            "municipality_count": len(municipalities),
            "municipalities": [m["nome"] for m in municipalities[:5]],  # preview
        }

    @task()
    def generate_report(state_results: list[dict]) -> str:
        """Aggregates results from all states into a summary report."""
        total = sum(r["municipality_count"] for r in state_results)
        report_lines = [
            "=== IBGE Municipality Report ===",
            f"States processed: {len(state_results)}",
            f"Total municipalities: {total}",
            "",
            "Breakdown by state:",
        ]
        for r in sorted(state_results, key=lambda x: x["municipality_count"], reverse=True):
            report_lines.append(f"  {r['state_code']}: {r['municipality_count']} municipalities")

        report = "\n".join(report_lines)
        print(report)
        return report

    states = get_state_codes()
    results = fetch_municipalities.expand(state_code=states)
    generate_report(results)


dynamic_task_mapping_demo()
