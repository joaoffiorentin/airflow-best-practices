"""
DAG 05: Data Quality with SQL Operators
Demonstrates SQLCheckOperator and SQLValueCheckOperator
to enforce data quality rules directly in the warehouse.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
    SQLThresholdCheckOperator,
)
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}
CONN_ID = "postgres_dw"


@dag(
    dag_id="05_data_quality_checks",
    default_args=DEFAULT_ARGS,
    description="SQL-based data quality checks using SQLCheckOperator",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["demo", "data-quality", "sql-checks"],
)
def data_quality_checks_demo():

    # Check 1: Table is not empty
    check_table_not_empty = SQLCheckOperator(
        task_id="check_orders_not_empty",
        conn_id=CONN_ID,
        sql="SELECT COUNT(*) FROM fct_orders WHERE purchase_date = CURRENT_DATE - 1",
    )

    # Check 2: No nulls in critical column
    check_no_null_customer = SQLCheckOperator(
        task_id="check_no_null_customer_id",
        conn_id=CONN_ID,
        sql="""
            SELECT COUNT(*) = 0
            FROM fct_orders
            WHERE customer_id IS NULL
              AND purchase_date = CURRENT_DATE - 1
        """,
    )

    # Check 3: Order total must be positive
    check_positive_totals = SQLCheckOperator(
        task_id="check_positive_order_totals",
        conn_id=CONN_ID,
        sql="""
            SELECT COUNT(*) = 0
            FROM fct_orders
            WHERE order_total_value <= 0
              AND purchase_date = CURRENT_DATE - 1
        """,
    )

    # Check 4: Verify row count is within expected range
    check_row_count_threshold = SQLThresholdCheckOperator(
        task_id="check_daily_order_volume",
        conn_id=CONN_ID,
        sql="SELECT COUNT(*) FROM fct_orders WHERE purchase_date = CURRENT_DATE - 1",
        min_threshold=100,
        max_threshold=50_000,
    )

    # Check 5: Cancellation rate should not exceed 20%
    check_cancellation_rate = SQLValueCheckOperator(
        task_id="check_cancellation_rate",
        conn_id=CONN_ID,
        sql="""
            SELECT
                ROUND(
                    100.0 * SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
                    / COUNT(*),
                    2
                )
            FROM fct_orders
            WHERE purchase_date = CURRENT_DATE - 1
        """,
        pass_value=20,
        tolerance=0.05,
    )

    @task()
    def log_quality_pass() -> None:
        print("All data quality checks PASSED. Pipeline may proceed.")

    (
        [
            check_table_not_empty,
            check_no_null_customer,
            check_positive_totals,
            check_row_count_threshold,
            check_cancellation_rate,
        ]
        >> log_quality_pass()
    )


data_quality_checks_demo()
