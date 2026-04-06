# Airflow Best Practices

Collection of Apache Airflow 2.x DAGs demonstrating modern patterns and best practices. Each DAG is a self-contained, working example of a specific concept.

## DAGs

| # | DAG ID | Pattern Demonstrated |
|---|--------|----------------------|
| 01 | `01_taskflow_api` | `@task` decorator, typed XComs, `expand()` |
| 02 | `02_dynamic_task_mapping` | `.expand()` for parallel processing |
| 03 | `03_sensors_and_hooks` | `HttpSensor` + custom `BaseHook` |
| 04 | `04_branching_and_xcoms` | `BranchPythonOperator` + XCom pull |
| 05 | `05_data_quality_checks` | `SQLCheckOperator`, `SQLValueCheckOperator` |

## Quick Start

```bash
git clone https://github.com/your-username/airflow-best-practices
cd airflow-best-practices
cp .env.example .env
docker compose up -d
```

Airflow UI: http://localhost:8080 (admin/admin)

## Running Tests

```bash
pip install apache-airflow pytest
pytest tests/ -v
```

## Pattern Details

### 01 — TaskFlow API
Uses the modern `@task` decorator instead of `PythonOperator`. Fetches weather for 5 Brazilian cities, transforms, and saves to a JSON file. Demonstrates typed XCom passing between tasks.

```python
@task()
def fetch_weather(city: str) -> dict:
    ...

raw_data = fetch_weather.expand(city=CITIES)
```

### 02 — Dynamic Task Mapping
Uses `.expand()` to generate one task instance per item in a list. Fetches IBGE municipality data for 8 Brazilian states in parallel — the list could come from a database or config file.

```python
results = fetch_municipalities.expand(state_code=states)
```

### 03 — Sensors and Custom Hooks
Demonstrates `HttpSensor` (waits for an API to respond before proceeding) and a custom `WeatherHook` that encapsulates all API interaction logic, reusable across DAGs.

### 04 — Branching and XComs
Uses `BranchPythonOperator` to route execution based on a data quality check. Passes data between tasks via XComs. Uses `trigger_rule="none_failed_min_one_success"` on the join task.

### 05 — Data Quality with SQL Operators
Uses built-in SQL operators to enforce quality rules without writing custom Python:
- `SQLCheckOperator` — asserts a SQL expression returns truthy
- `SQLValueCheckOperator` — checks a value is within a tolerance
- `SQLThresholdCheckOperator` — checks a value is within min/max bounds

## Project Structure

```
dags/
├── 01_taskflow_api.py
├── 02_dynamic_task_mapping.py
├── 03_sensors_and_hooks.py
├── 04_branching_and_xcoms.py
└── 05_data_quality_checks.py
plugins/
└── hooks/
    └── weather_hook.py       # Reusable custom hook
tests/
└── test_dag_integrity.py     # Validates all DAGs load correctly
```
