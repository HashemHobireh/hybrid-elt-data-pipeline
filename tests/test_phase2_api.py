"""
tests/test_phase2_api.py
------------------------
اختبارات الـ API الموحدة (FastAPI) باستخدام TestClient.
تتحقق من وجود جميع المسارات واستجابتها بصيغة JSON ومعالجة الأخطاء 404.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from fastapi.testclient import TestClient  # noqa: E402
from api import app  # noqa: E402

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "mongodb" in data
    assert "collection_counts" in data


def test_list_queries_endpoint():
    response = client.get("/queries")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 5
    query_names = {item["name"] for item in data}
    assert "customer_order_history" in query_names
    assert "city_confirmed_orders" in query_names
    assert "date_range_orders" in query_names
    assert "unpaid_orders_lookup" in query_names
    assert "quarantine_by_error" in query_names


def test_get_unknown_query_returns_404():
    response = client.get("/queries/non_existing_query_xyz")
    assert response.status_code == 404
    assert "detail" in response.json()


def test_list_aggregations_endpoint():
    response = client.get("/aggregations")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 5
    names = {item["name"] for item in data}
    assert "sales_by_city" in names
    assert "top_products" in names
    assert "top_customers" in names
    assert "monthly_sales_trend" in names
    assert "orders_distribution_by_status" in names


def test_get_unknown_aggregation_returns_404():
    response = client.get("/aggregations/unknown_agg_report")
    assert response.status_code == 404


def test_list_materialized_views_endpoint():
    response = client.get("/materialized-views")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2
    view_names = {v["name"] for v in data}
    assert "daily_sales_summary" in view_names
    assert "top_products_summary" in view_names


def test_get_jobs_endpoint():
    response = client.get("/jobs")
    assert response.status_code == 200
    data = response.json()
    assert "jobs" in data
    assert "recent_history" in data
    job_names = {j["job_name"] for j in data["jobs"]}
    assert "refresh_materialized_views" in job_names
    assert "generate_periodic_report" in job_names


def test_run_unknown_job_returns_404():
    response = client.post("/jobs/unknown_fake_job/run")
    assert response.status_code == 404


def test_ingest_endpoint_file_not_found():
    response = client.post("/ingest", json={"file_path": "non_existing_file_999.csv"})
    assert response.status_code == 404
