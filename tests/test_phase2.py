"""
test_phase2.py — اختبارات المرحلة الثانية (Phase 2 Integration & Unit Tests)
مقرر البيانات الضخمة (العملي)
"""

import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api import app
from src.mongo_setup import get_database
from src.indexes import create_phase2_indexes, INDEX_DEFINITIONS
from src.queries import execute_query, QUERY_CATALOG
from src.aggregations import run_aggregation, AGGREGATION_CATALOG
from src.materialized_views import MaterializedViewManager, VIEW_DAILY_SALES, VIEW_TOP_PRODUCTS
from src.scheduler import get_scheduler


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="session")
def db():
    return get_database()


# ─── 1. اختبارات الفهارس ───
def test_indexes_creation(db):
    result = create_phase2_indexes(db)
    assert result["status"] == "SUCCESS"
    assert result["total_indexes"] >= 3

    # التأكد من وجود فهرس مركب واحد على الأقل
    col_indexes = list(db["orders_validated"].list_indexes())
    compound_indexes = [idx for idx in col_indexes if len(idx.get("key", {})) > 1]
    assert len(compound_indexes) >= 1, "يجب وجود فهرس مركب واحد على الأقل"


# ─── 2. اختبارات الاستعلامات الخمسة ───
@pytest.mark.parametrize("query_name", list(QUERY_CATALOG.keys()))
def test_queries_execution(query_name, db):
    res = execute_query(query_name, params={"limit": 5}, explain=False, db=db)
    assert res["query_name"] == query_name
    assert "records" in res
    assert isinstance(res["records"], list)


def test_query_explain(db):
    res = execute_query("orders_by_status_and_date", params={"limit": 10}, explain=True, db=db)
    assert "execution_metrics" in res
    metrics = res["execution_metrics"]
    assert "execution_time_ms" in metrics
    assert "total_docs_examined" in metrics
    assert "winning_stage" in metrics


# ─── 3. اختبارات التجميعات الخمسة ───
@pytest.mark.parametrize("rep_name", list(AGGREGATION_CATALOG.keys()))
def test_aggregations_execution(rep_name, db):
    res = run_aggregation(rep_name, params={"limit": 5}, db=db)
    assert res["report_name"] == rep_name
    assert "data" in res
    assert len(res["data"]) > 0


# ─── 4. اختبارات العروض المادية والتحديث التزايدي ───
def test_materialized_views_refresh(db):
    mgr = MaterializedViewManager(db)
    # تحديث كلي
    full_res = mgr.refresh_all(mode="full")
    assert full_res["status"] == "SUCCESS"
    assert db[VIEW_DAILY_SALES].count_documents({}) > 0
    assert db[VIEW_TOP_PRODUCTS].count_documents({}) > 0

    # تحديث تزايدي (عند عدم وجود جديد، يجب أن يرجع UP_TO_DATE)
    inc_res = mgr.refresh_all(mode="incremental")
    assert inc_res["status"] == "SUCCESS"
    assert inc_res["views"][0]["status"] in ["UP_TO_DATE", "SUCCESS"]


# ─── 5. اختبارات المهام المجدولة ───
def test_scheduled_jobs_manual_run():
    scheduler = get_scheduler()
    res1 = scheduler.run_job("refresh_materialized_views", trigger_type="MANUAL")
    assert res1["status"] == "SUCCESS"
    assert res1["duration_seconds"] >= 0

    res2 = scheduler.run_job("generate_periodic_kpi_report", trigger_type="MANUAL")
    assert res2["status"] == "SUCCESS"


# ─── 6. اختبارات مسارات API ───
def test_api_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["mongodb_connected"] is True


def test_api_queries_list(client):
    response = client.get("/queries")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 5


def test_api_run_query(client):
    response = client.get("/queries/orders_by_status_and_date?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "records" in data


def test_api_run_query_explain(client):
    response = client.get("/queries/orders_by_status_and_date?explain=true")
    assert response.status_code == 200
    data = response.json()
    assert "execution_metrics" in data


def test_api_aggregations_list(client):
    response = client.get("/aggregations")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 5


def test_api_run_aggregation(client):
    response = client.get("/aggregations/sales_by_city")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data


def test_api_refresh_mv(client):
    response = client.post("/refresh-mv", json={"view_name": "all", "mode": "incremental"})
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"


def test_api_jobs_list(client):
    response = client.get("/jobs")
    assert response.status_code == 200
    data = response.json()
    assert "scheduled_jobs" in data
    assert len(data["scheduled_jobs"]) >= 2


def test_api_run_job(client):
    response = client.post("/jobs/refresh_materialized_views/run")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"
