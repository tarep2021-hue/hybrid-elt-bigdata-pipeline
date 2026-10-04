"""
api.py — واجهة API الموحدة للتشغيل والاختبار (FastAPI)
Big Data - Phase 2: Unified API Gateway & Evaluation Interface

المسارات المحددة في الوثيقة الرسمية للمشروع:
- GET  /health
- POST /ingest
- POST /indexes
- GET  /queries
- GET  /queries/{name}
- GET  /aggregations
- GET  /aggregations/{name}
- POST /refresh-mv
- GET  /jobs
- POST /jobs/{name}/run
"""

import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DB_NAME, MONGO_URI
from src.mongo_setup import get_database
from src.elt_pipeline import run as run_midterm_pipeline
from src.indexes import create_phase2_indexes, list_collection_indexes, drop_phase2_indexes
from src.queries import execute_query, list_available_queries, QUERY_CATALOG
from src.aggregations import run_aggregation, list_available_aggregations, AGGREGATION_CATALOG
from src.materialized_views import MaterializedViewManager, VIEW_DAILY_SALES, VIEW_TOP_PRODUCTS
from src.scheduler import get_scheduler, JobScheduler
from src.explain_benchmarks import run_explain_benchmarks


# ─── Lifespan Context Manager ───
@asynccontextmanager
async def lifespan(app: FastAPI):
    # بدء خيط الجدولة في الخلفية عند الإقلاع
    scheduler = get_scheduler()
    scheduler.start_scheduler_thread()
    print("  [API] Antigravity FastAPI Gateway ready on http://localhost:8000")
    print("  [API] Interactive Swagger documentation: http://localhost:8000/docs")
    yield
    # إيقاف خيط الجدولة عند الإغلاق
    scheduler.stop_scheduler_thread()


app = FastAPI(
    title="Big Data - Phase 2 Unified API",
    description="واجهة برمجة التطبيقات الموحدة لتشغيل واختبار خط البيانات الضخمة (جامعة الرازي)",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Pydantic Models للطلبات ───
class IngestRequest(BaseModel):
    file_path: Optional[str] = Field(
        default="data/sample_orders.csv",
        description="مسار ملف CSV المراد معالجته (نسبي أو مطلق)"
    )
    reports_dir: Optional[str] = Field(
        default="reports",
        description="مجلد حفظ تقارير المعالجة والاتساق"
    )


class RefreshMVRequest(BaseModel):
    view_name: Optional[str] = Field(
        default="all",
        description="اسم العرض المراد تحديثه ('all' أو 'daily_sales_summary' أو 'top_products_summary')"
    )
    mode: Optional[str] = Field(
        default="incremental",
        description="وضع التحديث: 'incremental' (تزايدي عبر الـ Watermark) أو 'full' (إعادة بناء كاملة)"
    )


# ─── Endpoints ───

@app.get("/", include_in_schema=False)
async def root():
    """تحويل تلقائي إلى وثائق Swagger التفاعلية."""
    return RedirectResponse(url="/docs")


@app.get(
    "/health",
    summary="فحص صحة النظام وحالة الاتصال بقاعدة البيانات",
    tags=["System Health"]
)
async def health_check():
    """
    إرجاع حالة الاتصال بـ MongoDB وتعداد السجلات في كافة المجموعات والعروض المادية.
    """
    try:
        db = get_database()
        # اختبار ping للتأكد من استجابة الخادم
        db.command("ping")
        col_names = db.list_collection_names()
        counts = {c: db[c].count_documents({}) for c in col_names}

        return {
            "status": "healthy",
            "database": DB_NAME,
            "mongodb_connected": True,
            "collections_count": len(col_names),
            "collections": counts,
            "server_timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "database": DB_NAME,
                "mongodb_connected": False,
                "error": str(exc),
                "server_timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )


@app.post(
    "/ingest",
    summary="تشغيل خط الإدخال والمعالجة للمشروع النصفي (Ingestion Pipeline)",
    tags=["Pipeline Ingestion"]
)
async def ingest_data(request: Optional[IngestRequest] = None):
    """
    تشغيل نفس بوابة الإدخال وخط معالجة البيانات (ELT Pipeline) المنفذة في المشروع النصفي
    دون أي تعديل على كود المشروع النصفي، مع استرجاع تقرير المقاييس والاتساق.
    """
    req = request or IngestRequest()
    file_path_str = req.file_path or "data/sample_orders.csv"
    file_path = Path(file_path_str)

    if not file_path.is_absolute():
        file_path = PROJECT_ROOT / file_path

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ملف الإدخال غير موجود: {file_path_str}"
        )

    try:
        report = run_midterm_pipeline(
            file_path=str(file_path),
            reports_dir=str(PROJECT_ROOT / req.reports_dir)
        )
        return {
            "status": "SUCCESS",
            "message": "تمت معالجة البيانات عبر خط الإدخال بنجاح",
            "pipeline_report": report,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تشغيل خط البيانات: {str(exc)}"
        )


@app.post(
    "/indexes",
    summary="إنشاء فهارس المرحلة الثانية المحسنة لأداء الاستعلامات",
    tags=["Indexes & Performance"]
)
async def create_indexes():
    """
    إنشاء الفهارس الـ 3 المطلوبة على الأقل (بما فيها Compound Index)
    لتسريع استعلامات الطلبات والعملاء والمنتجات.
    """
    try:
        result = create_phase2_indexes()
        return result
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل إنشاء الفهارس: {str(exc)}"
        )


@app.get(
    "/queries",
    summary="قائمة الاستعلامات الخمسة المتاحة مع وصفها ومعاملاتها",
    tags=["Queries & Explain"]
)
async def list_queries():
    """
    عرض قائمة بالاستعلامات العملية الـ 5 المنفذة في المشروع.
    """
    return {
        "count": len(QUERY_CATALOG),
        "queries": list_available_queries()
    }


@app.get(
    "/queries/{name}",
    summary="تنفيذ استعلام محدد أو استخراج خطة التنفيذ Explain",
    tags=["Queries & Explain"]
)
async def run_query(
    name: str,
    explain: bool = Query(False, description="إذا كانت True يتم إرجاع تحليل explain('executionStats')"),
    limit: Optional[int] = Query(20, description="الحد الأقصى للسجلات المعادة"),
    status: Optional[str] = Query(None, description="حالة الطلب (للاستعلام 1)"),
    start_date: Optional[str] = Query(None, description="تاريخ البداية YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="تاريخ النهاية YYYY-MM-DD"),
    phone: Optional[str] = Query(None, description="رقم هاتف العميل (للاستعلام 2)"),
    city: Optional[str] = Query(None, description="المدينة (للاستعلام 3)"),
    min_amount: Optional[float] = Query(None, description="الحد الأدنى لقيمة الطلب (للاستعلام 3)"),
    sku: Optional[str] = Query(None, description="رمز صنف المنتج (للاستعلام 4)"),
):
    """
    تشغيل استعلام محدد بالاسم، مع إمكانية تمرير معاملات التصفية وتفعيل Explain ExecutionStats.
    """
    if name not in QUERY_CATALOG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"الاستعلام '{name}' غير موجود. الاستعلامات المتاحة: {list(QUERY_CATALOG.keys())}"
        )

    # تجميع المعاملات المدخلة
    params: Dict[str, Any] = {"limit": limit}
    if status is not None:
        params["status"] = status
    if start_date is not None:
        params["start_date"] = start_date
    if end_date is not None:
        params["end_date"] = end_date
    if phone is not None:
        params["phone"] = phone
    if city is not None:
        params["city"] = city
    if min_amount is not None:
        params["min_amount"] = min_amount
    if sku is not None:
        params["sku"] = sku

    try:
        res = execute_query(query_name=name, params=params, explain=explain)
        return res
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تنفيذ الاستعلام '{name}': {str(exc)}"
        )


@app.get(
    "/aggregations",
    summary="قائمة تقارير التجميع الخمسة المتاحة",
    tags=["Aggregations & Reports"]
)
async def list_aggregations():
    """
    عرض قائمة بتقارير التجميع الـ 5 المنفذة في المشروع.
    """
    return {
        "count": len(AGGREGATION_CATALOG),
        "aggregations": list_available_aggregations()
    }


@app.get(
    "/aggregations/{name}",
    summary="تشغيل تقرير تجميع محدد واسترجاع البيانات التحليلية",
    tags=["Aggregations & Reports"]
)
async def run_aggregation_report(
    name: str,
    limit: Optional[int] = Query(10, description="الحد الأقصى لعدد النتائج المعادة"),
    min_orders: Optional[int] = Query(1, description="الحد الأدنى لعدد الطلبات (لتقرير المبيعات حسب المدينة)"),
    period_format: Optional[str] = Query("monthly", description="تنسيق الفترة: 'monthly' أو 'daily'"),
):
    """
    تشغيل تقرير تجميع محدد وإرجاع النتائج الإحصائية.
    """
    if name not in AGGREGATION_CATALOG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"تقرير التجميع '{name}' غير موجود. التقارير المتاحة: {list(AGGREGATION_CATALOG.keys())}"
        )

    params: Dict[str, Any] = {
        "limit": limit,
        "min_orders": min_orders,
        "period_format": period_format,
    }

    try:
        report_data = run_aggregation(report_name=name, params=params)
        return report_data
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تشغيل تقرير التجميع '{name}': {str(exc)}"
        )


@app.post(
    "/refresh-mv",
    summary="تحديث العروض المادية تزايدياً أو كلياً (Materialized Views Refresh)",
    tags=["Materialized Views"]
)
async def refresh_materialized_views(request: Optional[RefreshMVRequest] = None):
    """
    تحديث العروض المادية (daily_sales_summary, top_products_summary)
    باستخدام آلية التحديث التزايدي المعتمدة على الـ Watermark لتفادي إعادة الحساب الكامل.
    """
    req = request or RefreshMVRequest()
    mgr = MaterializedViewManager()
    mode = req.mode or "incremental"
    view_name = (req.view_name or "all").lower()

    try:
        if view_name == "all":
            return mgr.refresh_all(mode=mode)
        elif view_name == VIEW_DAILY_SALES:
            return mgr.refresh_daily_sales(mode=mode)
        elif view_name == VIEW_TOP_PRODUCTS:
            return mgr.refresh_top_products(mode=mode)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"اسم العرض المادي '{view_name}' غير معروف. الخيارات: 'all', '{VIEW_DAILY_SALES}', '{VIEW_TOP_PRODUCTS}'"
            )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تحديث العرض المادي: {str(exc)}"
        )


@app.get(
    "/materialized-views/{name}",
    summary="استعراض بيانات العرض المادي مباشرة من MongoDB",
    tags=["Materialized Views"]
)
async def get_view_records(
    name: str,
    limit: Optional[int] = Query(50, description="عدد السجلات المعروضة")
):
    """عرض السجلات المحفوظة مسبقاً داخل الـ Materialized View في قاعدة البيانات."""
    mgr = MaterializedViewManager()
    try:
        data = mgr.get_view_data(name, limit=limit)
        return {
            "view_name": name,
            "records_count": len(data),
            "data": data,
        }
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(val_err))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@app.get(
    "/jobs",
    summary="قائمة المهام المجدولة وسجلات التنفيذ السابقة",
    tags=["Scheduled Jobs"]
)
async def list_jobs_info():
    """
    عرض قائمة بالمهام المجدولة دورياً وفترات تشغيلها وآخر حالة تنفيذ.
    """
    scheduler = get_scheduler()
    return {
        "scheduled_jobs": scheduler.list_jobs(),
        "recent_logs": scheduler.get_execution_logs(limit=10),
    }


@app.post(
    "/jobs/{name}/run",
    summary="التشغيل اليدوي الفوري لمهمة محددة (Manual Trigger)",
    tags=["Scheduled Jobs"]
)
async def run_scheduled_job_manually(name: str):
    """
    تشغيل يدوي فوري لمهمة محددة لتمكين لجان التقييم والمناقشة من اختبارها آلياً.
    """
    scheduler = get_scheduler()
    try:
        result = scheduler.run_job(job_name=name, trigger_type="MANUAL")
        return result
    except ValueError as val_err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(val_err)
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تنفيذ المهمة '{name}': {str(exc)}"
        )


@app.get(
    "/explain-benchmarks",
    summary="تنفيذ مقارنة Explain ExecutionStats (قبل وبعد الفهارس)",
    tags=["Indexes & Performance"]
)
async def explain_benchmarks_endpoint():
    """
    تشغيل تقييم شامل لـ Explain ExecutionStats لـ 3 استعلامات قبل وبعد الفهارس
    وحفظ التقرير وإرجاع جدول المقارنة.
    """
    try:
        rep = run_explain_benchmarks(reports_dir=str(PROJECT_ROOT / "reports"))
        return rep
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تشغيل اختبار Explain: {str(exc)}"
        )
