"""
scheduler.py — نظام إدارة وجدولة المهام الدورية (Phase 2)
Big Data - Phase 2: Scheduled Jobs & Task Automation

المتطلبات:
1. إنشاء مهمتين مجدولتين على الأقل:
   - refresh_materialized_views_job: تحديث تزايدي للعروض المادية (Materialized Views)
   - periodic_kpi_summary_job: توليد تقرير دوري لمؤشرات الأداء الرئيسية ومقاييس الجودة
2. جدول زمني محدد لكل مهمة مع إمكانية التشغيل اليدوي الفوري (Manual Trigger) للاختبار.
3. تسجيل وتوثيق كامل لكل عملية تنفيذ في MongoDB:
   - وقت البداية ووقت النهاية
   - مدة التنفيذ بالثواني
   - حالة النجاح أو الفشل (SUCCESS / FAILED)
   - نوع المشغل (MANUAL / SCHEDULED)
   - تفاصيل ومخرجات العملية أو رسالة الخطأ
"""

import sys
import time
import uuid
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from bson import ObjectId

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mongo_setup import get_database
from src.materialized_views import MaterializedViewManager
from src.aggregations import run_aggregation

JOB_LOGS_COLLECTION = "job_execution_logs"
PERIODIC_REPORTS_COLLECTION = "periodic_reports"


def _clean_doc(doc: Any) -> Any:
    if isinstance(doc, dict):
        return {k: _clean_doc(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [_clean_doc(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    return doc


class JobScheduler:
    """مدير المهام المجدولة والتشغيل اليدوي وتوثيق السجلات."""

    def __init__(self, db=None):
        self.db = get_database() if db is None else db
        self.logs_col = self.db[JOB_LOGS_COLLECTION]
        self.mv_manager = MaterializedViewManager(self.db)
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._jobs_registry: Dict[str, Dict[str, Any]] = {}
        self._register_default_jobs()

    def _register_default_jobs(self) -> None:
        """تسجيل المهام المحددة افتراضياً في النظام."""
        self._jobs_registry["refresh_materialized_views"] = {
            "name": "refresh_materialized_views",
            "title": "المهمة 1: التحديث التزايدي للعروض المادية (Materialized Views Refresh)",
            "description": "تحديث تزايدي ذكي لملخص المبيعات اليومية وملخص المنتجات دون إعادة حساب السجلات القديمة.",
            "interval_seconds": 300,  # كل 5 دقائق
            "func": self._execute_refresh_mv,
            "last_run": None,
            "last_status": None,
            "next_run": time.time() + 300,
        }

        self._jobs_registry["generate_periodic_kpi_report"] = {
            "name": "generate_periodic_kpi_report",
            "title": "المهمة 2: إنشاء تقرير دوري لمؤشرات الأداء ومقاييس النظام (Periodic KPI Report)",
            "description": "حساب إحصائيات المبيعات النشطة، توزيع الحالات، وتوليد لقطة إحصائية دورية وحفظها في قاعدة البيانات.",
            "interval_seconds": 600,  # كل 10 دقائق
            "func": self._execute_kpi_report,
            "last_run": None,
            "last_status": None,
            "next_run": time.time() + 600,
        }

    def _execute_refresh_mv(self) -> Dict[str, Any]:
        """دالة تنفيذ تحديث العروض المادية."""
        return self.mv_manager.refresh_all(mode="incremental")

    def _execute_kpi_report(self) -> Dict[str, Any]:
        """دالة توليد وحفظ التقرير الدوري."""
        status_dist = run_aggregation("order_status_distribution", db=self.db)
        top_prods = run_aggregation("top_products", {"limit": 5}, db=self.db)
        city_sales = run_aggregation("sales_by_city", {"min_orders": 1}, db=self.db)

        snapshot = {
            "report_id": f"kpi-{uuid.uuid4().hex[:8]}",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status_distribution": status_dist["data"],
            "top_5_products": top_prods["data"],
            "top_cities_summary": city_sales["data"][:5],
            "total_validated_records": self.db["orders_validated"].count_documents({}),
        }

        self.db[PERIODIC_REPORTS_COLLECTION].insert_one(snapshot)
        return {
            "report_id": snapshot["report_id"],
            "generated_at": snapshot["generated_at"],
            "status_distribution_count": len(status_dist["data"]),
            "top_products_count": len(top_prods["data"]),
            "saved_to_collection": PERIODIC_REPORTS_COLLECTION,
        }

    def run_job(self, job_name: str, trigger_type: str = "MANUAL") -> Dict[str, Any]:
        """
        تشغيل مهمة محددة فورياً وتسجيل دورة حياتها بالكامل في قاعدة البيانات.

        Args:
            job_name: اسم المهمة
            trigger_type: نوع المشغل ('MANUAL' أو 'SCHEDULED')

        Returns:
            Dict: تقرير نتيجة التنفيذ والوقت والحالة
        """
        if job_name not in self._jobs_registry:
            raise ValueError(f"Unknown job '{job_name}'. Available: {list(self._jobs_registry.keys())}")

        job_info = self._jobs_registry[job_name]
        start_dt = datetime.now(timezone.utc)
        start_perf = time.perf_counter()
        job_id = f"job-{uuid.uuid4().hex[:8]}"

        status = "FAILED"
        result = None
        error_msg = None

        try:
            result = job_info["func"]()
            status = "SUCCESS"
        except Exception as exc:
            status = "FAILED"
            error_msg = str(exc)

        end_dt = datetime.now(timezone.utc)
        duration_sec = round(time.perf_counter() - start_perf, 4)

        log_record = {
            "job_id": job_id,
            "job_name": job_name,
            "title": job_info["title"],
            "trigger_type": trigger_type,
            "start_time": start_dt.isoformat(),
            "end_time": end_dt.isoformat(),
            "duration_seconds": duration_sec,
            "status": status,
            "result": result,
            "error_message": error_msg,
        }

        # حفظ السجل في MongoDB
        self.logs_col.insert_one(log_record)

        # تحديث بيانات التعريف في الذاكرة
        job_info["last_run"] = end_dt.isoformat()
        job_info["last_status"] = status
        job_info["next_run"] = time.time() + job_info["interval_seconds"]

        return _clean_doc(log_record)

    def list_jobs(self) -> List[Dict[str, Any]]:
        """قائمة بالمهام المسجلة وجدولها وآخر حالة تشغيل."""
        jobs_list = []
        for name, meta in self._jobs_registry.items():
            last_log = self.logs_col.find_one({"job_name": name}, sort=[("start_time", -1)])
            jobs_list.append({
                "job_name": name,
                "title": meta["title"],
                "description": meta["description"],
                "interval_seconds": meta["interval_seconds"],
                "last_run": meta["last_run"] or (last_log["start_time"] if last_log else None),
                "last_status": meta["last_status"] or (last_log["status"] if last_log else "NEVER_RUN"),
                "next_run_in_seconds": max(0, int(meta["next_run"] - time.time())),
            })
        return jobs_list

    def get_execution_logs(self, limit: int = 20, job_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """استرجاع سجلات تنفيذ المهام السابقة."""
        query = {"job_name": job_name} if job_name else {}
        cursor = self.logs_col.find(query).sort("start_time", -1).limit(limit)
        return [_clean_doc(doc) for doc in cursor]

    def start_scheduler_thread(self) -> None:
        """تشغيل مؤشر الجدولة في خيط خلفي (Background Thread)."""
        if self._running:
            return

        self._running = True

        def _loop():
            while self._running:
                now = time.time()
                for job_name, job_info in list(self._jobs_registry.items()):
                    if now >= job_info["next_run"]:
                        try:
                            self.run_job(job_name, trigger_type="SCHEDULED")
                        except Exception as e:
                            print(f"[Scheduler Error] Job {job_name} failed: {e}")
                time.sleep(10)

        self._thread = threading.Thread(target=_loop, daemon=True, name="JobSchedulerThread")
        self._thread.start()
        print("  [Scheduler] Background Job Scheduler started successfully.")

    def stop_scheduler_thread(self) -> None:
        """إيقاف خيط الجدولة الخلفي."""
        self._running = False


# كائن افتراضي موحد للمشروع
_scheduler_instance: Optional[JobScheduler] = None


def get_scheduler() -> JobScheduler:
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = JobScheduler()
    return _scheduler_instance


if __name__ == "__main__":
    sched = get_scheduler()
    print("Testing Job 1...")
    res1 = sched.run_job("refresh_materialized_views", trigger_type="MANUAL")
    print(f"Job 1 result: {res1['status']} in {res1['duration_seconds']}s")

    print("\nTesting Job 2...")
    res2 = sched.run_job("generate_periodic_kpi_report", trigger_type="MANUAL")
    print(f"Job 2 result: {res2['status']} in {res2['duration_seconds']}s")

    print("\nRecent Logs:")
    for log in sched.get_execution_logs(5):
        print(f"- [{log['status']}] {log['job_name']} triggered by {log['trigger_type']} at {log['start_time']}")
