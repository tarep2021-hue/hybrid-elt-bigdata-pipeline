"""
queries.py — استعلامات المشروع النهائي مع دعم Explain ExecutionStats
Big Data - Phase 2: Practical Queries & Query Execution Engine

المتطلبات:
1. تنفيذ 5 استعلامات عملية على الأقل تناسب بيانات المشروع.
2. خدمة الاستعلامات بالفهارس المناسبة.
3. دعم explain("executionStats") مع استخراج مؤشرات الأداء:
   - زمن التنفيذ (executionTimeMillis)
   - عدد المستندات المفحوصة (totalDocsExamined)
   - عدد المفاتيح المفحوصة (totalKeysExamined)
   - عدد السجلات المعادة (nReturned)
   - مرحلة التنفيذ (COLLSCAN vs IXSCAN)
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from bson import ObjectId

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database


def _clean_doc(doc: Any) -> Any:
    """تحويل ObjectId وتواريخ BSON إلى نصوص لتسهيل ترميز JSON."""
    if isinstance(doc, dict):
        return {k: _clean_doc(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [_clean_doc(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    return doc


def extract_explain_metrics(explain_res: Dict[str, Any]) -> Dict[str, Any]:
    """استخراج أهم مؤشرات الأداء من تقرير explain('executionStats')."""
    stats = explain_res.get("executionStats", {})
    server_info = explain_res.get("serverInfo", {})

    # البحث عن المرحلة الفائزة والمؤشرات
    stages = stats.get("executionStages", {})
    winning_stage = stages.get("stage", "UNKNOWN")
    index_name = stages.get("indexName")

    # في حال كانت المرحلة FETCH، قد يكون الفهرس في inputStage
    if not index_name and "inputStage" in stages:
        input_stage = stages["inputStage"]
        index_name = input_stage.get("indexName")
        if winning_stage == "FETCH" and input_stage.get("stage"):
            winning_stage = f"{winning_stage} -> {input_stage.get('stage')}"

    return {
        "execution_time_ms": stats.get("executionTimeMillis", 0),
        "total_docs_examined": stats.get("totalDocsExamined", 0),
        "total_keys_examined": stats.get("totalKeysExamined", 0),
        "n_returned": stats.get("nReturned", 0),
        "winning_stage": winning_stage,
        "index_name": index_name,
        "is_indexed": "IXSCAN" in str(winning_stage) or index_name is not None,
    }


# قاموس تعريف الاستعلامات الخمسة
QUERY_CATALOG: Dict[str, Dict[str, Any]] = {
    "orders_by_status_and_date": {
        "title": "الاستعلام 1: الطلبات حسب الحالة ونطاق التاريخ",
        "description": "استعلام لتصفية الطلبات بحالتها (مثل: تم التسليم) وتاريخ محدد مع ترتيب النتائج تنازلياً.",
        "servicing_index": "idx_status_order_date (Compound)",
        "default_params": {
            "status": "تم التسليم",
            "start_date": "2025-01-01",
            "end_date": "2025-06-30",
            "limit": 20,
        },
    },
    "customer_order_history": {
        "title": "الاستعلام 2: سجل طلبات العميل برقم الهاتف",
        "description": "استعلام سريع للبحث عن تاريخ ومشتريات عميل معين باستخدام رقم الهاتف المحمول.",
        "servicing_index": "idx_customer_phone (Single)",
        "default_params": {
            "phone": "770000000",
            "limit": 20,
        },
    },
    "high_value_orders_by_city": {
        "title": "الاستعلام 3: الطلبات عالية القيمة في مدينة معينة",
        "description": "استعلام لاكتشاف الطلبات ذات المبالغ الكبيرة في مدينة معينة وتسهيل استهداف العملاء المميزين.",
        "servicing_index": "idx_city_total_amount (Compound)",
        "default_params": {
            "city": "صنعاء",
            "min_amount": 100000,
            "limit": 20,
        },
    },
    "product_item_search": {
        "title": "الاستعلام 4: البحث عن الطلبات المحتوية على منتج أو رمز صنف",
        "description": "استعلام يبحث داخل مصفوفة items عن الطلبات التي اشترت رمز منتج معين (SKU).",
        "servicing_index": "idx_items_sku (Multikey)",
        "default_params": {
            "sku": "SKU-1010",
            "limit": 20,
        },
    },
    "pending_cash_deliveries": {
        "title": "الاستعلام 5: الطلبات قيد الشحن/المؤكدة بنظام الدفع عند الاستلام",
        "description": "استعلام يخدم فريق التوصيل والتحصيل لمتابعة الطلبات الجاري تسليمها والتي لم يُسدد ثمنها بعد.",
        "servicing_index": "idx_payment_method_status (Compound)",
        "default_params": {
            "payment_method": "نقدًا عند التسليم",
            "payment_status": "بانتظار الدفع",
            "limit": 20,
        },
    },
}


def build_mongo_query(query_name: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """بناء تعبير استعلام MongoDB وتحديد حقول الترتيب والحد."""
    if query_name not in QUERY_CATALOG:
        raise ValueError(f"Unknown query: '{query_name}'. Available: {list(QUERY_CATALOG.keys())}")

    p = {**QUERY_CATALOG[query_name]["default_params"], **(params or {})}
    limit = int(p.get("limit", 20))

    if query_name == "orders_by_status_and_date":
        filter_doc = {"status": p.get("status", "تم التسليم")}
        date_cond = {}
        if "start_date" in p:
            date_cond["$gte"] = str(p["start_date"])
        if "end_date" in p:
            date_cond["$lte"] = str(p["end_date"]) + "T23:59:59"
        if date_cond:
            filter_doc["order_date"] = date_cond
        sort_doc = [("order_date", -1)]

    elif query_name == "customer_order_history":
        filter_doc = {"customer.phone": str(p.get("phone", "770000000"))}
        sort_doc = [("order_date", -1)]

    elif query_name == "high_value_orders_by_city":
        filter_doc = {
            "customer.address.city": p.get("city", "صنعاء"),
            "total_amount": {"$gte": float(p.get("min_amount", 100000))},
        }
        sort_doc = [("total_amount", -1)]

    elif query_name == "product_item_search":
        filter_doc = {"items.sku": str(p.get("sku", "SKU-1010"))}
        sort_doc = [("order_date", -1)]

    elif query_name == "pending_cash_deliveries":
        filter_doc = {
            "payment.method": p.get("payment_method", "نقدًا عند التسليم"),
            "payment.status": p.get("payment_status", "بانتظار الدفع"),
            "status": {"$in": ["قيد الشحن", "مؤكد"]},
        }
        sort_doc = [("order_date", 1)]

    else:
        raise ValueError(f"Query implementation missing: {query_name}")

    return {
        "filter": filter_doc,
        "sort": sort_doc,
        "limit": limit,
    }


def execute_query(
    query_name: str,
    params: Optional[Dict[str, Any]] = None,
    explain: bool = False,
    db=None
) -> Dict[str, Any]:
    """
    تنفيذ أحد الاستعلامات الخمسة مع دعم explain('executionStats').

    Args:
        query_name: اسم الاستعلام
        params: متغيرات التصفية والحد
        explain: إذا كانت True يتم إرجاع مقاييس Explain بدلاً من السجلات فقط
        db: كائن قاعدة البيانات

    Returns:
        Dict: تفاصيل التنفيذ والنتائج
    """
    if db is None:
        db = get_database()

    col = db[VALIDATED_COLLECTION]
    spec = build_mongo_query(query_name, params)

    cursor = col.find(spec["filter"]).sort(spec["sort"]).limit(spec["limit"])

    if explain:
        find_cmd = {
            "find": VALIDATED_COLLECTION,
            "filter": spec["filter"],
            "sort": dict(spec["sort"]),
            "limit": spec["limit"],
        }
        raw_explain = db.command("explain", find_cmd, verbosity="executionStats")
        summary_stats = extract_explain_metrics(raw_explain)
        return {
            "query_name": query_name,
            "title": QUERY_CATALOG[query_name]["title"],
            "servicing_index": QUERY_CATALOG[query_name]["servicing_index"],
            "filter": spec["filter"],
            "sort": spec["sort"],
            "limit": spec["limit"],
            "execution_metrics": summary_stats,
            "raw_explain": _clean_doc(raw_explain),
        }

    # تنفيذ الاستعلام وجلب السجلات
    results = [_clean_doc(doc) for doc in cursor]
    return {
        "query_name": query_name,
        "title": QUERY_CATALOG[query_name]["title"],
        "count": len(results),
        "limit": spec["limit"],
        "filter": spec["filter"],
        "records": results,
    }


def list_available_queries() -> List[Dict[str, Any]]:
    """قائمة بكافة الاستعلامات المتاحة مع وصفها ومعاملاتها."""
    return [
        {
            "name": name,
            "title": meta["title"],
            "description": meta["description"],
            "servicing_index": meta["servicing_index"],
            "default_params": meta["default_params"],
        }
        for name, meta in QUERY_CATALOG.items()
    ]
