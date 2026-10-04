"""
aggregations.py — تقارير التجميع لمشروع البيانات الضخمة (Phase 2)
Big Data - Phase 2: Aggregation Reports Pipeline

يحتوي هذا الملف على 5 تقارير تجميع تحليلية متقدمة:
1. sales_by_city: تقرير المبيعات حسب المدينة (إجمالي المبيعات، عدد الطلبات، متوسط قيمة الطلب)
2. top_products: أفضل المنتجات مبيعاً من حيث الإيرادات والكميات (باستخدام $unwind للسلع)
3. top_customers: أفضل العملاء من حيث إجمالي الإنفاق وتكرار الشراء
4. sales_by_period: تحليل المبيعات حسب الفترة الزمنية (يومي أو شهري)
5. order_status_distribution: توزيع الطلبات حسب الحالة وتكلفة التوصيل ومعدل الإلغاء
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
    """تحويل قيم BSON غير القابلة للترميز في JSON إلى قيم نصية قياسية."""
    if isinstance(doc, dict):
        return {k: _clean_doc(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [_clean_doc(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    return doc


AGGREGATION_CATALOG: Dict[str, Dict[str, Any]] = {
    "sales_by_city": {
        "title": "تقرير المبيعات حسب المدينة",
        "description": "تحليل إجمالي وحجم وقيم المبيعات ومتوسط قيمة السلة الشرائية لكل مدينة.",
        "default_params": {"min_orders": 1},
    },
    "top_products": {
        "title": "تقرير أفضل المنتجات مبيعاً",
        "description": "تفكيك مصفوفة السلع (Unwind Items) وترتيب المنتجات الأكثر تحقيقاً للإيرادات والكميات المباعة.",
        "default_params": {"limit": 10},
    },
    "top_customers": {
        "title": "تقرير كبار العملاء (VIP Customers)",
        "description": "تجميع تاريخ مشتريات العملاء وتحديد الأكثر إنفاقاً وتكراراً للطلبات وتاريخ آخر طلب.",
        "default_params": {"limit": 10},
    },
    "sales_by_period": {
        "title": "تقرير المبيعات حسب الفترة الزمنية",
        "description": "توزيع نمو المبيعات والإيرادات حسب الأشهر والسنوات (Trend Analysis).",
        "default_params": {"period_format": "monthly"},  # 'daily' or 'monthly'
    },
    "order_status_distribution": {
        "title": "تقرير توزيع حالات الطلبات وتكاليف التوصيل",
        "description": "إحصائيات حالات الطلب (مسلّم، ملغي، مرتجع، إلخ) مع متوسط تكلفة الشحن ومبالغ التحصيل.",
        "default_params": {},
    },
}


def build_pipeline(report_name: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """بناء Pipeline التجميع لـ MongoDB بناءً على اسم التقرير والمعاملات."""
    if report_name not in AGGREGATION_CATALOG:
        raise ValueError(f"Unknown aggregation report '{report_name}'. Available: {list(AGGREGATION_CATALOG.keys())}")

    p = {**AGGREGATION_CATALOG[report_name]["default_params"], **(params or {})}

    if report_name == "sales_by_city":
        min_orders = int(p.get("min_orders", 1))
        return [
            {
                "$group": {
                    "_id": "$customer.address.city",
                    "city": {"$first": "$customer.address.city"},
                    "total_sales": {"$sum": "$total_amount"},
                    "total_orders": {"$sum": 1},
                    "avg_order_value": {"$avg": "$total_amount"},
                    "max_order_value": {"$max": "$total_amount"},
                    "delivered_orders": {
                        "$sum": {"$cond": [{"$eq": ["$status", "تم التسليم"]}, 1, 0]}
                    },
                }
            },
            {"$match": {"total_orders": {"$gte": min_orders}}},
            {
                "$project": {
                    "_id": 0,
                    "city": "$city",
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "total_orders": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                    "max_order_value": {"$round": ["$max_order_value", 2]},
                    "delivered_orders": 1,
                    "delivery_rate_pct": {
                        "$round": [
                            {
                                "$multiply": [
                                    {"$divide": ["$delivered_orders", {"$max": ["$total_orders", 1]}]},
                                    100,
                                ]
                            },
                            2,
                        ]
                    },
                }
            },
            {"$sort": {"total_sales": -1}},
        ]

    elif report_name == "top_products":
        limit = int(p.get("limit", 10))
        return [
            {"$unwind": "$items"},
            {
                "$group": {
                    "_id": "$items.sku",
                    "sku": {"$first": "$items.sku"},
                    "product_name": {"$first": "$items.name"},
                    "total_quantity_sold": {"$sum": "$items.qty"},
                    "total_revenue": {"$sum": "$items.total"},
                    "order_appearances": {"$sum": 1},
                    "avg_unit_price": {"$avg": "$items.unit_price"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "sku": "$sku",
                    "product_name": "$product_name",
                    "total_quantity_sold": 1,
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "order_appearances": 1,
                    "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
                }
            },
            {"$sort": {"total_revenue": -1}},
            {"$limit": limit},
        ]

    elif report_name == "top_customers":
        limit = int(p.get("limit", 10))
        return [
            {
                "$group": {
                    "_id": "$customer.customer_id",
                    "customer_id": {"$first": "$customer.customer_id"},
                    "customer_name": {"$first": "$customer.name"},
                    "phone": {"$first": "$customer.phone"},
                    "city": {"$first": "$customer.address.city"},
                    "total_spend": {"$sum": "$total_amount"},
                    "total_orders": {"$sum": 1},
                    "avg_order_spend": {"$avg": "$total_amount"},
                    "last_order_date": {"$max": "$order_date"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_id": "$customer_id",
                    "customer_name": "$customer_name",
                    "phone": "$phone",
                    "city": "$city",
                    "total_spend": {"$round": ["$total_spend", 2]},
                    "total_orders": 1,
                    "avg_order_spend": {"$round": ["$avg_order_spend", 2]},
                    "last_order_date": 1,
                }
            },
            {"$sort": {"total_spend": -1}},
            {"$limit": limit},
        ]

    elif report_name == "sales_by_period":
        period_format = str(p.get("period_format", "monthly")).lower()
        substr_len = 7 if period_format == "monthly" else 10  # 'YYYY-MM' or 'YYYY-MM-DD'
        return [
            {
                "$project": {
                    "period": {"$substrCP": ["$order_date", 0, substr_len]},
                    "total_amount": "$total_amount",
                    "status": "$status",
                }
            },
            {
                "$group": {
                    "_id": "$period",
                    "period": {"$first": "$period"},
                    "total_sales": {"$sum": "$total_amount"},
                    "order_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$total_amount"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "period": "$period",
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "order_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                }
            },
            {"$sort": {"period": 1}},
        ]

    elif report_name == "order_status_distribution":
        return [
            {
                "$group": {
                    "_id": "$status",
                    "status": {"$first": "$status"},
                    "count": {"$sum": 1},
                    "total_value": {"$sum": "$total_amount"},
                    "avg_order_value": {"$avg": "$total_amount"},
                    "avg_delivery_cost": {"$avg": "$delivery.cost"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "status": "$status",
                    "count": 1,
                    "total_value": {"$round": ["$total_value", 2]},
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                    "avg_delivery_cost": {"$round": ["$avg_delivery_cost", 2]},
                }
            },
            {"$sort": {"count": -1}},
        ]

    else:
        raise ValueError(f"Pipeline definition missing for '{report_name}'")


def run_aggregation(
    report_name: str,
    params: Optional[Dict[str, Any]] = None,
    db=None
) -> Dict[str, Any]:
    """
    تنفيذ تقرير تجميع محدد واسترجاع النتائج بتنسيق JSON.
    """
    if db is None:
        db = get_database()

    col = db[VALIDATED_COLLECTION]
    pipeline = build_pipeline(report_name, params)

    results = list(col.aggregate(pipeline))
    cleaned_results = [_clean_doc(doc) for doc in results]

    return {
        "report_name": report_name,
        "title": AGGREGATION_CATALOG[report_name]["title"],
        "description": AGGREGATION_CATALOG[report_name]["description"],
        "records_count": len(cleaned_results),
        "data": cleaned_results,
    }


def list_available_aggregations() -> List[Dict[str, Any]]:
    """قائمة بكافة تقارير التجميع المتاحة."""
    return [
        {
            "name": name,
            "title": meta["title"],
            "description": meta["description"],
            "default_params": meta["default_params"],
        }
        for name, meta in AGGREGATION_CATALOG.items()
    ]


if __name__ == "__main__":
    for rep in AGGREGATION_CATALOG:
        res = run_aggregation(rep)
        print(f"[OK] {rep}: returned {res['records_count']} rows")
