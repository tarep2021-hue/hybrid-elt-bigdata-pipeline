"""
indexes.py — إدارة فهارس MongoDB لمشروع البيانات الضخمة (Phase 2)
Big Data - Phase 2: Index Management & Optimization

يتولى هذا الملف إنشاء وإدارة وفحص الفهارس المطلوبة لتحسين أداء الاستعلامات:
1. الفهرس المركب (Compound Index): [("status", 1), ("order_date", -1)]
2. الفهرس الفردي (Single Index): [("customer.phone", 1)]
3. الفهرس المركب الإقليمي (Compound Index): [("customer.address.city", 1), ("total_amount", -1)]
4. فهرس العناصر متعدد المفاتيح (Multikey Index): [("items.sku", 1)]
5. فهرس الدفع المركب (Compound Index): [("payment.method", 1), ("payment.status", 1)]
"""

import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database


INDEX_DEFINITIONS = [
    {
        "name": "idx_status_order_date",
        "keys": [("status", ASCENDING), ("order_date", DESCENDING)],
        "is_compound": True,
        "description": "فهرس مركب يخدم استعلام الطلبات حسب الحالة والفترة الزمنية ويسرع الترتيب الزمني",
        "serves_query": "orders_by_status_and_date",
        "collection": VALIDATED_COLLECTION,
    },
    {
        "name": "idx_customer_phone",
        "keys": [("customer.phone", ASCENDING)],
        "is_compound": False,
        "description": "فهرس فردي يتيح البحث الفوري المباشر عن سجل طلبات العميل برقم الهاتف دون مسح كامل للجدول",
        "serves_query": "customer_order_history",
        "collection": VALIDATED_COLLECTION,
    },
    {
        "name": "idx_city_total_amount",
        "keys": [("customer.address.city", ASCENDING), ("total_amount", DESCENDING)],
        "is_compound": True,
        "description": "فهرس مركب يسرع استعلام وتصفية وترتيب الطلبات ذات القيمة العالية حسب كل مدينة",
        "serves_query": "high_value_orders_by_city",
        "collection": VALIDATED_COLLECTION,
    },
    {
        "name": "idx_items_sku",
        "keys": [("items.sku", ASCENDING)],
        "is_compound": False,
        "description": "فهرس متعدد المفاتيح (Multikey) للبحث السريع في مصفوفة المنتجات والعناصر داخل الطلب",
        "serves_query": "product_item_search",
        "collection": VALIDATED_COLLECTION,
    },
    {
        "name": "idx_payment_method_status",
        "keys": [("payment.method", ASCENDING), ("payment.status", ASCENDING)],
        "is_compound": True,
        "description": "فهرس مركب لتسريع متابعة عمليات الدفع (مثل نقدًا عند التسليم بانتظار التحصيل)",
        "serves_query": "pending_cash_deliveries",
        "collection": VALIDATED_COLLECTION,
    },
]


def create_phase2_indexes(db: Optional[Database] = None) -> Dict[str, Any]:
    """
    إنشاء كافة الفهارس المطلوبة للمشروع النهائي مع توثيق وقت التنفيذ.

    Returns:
        Dict: تقرير بنتائج إنشاء الفهارس.
    """
    if db is None:
        db = get_database()

    col = db[VALIDATED_COLLECTION]
    start_time = time.perf_counter()
    created = []
    already_existing = []

    existing_index_names = {idx["name"] for idx in col.list_indexes()}

    for defn in INDEX_DEFINITIONS:
        name = defn["name"]
        keys = defn["keys"]
        if name in existing_index_names:
            already_existing.append(name)
        else:
            col.create_index(keys, name=name)
            created.append({
                "name": name,
                "keys": [f"{k[0]}:{k[1]}" for k in keys],
                "is_compound": defn["is_compound"],
                "serves_query": defn["serves_query"],
                "description": defn["description"]
            })

    elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

    return {
        "status": "SUCCESS",
        "collection": VALIDATED_COLLECTION,
        "created_count": len(created),
        "created_indexes": created,
        "existing_indexes": already_existing,
        "elapsed_ms": elapsed_ms,
        "total_indexes": len(list(col.list_indexes())),
    }


def drop_phase2_indexes(db: Optional[Database] = None) -> List[str]:
    """
    حذف فهارس المرحلة الثانية (يُستخدم لاختبار Explain قبل وبعد الفهرسة).
    """
    if db is None:
        db = get_database()

    col = db[VALIDATED_COLLECTION]
    dropped = []
    existing = {idx["name"] for idx in col.list_indexes()}

    for defn in INDEX_DEFINITIONS:
        name = defn["name"]
        if name in existing:
            col.drop_index(name)
            dropped.append(name)

    return dropped


def list_collection_indexes(col_name: str = VALIDATED_COLLECTION, db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """
    قائمة بكافة الفهارس الحالية على المجموعة.
    """
    if db is None:
        db = get_database()
    return list(db[col_name].list_indexes())


if __name__ == "__main__":
    result = create_phase2_indexes()
    print("=" * 60)
    print("  Index Creation Result:")
    print(f"  Created: {result['created_count']}")
    print(f"  Existing: {result['existing_indexes']}")
    print(f"  Elapsed: {result['elapsed_ms']} ms")
    print("=" * 60)
