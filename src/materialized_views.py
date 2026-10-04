"""
materialized_views.py — العروض المادية والتحديث التزايدي (Phase 2)
Big Data - Phase 2: Materialized Views with Incremental Refresh Mechanism

المتطلبات:
1. إنشاء 2 Materialized Views مبنية على نتائج التجميعات:
   - daily_sales_summary: ملخص المبيعات اليومية
   - top_products_summary: ملخص أداء ومبيعات المنتجات
2. آلية تحديث تزايدي (Incremental Refresh) تستفيد من علامات التدفق (Watermark)
   لتحديث التواريخ أو المنتجات المتأثرة فقط دون إعادة بناء البيانات بالكامل في كل مرة.
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo import UpdateOne
from bson import ObjectId

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database

VIEW_DAILY_SALES = "daily_sales_summary"
VIEW_TOP_PRODUCTS = "top_products_summary"
MV_METADATA_COLLECTION = "materialized_view_metadata"


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


class MaterializedViewManager:
    """مدير العروض المادية وآلية التحديث التزايدي الذكية."""

    def __init__(self, db=None):
        self.db = get_database() if db is None else db
        self.val_col = self.db[VALIDATED_COLLECTION]
        self.meta_col = self.db[MV_METADATA_COLLECTION]
        self.daily_mv_col = self.db[VIEW_DAILY_SALES]
        self.products_mv_col = self.db[VIEW_TOP_PRODUCTS]

    def get_watermark(self, view_name: str) -> Optional[str]:
        """استرجاع آخر علامة تدفق (Watermark) للعرض المادي."""
        doc = self.meta_col.find_one({"_id": view_name})
        if doc and "last_watermark" in doc:
            return doc["last_watermark"]
        return None

    def update_watermark(self, view_name: str, new_watermark: str, refresh_mode: str, affected_count: int) -> None:
        """تحديث بيانات التعريف وعلامة التدفق للعرض المادي."""
        self.meta_col.update_one(
            {"_id": view_name},
            {
                "$set": {
                    "last_watermark": new_watermark,
                    "last_refreshed_at": datetime.now(timezone.utc).isoformat(),
                    "last_refresh_mode": refresh_mode,
                    "last_affected_count": affected_count,
                },
                "$inc": {"refresh_cycle_count": 1}
            },
            upsert=True
        )

    def refresh_daily_sales(self, mode: str = "incremental") -> Dict[str, Any]:
        """
        تحديث العرض المادي daily_sales_summary.
        - إذا كان الوضع incremental ويوجد watermark، يتم تحديث التواريخ المتأثرة بالسجلات الجديدة فقط.
        - إذا كان الوضع full، يتم إعادة بناء العرض المادي بالكامل.
        """
        start_time = time.perf_counter()
        watermark = self.get_watermark(VIEW_DAILY_SALES)
        now_iso = datetime.now(timezone.utc).isoformat()

        # استعلام أعلى تاريخ معالجة متاح حالياً
        latest_doc = self.val_col.find_one(
            {"validated_at": {"$exists": True}},
            sort=[("validated_at", -1)]
        )
        current_max_timestamp = latest_doc.get("validated_at", now_iso) if latest_doc else now_iso

        affected_dates = []

        if mode == "incremental" and watermark:
            # البحث عن التواريخ المتأثرة بالسجلات التي أضيفت أو عُدلت بعد الـ Watermark
            changed_orders = list(self.val_col.find(
                {"validated_at": {"$gt": watermark}},
                {"order_date": 1}
            ))
            if not changed_orders:
                elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
                return {
                    "view_name": VIEW_DAILY_SALES,
                    "status": "UP_TO_DATE",
                    "mode": "incremental",
                    "affected_dates_count": 0,
                    "elapsed_ms": elapsed_ms,
                    "message": "العرض المادي محدث بالفعل ولا توجد سجلات جديدة منذ آخر تحديث."
                }

            # استخراج التواريخ الفريدة فقط (YYYY-MM-DD)
            affected_dates = list({doc["order_date"][:10] for doc in changed_orders if "order_date" in doc})
            print(f"  [Incremental MV] Refreshing {len(affected_dates)} affected dates for {VIEW_DAILY_SALES}...")

        # بناء خط أنابيب التجميع
        pipeline: List[Dict[str, Any]] = []

        # إذا كنا في الوضع التزايدي، نقصر المعالجة على التواريخ المتأثرة فقط
        if affected_dates:
            # مطابقة الطلبات التي تقع ضمن التواريخ المتأثرة
            regex_pattern = f"^({'|'.join(affected_dates)})"
            pipeline.append({"$match": {"order_date": {"$regex": regex_pattern}}})

        pipeline.extend([
            {
                "$project": {
                    "date": {"$substrCP": ["$order_date", 0, 10]},
                    "total_amount": "$total_amount",
                    "status": "$status",
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "date": {"$first": "$date"},
                    "total_sales": {"$sum": "$total_amount"},
                    "order_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$total_amount"},
                    "delivered_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "تم التسليم"]}, 1, 0]}
                    },
                    "canceled_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "ملغي"]}, 1, 0]}
                    },
                }
            },
            {
                "$project": {
                    "_id": "$date",
                    "date": "$date",
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "order_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]},
                    "delivered_count": 1,
                    "canceled_count": 1,
                    "last_refreshed_at": now_iso,
                }
            },
            {
                "$merge": {
                    "into": VIEW_DAILY_SALES,
                    "on": "_id",
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            }
        ])

        self.val_col.aggregate(pipeline)

        # تحديث علامة التدفق
        self.update_watermark(
            VIEW_DAILY_SALES,
            current_max_timestamp,
            mode if not affected_dates else "incremental",
            len(affected_dates) if affected_dates else self.daily_mv_col.count_documents({})
        )

        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        total_rows = self.daily_mv_col.count_documents({})

        return {
            "view_name": VIEW_DAILY_SALES,
            "status": "SUCCESS",
            "mode": "incremental" if affected_dates else "full",
            "affected_count": len(affected_dates) if affected_dates else total_rows,
            "total_view_documents": total_rows,
            "watermark": current_max_timestamp,
            "elapsed_ms": elapsed_ms,
        }

    def refresh_top_products(self, mode: str = "incremental") -> Dict[str, Any]:
        """
        تحديث العرض المادي top_products_summary.
        - يدعم التحديث التزايدي للأصناف المتأثرة بالطلبات الجديدة.
        """
        start_time = time.perf_counter()
        watermark = self.get_watermark(VIEW_TOP_PRODUCTS)
        now_iso = datetime.now(timezone.utc).isoformat()

        latest_doc = self.val_col.find_one(
            {"validated_at": {"$exists": True}},
            sort=[("validated_at", -1)]
        )
        current_max_timestamp = latest_doc.get("validated_at", now_iso) if latest_doc else now_iso

        affected_skus = []

        if mode == "incremental" and watermark:
            changed_orders = list(self.val_col.find(
                {"validated_at": {"$gt": watermark}},
                {"items.sku": 1}
            ))
            if not changed_orders:
                elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
                return {
                    "view_name": VIEW_TOP_PRODUCTS,
                    "status": "UP_TO_DATE",
                    "mode": "incremental",
                    "affected_skus_count": 0,
                    "elapsed_ms": elapsed_ms,
                    "message": "العرض المادي محدث بالفعل ولا توجد أصناف جديدة منذ آخر تحديث."
                }

            # استخراج الـ SKUs المتأثرة
            sku_set = set()
            for doc in changed_orders:
                for item in doc.get("items", []):
                    if "sku" in item:
                        sku_set.add(item["sku"])
            affected_skus = list(sku_set)
            print(f"  [Incremental MV] Refreshing {len(affected_skus)} affected SKUs for {VIEW_TOP_PRODUCTS}...")

        pipeline: List[Dict[str, Any]] = []

        if affected_skus:
            pipeline.append({"$match": {"items.sku": {"$in": affected_skus}}})

        pipeline.extend([
            {"$unwind": "$items"},
        ])

        if affected_skus:
            pipeline.append({"$match": {"items.sku": {"$in": affected_skus}}})

        pipeline.extend([
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
                    "_id": "$sku",
                    "sku": "$sku",
                    "product_name": "$product_name",
                    "total_quantity_sold": 1,
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "order_appearances": 1,
                    "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
                    "last_refreshed_at": now_iso,
                }
            },
            {
                "$merge": {
                    "into": VIEW_TOP_PRODUCTS,
                    "on": "_id",
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            }
        ])

        self.val_col.aggregate(pipeline)

        self.update_watermark(
            VIEW_TOP_PRODUCTS,
            current_max_timestamp,
            mode if not affected_skus else "incremental",
            len(affected_skus) if affected_skus else self.products_mv_col.count_documents({})
        )

        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        total_rows = self.products_mv_col.count_documents({})

        return {
            "view_name": VIEW_TOP_PRODUCTS,
            "status": "SUCCESS",
            "mode": "incremental" if affected_skus else "full",
            "affected_count": len(affected_skus) if affected_skus else total_rows,
            "total_view_documents": total_rows,
            "watermark": current_max_timestamp,
            "elapsed_ms": elapsed_ms,
        }

    def refresh_all(self, mode: str = "incremental") -> Dict[str, Any]:
        """تحديث كافة العروض المادية في خطوة واحدة."""
        r1 = self.refresh_daily_sales(mode=mode)
        r2 = self.refresh_top_products(mode=mode)
        return {
            "status": "SUCCESS",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "views": [r1, r2],
        }

    def get_view_data(self, view_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        """استرجاع بيانات العرض المادي المخزنة في MongoDB."""
        if view_name not in [VIEW_DAILY_SALES, VIEW_TOP_PRODUCTS]:
            raise ValueError(f"Unknown view: {view_name}")
        cursor = self.db[view_name].find().limit(limit)
        return [_clean_doc(doc) for doc in cursor]

    def get_metadata(self) -> List[Dict[str, Any]]:
        """استرجاع بيانات تتبع العروض المادية وحالة الـ Watermark."""
        cursor = self.meta_col.find()
        return [_clean_doc(doc) for doc in cursor]


if __name__ == "__main__":
    mgr = MaterializedViewManager()
    res = mgr.refresh_all(mode="full")
    print("Materialized Views Refreshed:", res)
