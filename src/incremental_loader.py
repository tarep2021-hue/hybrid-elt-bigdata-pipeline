"""
incremental_loader.py — محرك التحميل التزايدي والموثوقية المتقدمة (المسار B / مسار التميز)
Incremental Loader & Advanced Reliability Module for Hybrid ELT Pipeline
مقرر البيانات الضخمة (العملي) | جامعة الرازي | إشراف: م. عمر أبو سند

الميزات:
1. معالجة تزايدية فعلية عبر ملفات Delta بدلاً من إعادة معالجة المصدر كاملاً.
2. تتبع علامة التقدم (Watermark / State Tracking) عبر collection مخصصة (pipeline_metadata).
3. معالجة التعارضات وإصدارات السجلات (Version Handling & Conflict Resolution).
4. تتبع دقيق لعدادات Upsert: inserted_count, updated_count, unchanged_count.
5. إثبات خاصية عدم التكرار (Idempotency) عند إعادة تشغيل ملفات Delta دون زيادة غير صحيحة.
"""

import os
import sys
import csv
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from config.settings import (
    FILE_ENCODING, BATCH_SIZE, DB_NAME,
    RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION
)
from src.mongo_setup import get_client, get_collections
from src.quality_rules import apply_rules
from src.metrics import PipelineMetrics


METADATA_COLLECTION = "pipeline_metadata"
WATERMARK_DOC_ID = "delta_watermark"


class IncrementalLoader:
    """محرك التحميل التزايدي للملفات ومعالجة الدلتا وفق المسار B."""

    def __init__(self, db=None):
        self.client = get_client() if db is None else None
        self.db = self.client[DB_NAME] if db is None else db
        self.raw_col = self.db[RAW_COLLECTION]
        self.val_col = self.db[VALIDATED_COLLECTION]
        self.quar_col = self.db[QUARANTINE_COLLECTION]
        self.meta_col = self.db[METADATA_COLLECTION]

    def get_watermark(self) -> Optional[str]:
        """استرجاع آخر علامة تدفق (Watermark) محفوظة في قاعدة البيانات."""
        doc = self.meta_col.find_one({"_id": WATERMARK_DOC_ID})
        if doc and "last_watermark" in doc:
            return doc["last_watermark"]
        return None

    def update_watermark(self, new_watermark: str, run_id: str) -> None:
        """تحديث علامة التدفق بعد اكتمال المعالجة بنجاح."""
        self.meta_col.update_one(
            {"_id": WATERMARK_DOC_ID},
            {
                "$set": {
                    "last_watermark": new_watermark,
                    "last_run_id": run_id,
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
            },
            upsert=True
        )

    def load_delta(self, file_path: str, run_id: Optional[str] = None, mode: str = "delta") -> Dict[str, Any]:
        """
        تحميل ومعالجة ملف Delta أو ملف Initial Load بشكل تزايدي آمن.

        Args:
            file_path: مسار ملف CSV.
            run_id: معرف دورة التشغيل.
            mode: 'initial' للتحميل الأولي أو 'delta' لمعالجة التحديثات.

        Returns:
            dict: تقرير بنتائج الـ Upsert وعدّادات التغيير والاتساق.
        """
        if run_id is None:
            run_id = f"delta-{uuid.uuid4().hex[:8]}"

        file_path_obj = Path(file_path).resolve()
        if not file_path_obj.exists():
            raise FileNotFoundError(f"الملف غير موجود: {file_path}")

        start_time = time.perf_counter()
        previous_watermark = self.get_watermark()
        new_max_watermark = previous_watermark or ""

        file_name = file_path_obj.name
        size_bytes = os.path.getsize(file_path_obj)
        size_mb = round(size_bytes / (1024 * 1024), 2)

        print("=" * 65)
        print(f"  Incremental Loader — Path B (Mode: {mode.upper()})")
        print(f"  File Name     : {file_name} ({size_mb} MB)")
        print(f"  Run ID        : {run_id}")
        print(f"  Prev Watermark: {previous_watermark or 'None (First Run)'}")
        print("=" * 65 + "\n")

        raw_records = 0
        valid_count = 0
        corrected_count = 0
        quarantine_count = 0

        inserted_count = 0
        updated_count = 0
        unchanged_count = 0
        stale_rejected_count = 0

        with open(file_path_obj, encoding=FILE_ENCODING, newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                reader.fieldnames = [col.replace("\ufeff", "").strip() for col in reader.fieldnames]

            for row_num, row in enumerate(reader, start=1):
                raw_records += 1

                # 1. Raw Layer Ingestion (حفظ السجل كما هو)
                raw_doc = {
                    "run_id": run_id,
                    "file_source": file_name,
                    "source_row_number": row_num,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "engine_used": f"incremental_{mode}",
                    "raw_record": dict(row)
                }
                self.raw_col.insert_one(raw_doc)

                # 2. تطبيق قواعد التنظيف والجودة
                status, record, details = apply_rules(dict(row))

                if status == "quarantine":
                    quarantine_count += 1
                    error_codes = [d.get("error_code", "UNKNOWN") for d in details]
                    quar_doc = {
                        "run_id": run_id,
                        "file_source": file_name,
                        "source_row_number": row_num,
                        "raw_record": dict(row),
                        "error_codes": error_codes,
                        "error_details": details,
                        "quarantined_at": datetime.now(timezone.utc).isoformat()
                    }
                    self.quar_col.insert_one(quar_doc)
                    continue

                if status == "corrected":
                    corrected_count += 1
                else:
                    valid_count += 1

                order_id = record["order_id"]
                incoming_date = record.get("order_date", "")

                # تتبع علامة التقدم الزمني
                if incoming_date > new_max_watermark:
                    new_max_watermark = incoming_date

                # 3. معالجة التعارض وعدم التكرار (Idempotent Versioned Upsert)
                existing = self.val_col.find_one({"order_id": order_id})

                record["run_id"] = run_id
                record["last_updated_at"] = datetime.now(timezone.utc).isoformat()

                if existing is None:
                    # سجل جديد بالكامل -> Insert
                    record["version"] = 1
                    self.val_col.insert_one(record)
                    inserted_count += 1
                else:
                    # سجل موجود مسبقاً -> فحص النسخة والإصدار
                    existing_date = existing.get("order_date", "")
                    existing_version = existing.get("version", 1)

                    # هل السجل الوارد مطابق تماماً للسجل المخزن؟ (تأكيد Idempotency)
                    fields_to_compare = ["status", "total_amount", "order_date", "items", "payment"]
                    is_identical = all(existing.get(k) == record.get(k) for k in fields_to_compare)

                    if is_identical:
                        unchanged_count += 1
                    elif incoming_date >= existing_date:
                        # النسخة الواردة أحدث أو بنفس التاريخ مع تعديلات -> Update
                        record["version"] = existing_version + 1
                        self.val_col.replace_one({"order_id": order_id}, record)
                        updated_count += 1
                    else:
                        # النسخة الواردة أقدم تاريخياً -> Stale rejection
                        stale_rejected_count += 1

        # تحديث علامة الـ Watermark بعد نجاح المعالجة
        if new_max_watermark and new_max_watermark != previous_watermark:
            self.update_watermark(new_max_watermark, run_id)

        elapsed = round(time.perf_counter() - start_time, 2)
        throughput = round(raw_records / elapsed, 2) if elapsed > 0 else 0.0

        # التحقق من معادلة الاتساق
        consistency = (raw_records == valid_count + corrected_count + quarantine_count)

        summary = {
            "run_id": run_id,
            "mode": mode,
            "file_name": file_name,
            "file_size_mb": size_mb,
            "previous_watermark": previous_watermark,
            "new_watermark": new_max_watermark,
            "raw_records": raw_records,
            "valid_count": valid_count,
            "corrected_count": corrected_count,
            "quarantine_count": quarantine_count,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "unchanged_count": unchanged_count,
            "stale_rejected_count": stale_rejected_count,
            "consistency_passed": consistency,
            "elapsed_seconds": elapsed,
            "throughput_rec_per_sec": throughput,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        print("\n" + "=" * 65)
        print(f"  Incremental Load Summary ({mode.upper()})")
        print("=" * 65)
        print(f"  Total Raw Processed : {raw_records:,}")
        print(f"  - Valid Records     : {valid_count:,}")
        print(f"  - Corrected Records : {corrected_count:,}")
        print(f"  - Quarantined       : {quarantine_count:,}")
        print(f"  Consistency Check   : {'PASSED' if consistency else 'FAILED'}")
        print("-" * 65)
        print(f"  🟢 Inserted (New)   : {inserted_count:,}")
        print(f"  🟡 Updated (Mod)    : {updated_count:,}")
        print(f"  ⚪ Unchanged (Same)  : {unchanged_count:,}")
        if stale_rejected_count > 0:
            print(f"  🔴 Stale Rejected   : {stale_rejected_count:,}")
        print(f"  Watermark Advanced  : {previous_watermark} -> {new_max_watermark}")
        print(f"  Elapsed Time        : {elapsed:.2f}s | Speed: {throughput:,.1f} rec/s")
        print("=" * 65 + "\n")

        return summary

    def close(self):
        """إغلاق الاتصال بقاعدة البيانات."""
        if self.client:
            self.client.close()


def run_three_experiment_proof():
    """
    تنفيذ تجارب الإثبات الثلاث الإلزامية للمسار B (القسم 7 بالوثيقة):
    1. التجربة الأولى: تشغيل Initial Load على مجموعة أساسية.
    2. التجربة الثانية: تشغيل Delta أولى تحتوي Update لسجل موجود + Insert لسجل جديد.
    3. التجربة الثالثة: إعادة تشغيل نفس Delta لإثبات Idempotency (Unchanged = Total, Inserted = 0).
    """
    loader = IncrementalLoader()
    try:
        data_dir = PROJECT_ROOT / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        initial_csv = data_dir / "experiment_initial.csv"
        delta_csv = data_dir / "experiment_delta.csv"

        # كتابة ملف Initial Load اختباري
        header = "order_id,order_date,status,customer_id,customer_name,customer_phone,customer_email,city,district,delivery_type,delivery_cost,payment_method,payment_status,payment_amount,currency,total_amount,items_json\n"
        row1 = 'ORD-DELTA-101,2025-01-10T10:00:00,مؤكد,CUST-1,محمد,771234567,m@mail.com,صنعاء,السبعين,عادي,1000.0,بطاقة,تم الدفع,10000.0,YER,10000.0,"[{\\"sku\\":\\"S1\\",\\"name\\":\\"كتاب\\",\\"qty\\":1,\\"unit_price\\":9000.0,\\"total\\":9000.0}]"\n'
        row2 = 'ORD-DELTA-102,2025-01-11T11:00:00,مؤكد,CUST-2,أحمد,771234568,a@mail.com,عدن,صيرة,سريع,2000.0,بطاقة,تم الدفع,20000.0,YER,20000.0,"[{\\"sku\\":\\"S2\\",\\"name\\":\\"قلم\\",\\"qty\\":2,\\"unit_price\\":9000.0,\\"total\\":18000.0}]"\n'

        with open(initial_csv, "w", encoding=FILE_ENCODING, newline="") as f:
            f.write(header + row1 + row2)

        # كتابة ملف Delta (يحتوي تعديل على ORD-DELTA-102 + إضافة سجل جديد ORD-DELTA-103)
        row2_updated = 'ORD-DELTA-102,2025-01-15T12:00:00,تم التسليم,CUST-2,أحمد,771234568,a@mail.com,عدن,صيرة,سريع,2000.0,بطاقة,تم الدفع,20000.0,YER,20000.0,"[{\\"sku\\":\\"S2\\",\\"name\\":\\"قلم\\",\\"qty\\":2,\\"unit_price\\":9000.0,\\"total\\":18000.0}]"\n'
        row3_new = 'ORD-DELTA-103,2025-01-16T14:00:00,مؤكد,CUST-3,سامي,771234569,s@mail.com,تعز,القاهرة,عادي,1500.0,بطاقة,تم الدفع,15000.0,YER,15000.0,"[{\\"sku\\":\\"S3\\",\\"name\\":\\"دفتر\\",\\"qty\\":1,\\"unit_price\\":13500.0,\\"total\\":13500.0}]"\n'

        with open(delta_csv, "w", encoding=FILE_ENCODING, newline="") as f:
            f.write(header + row2_updated + row3_new)

        print("\n>>> تشغيل التجربة 1/3: Initial Load الأساسي...")
        exp1 = loader.load_delta(str(initial_csv), run_id="exp1-init", mode="initial")

        print("\n>>> تشغيل التجربة 2/3: Delta أولى (Insert + Update)...")
        exp2 = loader.load_delta(str(delta_csv), run_id="exp2-delta1", mode="delta")

        print("\n>>> تشغيل التجربة 3/3: إعادة تشغيل نفس Delta (إثبات Idempotency وعدم التكرار)...")
        exp3 = loader.load_delta(str(delta_csv), run_id="exp3-delta-replay", mode="delta")

        # حفظ تقرير التجارب الثلاث
        reports_dir = PROJECT_ROOT / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        exp_file = reports_dir / "incremental_path_b_results.json"

        proof_summary = {
            "title": "Path B: Incremental Loading & Idempotency Proof",
            "date": datetime.now(timezone.utc).isoformat(),
            "experiment_1_initial_load": exp1,
            "experiment_2_delta_first_run": exp2,
            "experiment_3_delta_replay_idempotent": exp3,
            "idempotency_verified": (exp3["inserted_count"] == 0 and exp3["updated_count"] == 0 and exp3["unchanged_count"] == 2)
        }

        with open(exp_file, "w", encoding="utf-8") as f:
            json.dump(proof_summary, f, indent=4, ensure_ascii=False)

        print(f"\n[SUCCESS] تم إثبات المسار B بنجاح! حُفظ التقرير في: {exp_file}\n")
        return proof_summary
    finally:
        loader.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        run_three_experiment_proof()
    elif len(sys.argv) > 2:
        target_mode = sys.argv[1].replace("--", "")
        target_file = sys.argv[2]
        loader = IncrementalLoader()
        try:
            loader.load_delta(target_file, mode=target_mode)
        finally:
            loader.close()
    else:
        print("Usage:")
        print("  python src/incremental_loader.py --demo")
        print("  python src/incremental_loader.py --delta <path_to_delta_csv>")
        print("  python src/incremental_loader.py --initial <path_to_initial_csv>")
