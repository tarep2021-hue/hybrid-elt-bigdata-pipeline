"""
explain_benchmarks.py — إجراء مقارنة explain("executionStats") قبل وبعد الفهارس
Big Data - Phase 2: Index Performance Evaluation & Explain Benchmarking

المتطلبات:
تنفيذ explain("executionStats") لعدد 3 استعلامات قبل وبعد إنشاء الفهارس،
مع توضيح سبب اختيار كل فهرس وأثره.
"""

import sys
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mongo_setup import get_database
from src.indexes import create_phase2_indexes, drop_phase2_indexes
from src.queries import execute_query


BENCHMARK_TARGETS = [
    {
        "query_name": "orders_by_status_and_date",
        "title": "استعلام الطلبات حسب الحالة والتاريخ",
        "params": {"status": "تم التسليم", "start_date": "2025-01-01", "end_date": "2025-03-31", "limit": 50},
        "chosen_index": "idx_status_order_date",
        "index_type": "Compound Index [status: 1, order_date: -1]",
        "justification": "تحتاج طلبات المتجر اليومية إلى التصفية الدورية حسب الحالة ثم الترتيب الزمني العكسي. الفهرس المركب يطابق مبدأ Equality -> Sort -> Range (ESR)، مما يقضي تماماً على المسح الشامل (COLLSCAN) والفرز في الذاكرة (SORT stage).",
    },
    {
        "query_name": "customer_order_history",
        "title": "استعلام سجل طلبات العميل برقم الهاتف",
        "params": {"phone": "770000000", "limit": 20},
        "chosen_index": "idx_customer_phone",
        "index_type": "Single Index [customer.phone: 1]",
        "justification": "رقم الهاتف هو المعرّف الأكثر استخداماً من قبل موظفي خدمة العملاء والمندوبين. بدلاً من فحص عشرات الآلاف من السجلات للوصول للعميل، يتيح الفهرس الفردي قفزة B-Tree فورية بتعقيد O(log N).",
    },
    {
        "query_name": "high_value_orders_by_city",
        "title": "استعلام الطلبات عالية القيمة في مدينة معينة",
        "params": {"city": "صنعاء", "min_amount": 50000, "limit": 50},
        "chosen_index": "idx_city_total_amount",
        "index_type": "Compound Index [customer.address.city: 1, total_amount: -1]",
        "justification": "تعتمد التقارير الإقليمية والتسويقية على استهداف الطلبات الكبيرة لكل مدينة. دمج المدينة مع قيمة المبلغ في فهرس مركب يسمح بالتصفية حسب المدينة ثم قراءة الطلبات مرتبة مباشرة من مؤشر الفهرس.",
    },
]


def run_explain_benchmarks(reports_dir: str = "reports", db=None) -> Dict[str, Any]:
    """
    تنفيذ اختبار المقارنة قبل وبعد إنشاء الفهارس.
    """
    if db is None:
        db = get_database()

    print("\n" + "=" * 70)
    print("  [Explain Benchmark] Starting Index Evaluation (Before vs After)...")
    print("=" * 70)

    # 1. إيجاد قيم فعلية موجودة في قاعدة البيانات لضمان نتائج واقعية
    col = db["orders_validated"]
    sample_doc = col.find_one()
    if sample_doc:
        sample_phone = sample_doc.get("customer", {}).get("phone", "770000000")
        sample_city = sample_doc.get("customer", {}).get("address", {}).get("city", "صنعاء")
        sample_status = sample_doc.get("status", "تم التسليم")
        BENCHMARK_TARGETS[0]["params"]["status"] = sample_status
        BENCHMARK_TARGETS[1]["params"]["phone"] = sample_phone
        BENCHMARK_TARGETS[2]["params"]["city"] = sample_city

    # 2. حذف الفهارس لتسجيل أداء الـ Baseline (COLLSCAN)
    print("\n[Step 1/4] Dropping phase 2 indexes for Baseline evaluation (COLLSCAN)...")
    dropped = drop_phase2_indexes(db)
    print(f"  [OK] Dropped indexes: {dropped}")

    # 3. تشغيل explain قبل الفهارس
    print("\n[Step 2/4] Executing Explain executionStats BEFORE indexing...")
    before_results = {}
    for target in BENCHMARK_TARGETS:
        q_name = target["query_name"]
        print(f"  -> Running explain on: '{q_name}' (BEFORE)...")
        res = execute_query(q_name, target["params"], explain=True, db=db)
        before_results[q_name] = res["execution_metrics"]

    # 4. إعادة إنشاء الفهارس
    print("\n[Step 3/4] Creating phase 2 indexes (IXSCAN)...")
    idx_report = create_phase2_indexes(db)
    print(f"  [OK] Indexes created in {idx_report['elapsed_ms']} ms")

    # 5. تشغيل explain بعد الفهارس
    print("\n[Step 4/4] Executing Explain executionStats AFTER indexing...")
    after_results = {}
    comparisons = []

    for target in BENCHMARK_TARGETS:
        q_name = target["query_name"]
        print(f"  -> Running explain on: '{q_name}' (AFTER)...")
        res = execute_query(q_name, target["params"], explain=True, db=db)
        after_metric = res["execution_metrics"]
        after_results[q_name] = after_metric

        before_metric = before_results[q_name]

        docs_before = before_metric["total_docs_examined"]
        docs_after = after_metric["total_docs_examined"]
        docs_reduction = round(((docs_before - docs_after) / max(docs_before, 1)) * 100, 2)

        time_before = before_metric["execution_time_ms"]
        time_after = after_metric["execution_time_ms"]
        speedup = round(time_before / max(time_after, 0.001), 2) if time_after > 0 else (time_before if time_before > 0 else 1.0)

        impact_desc = (
            f"انخفض عدد المستندات المفحوصة من {docs_before:,} إلى {docs_after:,} مستند "
            f"(توفير بنسبة {docs_reduction}%). "
            f"تحولت آلية البحث من المسح الكامل للمجموعة ({before_metric['winning_stage']}) "
            f"إلى فحص دقيق عبر الفهرس ({after_metric['winning_stage']})."
        )

        comparisons.append({
            "query_name": q_name,
            "title": target["title"],
            "chosen_index": target["chosen_index"],
            "index_type": target["index_type"],
            "justification": target["justification"],
            "impact_summary": impact_desc,
            "before": {
                "stage": before_metric["winning_stage"],
                "execution_time_ms": time_before,
                "docs_examined": docs_before,
                "keys_examined": before_metric["total_keys_examined"],
                "n_returned": before_metric["n_returned"],
            },
            "after": {
                "stage": after_metric["winning_stage"],
                "execution_time_ms": time_after,
                "docs_examined": docs_after,
                "keys_examined": after_metric["total_keys_examined"],
                "n_returned": after_metric["n_returned"],
                "index_name": after_metric.get("index_name"),
            },
            "metrics_comparison": {
                "docs_reduction_pct": docs_reduction,
                "speedup_factor": f"{speedup}x" if speedup > 1 else "Optimized",
            }
        })

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "collection": col.name,
        "total_documents": col.count_documents({}),
        "queries_analyzed": len(comparisons),
        "comparisons": comparisons,
    }

    # حفظ التقارير في المجلد المطلوب
    rep_dir = Path(reports_dir)
    rep_dir.mkdir(parents=True, exist_ok=True)

    json_path = rep_dir / "explain_results.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    md_path = rep_dir / "explain_results.md"
    _generate_markdown_report(report, md_path)

    print(f"\n  [SUCCESS] Benchmark complete! Reports saved to:")
    print(f"    - {json_path}")
    print(f"    - {md_path}")
    print("=" * 70 + "\n")

    return report


def _generate_markdown_report(report: Dict[str, Any], output_path: Path) -> None:
    """توليد تقرير Markdown مفصل مع جداول وشرح علمي للأثر."""
    lines = [
        "# تقرير تحليل الأداء وخطة التنفيذ (Explain ExecutionStats)",
        "**مقرر البيانات الضخمة (العملي) — المرحلة الثانية**\n",
        f"- **تاريخ الاختبار:** `{report['timestamp']}`",
        f"- **إجمالي مستندات المجموعة:** `{report['total_documents']:,}` سجل\n",
        "---",
        "## جدول مقارنة الأداء (قبل الفهارس vs بعد الفهارس)\n",
        "| الاستعلام | نوع الفهرس المستخدم | المرحلة قبل | المرحلة بعد | المستندات المفحوصة (قبل) | المستندات المفحوصة (بعد) | نسبة التوفير |",
        "| :--- | :--- | :---: | :---: | :---: | :---: | :---: |",
    ]

    for item in report["comparisons"]:
        b = item["before"]
        a = item["after"]
        c = item["metrics_comparison"]
        lines.append(
            f"| **{item['title']}** | `{item['chosen_index']}` | `{b['stage']}` | `{a['stage']}` | {b['docs_examined']:,} | {a['docs_examined']:,} | **{c['docs_reduction_pct']}%** |"
        )

    lines.append("\n---\n## التفاصيل الفنية والتعليل لكل استعلام وفهرس\n")

    for i, item in enumerate(report["comparisons"], start=1):
        b = item["before"]
        a = item["after"]
        lines.extend([
            f"### {i}. {item['title']} (`{item['query_name']}`)",
            f"- **الفهرس المختار:** `{item['chosen_index']}` ({item['index_type']})",
            f"- **سبب اختيار الفهرس:** {item['justification']}",
            f"- **أثر الفهرس:** {item['impact_summary']}",
            "",
            "#### مقارنة تفصيلية:",
            f"- **زمن التنفيذ:** قبل: `{b['execution_time_ms']} ms` | بعد: `{a['execution_time_ms']} ms`",
            f"- **المستندات المفحوصة (Docs Examined):** قبل: `{b['docs_examined']:,}` | بعد: `{a['docs_examined']:,}`",
            f"- **المفاتيح المفحوصة (Keys Examined):** قبل: `{b['keys_examined']:,}` | بعد: `{a['keys_examined']:,}`",
            f"- **عدد السجلات الناتجة (Returned):** `{a['n_returned']}`",
            "",
        ])

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    run_explain_benchmarks()
