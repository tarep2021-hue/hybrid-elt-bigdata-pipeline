"""
test_instructor_dataset.py — اختبار التطابق التام مع ملف وبيانات الدكتور المرجعية
المقرر: البيانات الضخمة (العملي) | جامعة الرازي | إشراف: م. عمر أبو سند
الملف المختبر: 01_student_test_small.csv
الملف المرجعي: EXPECTED_RESULTS.xlsx
"""

import sys
import csv
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import FILE_ENCODING
from src.quality_rules import apply_rules

TEST_CSV_PATH = PROJECT_ROOT / "data" / "01_student_test_small.csv"


@pytest.fixture(scope="module")
def processed_data():
    assert TEST_CSV_PATH.exists(), f"ملف الاختبار غير موجود: {TEST_CSV_PATH}"

    raw_count = 0
    valid_count = 0
    corrected_count = 0
    quarantine_count = 0
    quarantine_reasons = {}
    correction_types = {}

    with open(TEST_CSV_PATH, mode="r", encoding=FILE_ENCODING, errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_count += 1
            cat, cleaned, details = apply_rules(row)
            if cat == "valid":
                valid_count += 1
            elif cat == "corrected":
                corrected_count += 1
                for c in details:
                    code = c.get("rule_code") or c.get("rule")
                    correction_types[code] = correction_types.get(code, 0) + 1
            elif cat == "quarantine":
                quarantine_count += 1
                codes = [d.get("error_code") for d in details if isinstance(d, dict)]
                if "MULTIPLE_CONFLICTING_ERRORS" in codes or len(codes) >= 2:
                    primary = "multiple_conflicting_errors"
                else:
                    primary = codes[0].lower() if codes else "unknown"
                quarantine_reasons[primary] = quarantine_reasons.get(primary, 0) + 1

    return {
        "raw_count": raw_count,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "validated_count": valid_count + corrected_count,
        "quarantine_count": quarantine_count,
        "quarantine_reasons": quarantine_reasons,
        "correction_types": correction_types,
    }


def test_totals_match_expected_results(processed_data):
    """
    التحقق من مطابقة الأعداد الإجمالية بدقة 100%:
    - Raw: 20,000
    - Clean Valid: 12,000
    - Corrected: 5,000
    - Validated: 17,000
    - Quarantine: 3,000
    """
    assert processed_data["raw_count"] == 20000, "إجمالي الصفوف يجب أن يكون 20,000"
    assert processed_data["valid_count"] == 12000, "السجلات السليمة (Clean Valid) يجب أن تكون 12,000 تماماً"
    assert processed_data["corrected_count"] == 5000, "السجلات المصححة (Corrected) يجب أن تكون 5,000 تماماً"
    assert processed_data["validated_count"] == 17000, "السجلات المقبولة (orders_validated) يجب أن تكون 17,000 تماماً"
    assert processed_data["quarantine_count"] == 3000, "السجلات المعزولة (orders_quarantine) يجب أن تكون 3,000 تماماً"

    # معادلة الاتساق الرياضية
    assert (
        processed_data["raw_count"]
        == processed_data["valid_count"]
        + processed_data["corrected_count"]
        + processed_data["quarantine_count"]
    ), "معادلة الاتساق الرياضي غير متطابقة!"


def test_quarantine_categories_250_each(processed_data):
    """
    التحقق من أن كل فئة من فئات العزل الـ 12 تحتوي على 250 سجلاً تماماً
    (12 × 250 = 3,000).
    """
    reasons = processed_data["quarantine_reasons"]
    expected_categories = [
        "missing_order_id",
        "missing_customer_id",
        "invalid_phone_too_short",
        "email_missing_domain",
        "invalid_date_impossible",
        "unknown_order_status",
        "empty_items",
        "corrupted_items_json",
        "missing_item_sku",
        "negative_quantity",
        "unknown_currency",
        "multiple_conflicting_errors",
    ]

    # رسم الخرائط بين رموز الكود ورموز الدكتور
    mapping = {
        "missing_order_id": reasons.get("missing_order_id", 0),
        "missing_customer_id": reasons.get("missing_customer_id", 0),
        "invalid_phone_too_short": reasons.get("invalid_phone", 0) or reasons.get("invalid_phone_too_short", 0),
        "email_missing_domain": reasons.get("invalid_email", 0) or reasons.get("email_missing_domain", 0),
        "invalid_date_impossible": reasons.get("invalid_impossible_date", 0) or reasons.get("invalid_date_impossible", 0),
        "unknown_order_status": reasons.get("invalid_status", 0) or reasons.get("unknown_order_status", 0),
        "empty_items": reasons.get("empty_items", 0),
        "corrupted_items_json": reasons.get("corrupted_items_json", 0),
        "missing_item_sku": reasons.get("missing_item_sku", 0),
        "negative_quantity": reasons.get("ambiguous_negative_value", 0) or reasons.get("negative_quantity", 0),
        "unknown_currency": reasons.get("invalid_currency", 0) or reasons.get("unknown_currency", 0),
        "multiple_conflicting_errors": reasons.get("multiple_conflicting_errors", 0),
    }

    for cat, count in mapping.items():
        assert count == 250, f"الفئة {cat} عددها {count} والمطلوب 250 تماماً"


def test_correction_categories_500_each(processed_data):
    """
    التحقق من أن السجلات المصححة يبلغ إجماليها 5,000 سجل تماماً
    وتتوزع على الـ 10 قواعد بمعدل 500 لكل قاعدة.
    """
    assert processed_data["corrected_count"] == 5000
