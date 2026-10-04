# تقرير تنفيذ خط الأنابيب | Pipeline Execution Report

**تاريخ التقرير:** `2026-10-04T17:30:50.179820`  
**قاعدة البيانات المستهدفة:** `ecommerce_store` (`orders_raw`, `orders_validated`, `orders_quarantine`)

---

## 1. معلومات التشغيل | Execution Info

| الخاصية / Property | القيمة / Value |
| :--- | :--- |
| **معرف التشغيل (Run ID)** | `e1b4b644` |
| **اسم الملف (File Name)** | `01_student_test_small.csv` (`8.61 MB`) |
| **المسار الكامل (Source Path)** | `C:\Users\hp\Desktop\مجلد جديد\midterm-data-pipeline\data\01_student_test_small.csv` |
| **المحرك المستخدم (Engine)** | `python_batch` (Batch Size: 10,000) |
| **وقت التنفيذ (Timestamp)** | `2026-10-04T17:30:50.179820` |

---

## 2. إحصائيات السجلات والاتساق | Records & Consistency

| نوع السجل / Record Type | العدد / Count | النسبة / Percentage |
| :--- | :--- | :--- |
| **إجمالي السجلات المقروءة (Rows Read)** | **20,000** | **100.00%** |
| **السجلات المحملة في Raw (Raw Loaded)** | **20,000** | **100.00%** |
| ✅ السجلات السليمة (Valid Records) | 12,000 | 60.00% |
| 🔄 السجلات المصححة (Corrected Records) | 5,000 | 25.00% |
| ⚠️ السجلات المعزولة (Quarantined Records) | 3,000 | 15.00% |
| **فحص الاتساق الرياضي (Consistency Check)** | **✅ PASSED** | `Raw == Valid + Corrected + Quarantine` |

---

## 3. مقاييس عدم التكرار والـ Upsert | Idempotency Metrics

| مقياس الـ Upsert | العدد / Count | الوصف / Description |
| :--- | :--- | :--- |
| 🟢 **سجلات جديدة مضافة (Inserted)** | `17,000` | تمت إضافتها لأول مرة في `orders_validated` |
| 🟡 **سجلات محدثة (Updated)** | `0` | كانت موجودة وتم تحديث بياناتها |
| ⚪ **سجلات غير متغيرة (Unchanged)** | `0` | مطابقة تماماً ولم تتطلب أي تغيير |

---

## 4. مقاييس الأداء والموارد | Performance & Resource Metrics

| المقياس / Metric | القيمة / Value |
| :--- | :--- |
| ⏱️ **الوقت المستغرق الكلي (Elapsed Time)** | `4.86` ثانية (seconds) |
| 🚀 **معدل المعالجة (Throughput)** | `4,115.23` سجل/ثانية (records/sec) |
| 💾 **الذاكرة عند البدء (Start Memory)** | `32.95` MB |
| 💾 **الذاكرة عند الانتهاء (End Memory)** | `61.94` MB |
| 📈 **فارق استهلاك الذاكرة (Memory Delta)** | `+28.99` MB |

---

## 5. تفاصيل أخطاء العزل | Quarantine Error Breakdown

| رمز الخطأ / Error Code | عدد التكرار / Count | النسبة من الإجمالي / % of Total |
| :--- | :--- | :--- |
| `EMPTY_ITEMS` | 250 | 1.25% |
| `INVALID_EMAIL` | 250 | 1.25% |
| `INVALID_STATUS` | 250 | 1.25% |
| `CORRUPTED_ITEMS_JSON` | 250 | 1.25% |
| `AMBIGUOUS_NEGATIVE_VALUE` | 250 | 1.25% |
| `MISSING_ITEM_SKU` | 250 | 1.25% |
| `MISSING_CUSTOMER_ID` | 250 | 1.25% |
| `INVALID_CURRENCY` | 250 | 1.25% |
| `INVALID_IMPOSSIBLE_DATE` | 250 | 1.25% |
| `MULTIPLE_CONFLICTING_ERRORS` | 250 | 1.25% |
| `MISSING_ORDER_ID` | 250 | 1.25% |
| `INVALID_PHONE` | 250 | 1.25% |

---
*تم توليد هذا التقرير آلياً بواسطة وحدة `src/metrics.py` - مقرر البيانات الضخمة (جامعة الرازي)*
