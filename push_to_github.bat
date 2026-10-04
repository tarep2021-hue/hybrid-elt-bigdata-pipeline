@echo off
chcp 65001 >nul
title رفع المشروع إلى GitHub
echo ========================================================
echo   جارٍ رفع مشروع خط البيانات الهجين إلى GitHub...
echo ========================================================
echo.

"C:\Program Files\Git\cmd\git.exe" remote set-url origin https://github.com/tarep2021-hue/hybrid-elt-bigdata-pipeline.git
"C:\Program Files\Git\cmd\git.exe" add .
"C:\Program Files\Git\cmd\git.exe" commit -m "Complete Hybrid ELT Big Data Pipeline: Phase 1 and Phase 2 with FastAPI, Indexes, Materialized Views"
"C:\Program Files\Git\cmd\git.exe" push -u origin main

echo.
echo ========================================================
echo   اكتملت العملية! اضغط أي مفتاح للإغلاق.
echo ========================================================
pause
