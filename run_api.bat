@echo off
title Big Data Phase 2 - FastAPI Server
echo ========================================================
echo   Starting Big Data Phase 2 Unified FastAPI Gateway...
echo   Swagger UI: http://localhost:8000/docs
echo   Health API: http://localhost:8000/health
echo ========================================================
python -m uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
pause
