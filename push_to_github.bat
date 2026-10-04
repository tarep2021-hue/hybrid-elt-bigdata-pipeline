@echo off
setlocal
cd /d "%~dp0"
set "PATH=C:\Program Files\Git\cmd;%PATH%"

echo ========================================================
echo   Pushing Hybrid ELT Data Pipeline to GitHub...
echo ========================================================
echo.

git remote set-url origin https://github.com/tarep2021-hue/hybrid-elt-bigdata-pipeline.git
git branch -M main
git add .
git commit -m "Complete Hybrid ELT Big Data Pipeline: Phase 1 & Phase 2"
git push -u origin main

echo.
echo ========================================================
echo   Process completed! Press any key to exit.
echo ========================================================
pause
