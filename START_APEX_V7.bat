@echo off
echo.
echo =====================================================
echo   APEX NSE v7 — Professional Trading Intelligence
echo   Swing + Intraday · Advanced Charts · Dark UI
echo =====================================================
echo.
cd /d "%~dp0"
echo Starting backend...
if exist ".venv\Scripts\python.exe" (
start /B "" ".venv\Scripts\python.exe" backend/app.py
) else (
start /B "" python backend/app.py
)
echo Waiting for server...
timeout /t 3 /nobreak >nul
echo Opening browser...
start http://localhost:6060
echo.
echo System running at http://localhost:6060
echo Press Ctrl+C in the Python window to stop
echo.


