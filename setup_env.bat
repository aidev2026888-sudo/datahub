@echo off
echo === DataHub Integration - Environment Setup ===
echo.

REM Create virtual environment
echo Creating Python virtual environment...
python -m venv .venv
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to create virtual environment. Make sure Python is installed.
    exit /b 1
)

REM Activate virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate.bat

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

REM Install dependencies
echo Installing dependencies from requirements.txt...
pip install -r requirements.txt
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to install dependencies.
    exit /b 1
)

echo.
echo === Setup Complete ===
echo.
echo To activate the environment in the future, run:
echo     .venv\Scripts\activate.bat
echo.
echo Configure your DataHub connection:
echo     set DATAHUB_GMS_SERVER=http://your-datahub-server:8080
echo     set DATAHUB_TOKEN=your-token
echo.
echo Run the CLI:
echo     python main.py --help
