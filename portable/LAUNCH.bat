@echo off
cd /d "%~dp0"
cd ..
echo Launching Email Query Tool...
echo.
echo This tool was developed by Birdwatcher Group.
echo.
echo Activating virtual environment...
echo.

call C:\Users\johnn\venv\Scripts\activate

echo Starting Streamlit...
echo To stop Streamlit, press Ctrl+C in this window, and close the browser tab.
echo.
REM Use full path to python.exe inside the portable env
call python -m streamlit run streamlit.py
pause
echo Streamlit has stopped. Press any key to exit.
pause