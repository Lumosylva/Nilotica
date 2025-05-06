@echo off
title build Project
:: activate Python
call .venv\Scripts\activate
echo Setting TA-Lib environment variables for build...
set current_path=%~dp0
echo Current path: %current_path%

set TA_INCLUDE_PATH=%current_path%\ta-lib\include
set TA_LIBRARY_PATH=%current_path%\ta-lib\lib

echo Running hatch build...
hatch build %*

echo Cleaning up TA-Lib environment variables...
set TA_INCLUDE_PATH=
set TA_LIBRARY_PATH=

echo Build process finished.
pause