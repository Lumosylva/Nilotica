@echo off
echo Setting TA-Lib environment variables for build...
set TA_INCLUDE_PATH=D:\Project\PycharmProjects\Nilotica_dev\ta-lib\include
set TA_LIBRARY_PATH=D:\Project\PycharmProjects\Nilotica_dev\ta-lib\lib

echo Running hatch build...
hatch build %*

echo Cleaning up TA-Lib environment variables...
set TA_INCLUDE_PATH=
set TA_LIBRARY_PATH=

echo Build process finished.