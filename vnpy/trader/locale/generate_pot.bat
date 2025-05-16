@echo off
cd ..\..\..\
python vnpy\trader\locale\pygettext.py -o vnpy\trader\locale\vnpy.pot vnpy\trader\*.py
pause