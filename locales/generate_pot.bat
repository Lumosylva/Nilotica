@echo off
title Internationalization scripts
:: Activate Python environment
cd ..\
call .venv\Scripts\activate
:: Generate pot file
pybabel extract -F babel.cfg -o locales/nilotica.pot .
:: Update an existing .po file with a new .pot file
pybabel init -i locales/nilotica.pot -d locales -l en
exit