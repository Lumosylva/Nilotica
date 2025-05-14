@echo off
title Internationalization scripts
:: Activate Python environment
call .venv\Scripts\activate
echo %CD%
:: Generate pot file
pybabel extract -F babel.cfg -o locales/messages.pot .
:: Update an existing .po file with a new .pot file
pybabel init -i locales/messages.pot -d locales -l en
exit