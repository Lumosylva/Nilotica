@echo off
title Internationalization scripts
:: Activate Python environment
cd ..\\
call .venv\\Scripts\\activate
:: Generate pot file
pybabel extract -F babel.cfg -o locales/messages.pot .
:: Update an existing .po file with a new .pot file
pybabel update -i locales/messages.pot -d locales -l en
exit