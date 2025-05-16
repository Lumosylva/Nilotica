#!/bin/bash
# You need to set the script execution permission: chmod +x locales.sh
# Set Title
echo "Internationalization scripts"

# Activate the Python environment
source .venv/bin/activate

# Generate pot file
pybabel extract -F babel.cfg -o locales/nilotica.pot .

# Update an existing .po file with a new .pot file
pybabel init -i locales/nilotica.pot -d locales -l en

# quit
exit 0 