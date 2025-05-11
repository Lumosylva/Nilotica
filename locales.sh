#!/bin/bash
# You need to set the script execution permission: chmod +x locales.sh
# Set Title
echo "Internationalization scripts"

# Activate the Python environment
source .venv/bin/activate

# Display the current directory
echo "$PWD"

# Generate pot file
pybabel extract -F babel.cfg -o locales/messages.pot .

# Update an existing .po file with a new .pot file
pybabel init -i locales/messages.pot -d locales -l en
pybabel init -i locales/messages.pot -d locales -l zh_CN

# quit
exit 0 