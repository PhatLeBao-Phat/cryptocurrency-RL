#!/bin/bash

# Check if the environment exists
if conda env list | grep -q "env"; then
    echo "Environment exists. Updating..."
    conda env update --name env --file /app/environment.yml --prune
else
    echo "Environment does not exist. Creating..."
    conda env create --name env --file /app/environment.yml
fi

# Activate the environment and start an interactive bash session
conda run -n env /bin/bash