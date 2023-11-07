#!/bin/bash

# Activates environmental variables
source ~/.bashrc

# Activate the virtual environment
source $Automations/gcp_env/bin/activate

# Navigate to the script's directory
cd $Automations

# Run the Python script
python amazon_planning_inventory.py

# Deactivate the virtual environment
deactivate