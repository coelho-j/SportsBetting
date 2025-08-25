#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

# This script serves as an entry point for installing system dependencies.
# It sources the main logic from utils.sh to keep things modular.

# Source the utility functions and the main installation logic.
# shellcheck source=./utils.sh
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR="${SCRIPT_DIR}/../"
source "${SCRIPT_DIR}/utils.sh"

# Define the path to the system package requirements file.
REQUIREMENTS_FILE="${PROJECT_DIR}/.linux_reqs.txt"

# Execute the OS-aware installation function.
os_source "${REQUIREMENTS_FILE}"
