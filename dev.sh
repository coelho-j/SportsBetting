#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

REQUIREMENTS_FILE="${1:-.linux_reqs.txt}"
INSTALL_SCRIPT="scripts/install_reqs.sh"

check_command() {
    local cmd="$1"
    command -v "$cmd" &> /dev/null
    return $?
}

while IFS= read -r cmd || [ -n "$cmd" ]; do
    # Skip empty lines or comments
    [[ -z "$cmd" || "$cmd" =~ ^# ]] && continue
    cmd=$(echo "$cmd" | xargs) # Trim whitespace
    if ! check_command "$cmd"; then
        missing_commands+=("$cmd")
        echo "Running $INSTALL_SCRIPT to install missing commands..."
        if bash "$INSTALL_SCRIPT"; then
            echo "Installation script completed successfully."
            break # Exit the while loop on successful installation
        else
            exit 1 # Exit the script with error message on failure
        fi
    fi
done < "$REQUIREMENTS_FILE"


if [ ! -f .python-version ]; then
    pyenv install 3.13 -s
    pyenv local 3.13
fi

if [ ! -d .venv ]; then
    uv venv
fi

source .venv/bin/activate

uv sync