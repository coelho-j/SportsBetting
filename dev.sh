#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

source scripts/utils.sh

export PROJECT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ "$(stat -c %U "$PROJECT_DIR")" != "$USER" ]; then
    sudo chown -R "$USER":"$USER" "$PROJECT_DIR"
fi


REQUIREMENTS_FILE=".linux_reqs.txt"
INSTALL_SCRIPT="scripts/install_reqs.sh"

while IFS= read -r cmd || [ -n "$cmd" ]; do
    # Skip empty lines or comments
    [[ -z "$cmd" || "$cmd" =~ ^# ]] && continue
    cmd=$(echo "$cmd" | xargs) # Trim whitespace
    if ! check_command "$cmd"; then
        echo "Running $INSTALL_SCRIPT to install missing commands..."
        if bash "$INSTALL_SCRIPT"; then
            echo "Installation script completed successfully."
            break # Exit the while loop on successful installation
        else
            exit 1 # Exit the script with error message on failure
        fi
    fi
done < "$REQUIREMENTS_FILE"


PYTHON_VERSION_FILE=".python-version"
PYTHON_VERSION=$(tr -d '[:space:]' < "$PYTHON_VERSION_FILE")

# Check if the exact python version is installed via pyenv. If not, install it.
if ! pyenv versions --bare | grep -i "${PYTHON_VERSION}" >> /dev/null; then
    echo "Python ${PYTHON_VERSION} not found in pyenv, installing..."
    pyenv install "${PYTHON_VERSION}" -s
fi

pyenv local "${PYTHON_VERSION}"

if [ ! -d .venv ]; then
    print_info "Creating virtual environment with Python ${PYTHON_VERSION}..."
    uv venv -p "$(pyenv which python)"
fi

source .venv/bin/activate
uv sync

uv run db-verify
if [ ! $? -eq 0 ]; then
    echo "db-verify failed"
    uv run db-setup
fi

