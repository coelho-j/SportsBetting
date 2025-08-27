#!/usr/bin/env bash

# This file contains utility functions for environment setup.
# It is intended to be sourced by other scripts.

# --- Functions for Colored Output ---
print_info() {
    # Blue for info
    echo -e "\e[34m[INFO]\e[0m $1"
}

print_success() {
    # Green for success
    echo -e "\e[32m[SUCCESS]\e[0m $1"
}

print_error() {
    # Red for error
    echo -e "\e[31m[ERROR]\e[0m $1" >&2
}

# --- Main Installation Function ---

# Installs system packages from a given file based on the detected OS.
# It reads the OS from the `.os` file in the project root to determine
# which package manager to use (pacman or apt).
#
# Usage: os_source <path_to_requirements_file>
os_source() {
    local requirements_file="$1"

    # 1. Check for root privileges, and elevate if necessary.
    # if [[ "${EUID}" -ne 0 ]]; then
    #     if ! command -v sudo &> /dev/null; then
    #         print_error "sudo is not installed. Please run this script as root."
    #         exit 1
    #     fi
    #     print_info "This script requires superuser privileges to install system packages."

    #     # Find the calling script. BASH_SOURCE[0] is this script (utils.sh),
    #     # and BASH_SOURCE[1] is the script that sourced it.
    #     local calling_script_path="${BASH_SOURCE[1]}"
    #     if [[ -z "$calling_script_path" ]]; then
    #         print_error "Cannot determine calling script. This script should be sourced."
    #         exit 1
    #     fi

    #     # Resolve the path of the calling script to be absolute. This is crucial
    #     # because `sudo` may not have the same working directory, and a relative
    #     # path would fail. The `cd ... && pwd` trick is a portable way to do this.
    #     local calling_script
    #     calling_script="$(cd -- "$(dirname -- "$calling_script_path")" &>/dev/null && pwd)/$(basename -- "$calling_script_path")"

    #     print_info "Attempting to re-run with sudo..."
    #     # Re-execute the calling script with sudo, passing along the arguments.
    #     # 'exec' replaces the current shell process with the new one.
    #     exec sudo -- bash "$calling_script" "$@"
    # fi

    # 2. Validate input
    if [[ -z "${requirements_file}" ]]; then
        print_error "Usage: os_source <path_to_requirements_file>"
        exit 1
    fi
    if [[ ! -f "${requirements_file}" ]]; then
        print_error "Requirements file not found at: ${requirements_file}"
        exit 1
    fi

    # 3. Detect the Operating System
    local os=""
    if [[ -f /etc/os-release ]]; then
        # Source the os-release file to get distribution info.
        # The file contains shell-compatible variable assignments.
        # shellcheck source=/dev/null
        . /etc/os-release
        os=$ID
    else
        print_error "Cannot determine OS: /etc/os-release not found."
        exit 1
    fi

    # 4. Read packages from file
    mapfile -t packages < <(grep -vE '^\s*#|^\s*$' "${requirements_file}")

    if [[ ${#packages[@]} -eq 0 ]]; then
        print_info "No packages to install from ${requirements_file}."
        return 0
    fi

    print_info "Detected OS: ${os}"
    print_info "Attempting to install: ${packages[*]}"

    # 5. Install packages based on the detected OS
    case "${os}" in
        "arch")
            print_info "Updating package database and upgrading system (yay)..."
            # pacman -Syu --noconfirm
            print_info "Installing packages..."
            yay -S --noconfirm --needed "${packages[@]}"
            ;;
        "ubuntu")
            print_info "Updating package database (apt)..."
            apt-get update
            print_info "Installing packages..."
            # apt install is idempotent and acts like --needed.
            # DEBIAN_FRONTEND=noninteractive prevents interactive prompts during installation.
            DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages[@]}"
            ;;
        *)
            print_error "Unsupported OS: '${os}'. Please add support in scripts/utils.sh"
            exit 1
            ;;
    esac

    print_success "System package installation complete."
}

check_command() {
    local cmd="$1"
    command -v "$cmd" &> /dev/null
    return $?
}

export_env() {
    # If an argument is provided, treat it as a file to source.
    if [[ -n "$1" ]]; then
        local source_file="$1"
        if [[ ! -f "$source_file" ]]; then
            print_error "Source file not found: ${source_file}"
            return 1
        fi
        print_info "Sourcing environment file: ${source_file}"
        # shellcheck source=/dev/null
        source "${source_file}"
        print_success "Sourced ${source_file}."
        return 0
    fi

    # If no argument, default to processing .env in PROJECT_DIR.
    if [[ -z "$PROJECT_DIR" ]]; then
        print_error "PROJECT_DIR is not set. Cannot find default .env file."
        return 1
    fi
    local env_file="${PROJECT_DIR}/.env"

    if [[ ! -f "$env_file" ]]; then
        print_info "Default environment file .env not found in ${PROJECT_DIR}. Skipping."
        return 0
    fi

    print_info "Exporting environment variables from ${env_file}..."
    set -a # Automatically export all variables from the sourced file.
    # shellcheck source=/dev/null
    source "${env_file}"
    set +a # Disable auto-export.
    print_success "Environment variables from ${env_file} exported."
}