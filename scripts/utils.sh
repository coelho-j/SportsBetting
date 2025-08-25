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

    # 1. Check for root privileges
    if [[ "${EUID}" -ne 0 ]]; then
        print_error "This function must be run as root. Try running with 'sudo'."
        exit 1
    fi

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
            print_info "Updating package database and upgrading system (pacman)..."
            pacman -Syu --noconfirm
            print_info "Installing packages..."
            pacman -S --noconfirm --needed "${packages[@]}"
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