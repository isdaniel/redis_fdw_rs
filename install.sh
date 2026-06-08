#!/usr/bin/env bash
set -euo pipefail

# One-line installer for redis_fdw_rs
# Usage: curl -fsSL https://isdaniel.github.io/redis_fdw_rs/install.sh | sudo bash

REPO_URL="isdaniel.github.io/redis_fdw_rs"

echo "==> Installing redis_fdw_rs PostgreSQL extension"

# Check dependencies
for cmd in curl gpg; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: '$cmd' is not installed. Please install it and try again." >&2
        exit 1
    fi
done

# Detect distro codename
if [ -f /etc/os-release ]; then
    . /etc/os-release
    CODENAME="${VERSION_CODENAME:-}"
else
    echo "ERROR: Cannot detect OS version. Install manually." >&2
    exit 1
fi

# Validate supported codename
case "${CODENAME}" in
    jammy|noble|bullseye|bookworm) ;;
    *)
        echo "ERROR: Unsupported distribution '${CODENAME}'." >&2
        echo "Supported: jammy, noble, bullseye, bookworm" >&2
        exit 1
        ;;
esac

# Detect installed PostgreSQL version
PG_VER=""
for ver in 18 17 16 15 14; do
    if dpkg -l "postgresql-${ver}" 2>/dev/null | grep -q "^ii"; then
        PG_VER="${ver}"
        break
    fi
done

if [ -z "${PG_VER}" ]; then
    echo "ERROR: No supported PostgreSQL installation detected (14-18)." >&2
    echo "Install PostgreSQL first: sudo apt install postgresql-16" >&2
    exit 1
fi

echo "==> Detected PostgreSQL ${PG_VER} on ${CODENAME}"

# Add GPG key
mkdir -p /etc/apt/keyrings
curl -fsSL "https://${REPO_URL}/gpg.key" | \
    gpg --dearmor --yes -o /etc/apt/keyrings/redis-fdw-rs.gpg

# Add repository
echo "deb [signed-by=/etc/apt/keyrings/redis-fdw-rs.gpg] https://${REPO_URL} ${CODENAME} main" \
    > /etc/apt/sources.list.d/redis-fdw-rs.list

# Install
apt-get update -o Dir::Etc::sourcelist="sources.list.d/redis-fdw-rs.list" \
               -o Dir::Etc::sourceparts="-" -o APT::Get::List-Cleanup="0"
apt-get install -y "postgresql-${PG_VER}-redis-fdw-rs"

echo ""
echo "==> redis_fdw_rs installed successfully!"
echo "    Enable it in your database with:"
echo "      psql -c 'CREATE EXTENSION redis_fdw_rs;'"
