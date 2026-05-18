#!/usr/bin/env bash
set -euo pipefail

# Integration test: verify a built .deb installs correctly in a clean container
# Usage: test-install.sh <deb-file> <pg-version>
# Example: test-install.sh postgresql-16-redis-fdw-rs_0.2.0-1_amd64.deb 16

DEB_FILE="${1:?Usage: test-install.sh <deb-file> <pg-version>}"
PG_VER="${2:?}"

if [ ! -f "${DEB_FILE}" ]; then
    echo "ERROR: deb file not found: ${DEB_FILE}" >&2
    exit 1
fi

echo "==> Testing installation of ${DEB_FILE} with PostgreSQL ${PG_VER}"

docker run --rm \
    -v "$(pwd)/${DEB_FILE}:/tmp/test.deb:ro" \
    ubuntu:22.04 bash -c "
        set -euo pipefail
        apt-get update
        apt-get install -y wget gnupg2 lsb-release

        # Install PostgreSQL (using modern keyring approach)
        mkdir -p /etc/apt/keyrings
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/keyrings/pgdg.gpg
        echo \"deb [signed-by=/etc/apt/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt \$(lsb_release -cs)-pgdg main\" > /etc/apt/sources.list.d/pgdg.list
        apt-get update
        apt-get install -y postgresql-${PG_VER}

        # Install our deb
        dpkg -i /tmp/test.deb || apt-get install -fy

        # Verify files are in place
        test -f /usr/lib/postgresql/${PG_VER}/lib/redis_fdw_rs.so
        test -f /usr/share/postgresql/${PG_VER}/extension/redis_fdw_rs.control
        ls /usr/share/postgresql/${PG_VER}/extension/redis_fdw_rs--*.sql

        # Verify extension can be loaded
        su - postgres -c \"pg_lsclusters\"
        su - postgres -c \"psql -c \\\"CREATE EXTENSION redis_fdw_rs;\\\" 2>&1\" | tee /tmp/output
        if grep -q 'CREATE EXTENSION' /tmp/output; then
            echo '==> SUCCESS: Extension created successfully'
        else
            echo '==> FAIL: Extension creation failed'
            cat /tmp/output
            exit 1
        fi
    "

echo "==> PASSED: ${DEB_FILE} installs and loads correctly"
