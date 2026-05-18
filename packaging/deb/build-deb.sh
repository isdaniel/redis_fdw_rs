#!/usr/bin/env bash
set -euo pipefail

# Usage: build-deb.sh <pg-version> <extension-version> <architecture> <pgrx-output-dir>
# Example: build-deb.sh 16 0.2.0 amd64 target/release/redis_fdw_rs-pg16

PG_VER="${1:?Usage: build-deb.sh <pg-ver> <ext-ver> <arch> <pgrx-output-dir>}"
EXT_VER="${2:?}"
ARCH="${3:?}"
PGRX_OUT="${4:?}"

PKG_NAME="postgresql-${PG_VER}-redis-fdw-rs"
PKG_DIR="${PKG_NAME}_${EXT_VER}-1_${ARCH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

rm -rf "${PKG_DIR}"
mkdir -p "${PKG_DIR}/DEBIAN"
mkdir -p "${PKG_DIR}/usr/lib/postgresql/${PG_VER}/lib"
mkdir -p "${PKG_DIR}/usr/share/postgresql/${PG_VER}/extension"

# Copy shared library
cp "${PGRX_OUT}/usr/lib/postgresql/${PG_VER}/lib/redis_fdw_rs.so" \
   "${PKG_DIR}/usr/lib/postgresql/${PG_VER}/lib/"

# Copy extension control and SQL files
cp "${PGRX_OUT}/usr/share/postgresql/${PG_VER}/extension/redis_fdw_rs.control" \
   "${PKG_DIR}/usr/share/postgresql/${PG_VER}/extension/"
cp "${PGRX_OUT}/usr/share/postgresql/${PG_VER}/extension/"redis_fdw_rs--*.sql \
   "${PKG_DIR}/usr/share/postgresql/${PG_VER}/extension/"

# Generate DEBIAN/control from template
sed -e "s/__PG_VER__/${PG_VER}/g" \
    -e "s/__VERSION__/${EXT_VER}/g" \
    -e "s/__ARCH__/${ARCH}/g" \
    "${SCRIPT_DIR}/control.template" > "${PKG_DIR}/DEBIAN/control"

# Build the deb
dpkg-deb --build --root-owner-group "${PKG_DIR}"

echo "Built: ${PKG_DIR}.deb"
