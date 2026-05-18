#!/usr/bin/env bash
set -euo pipefail

# Usage: publish-repo.sh <deb-dir> <gpg-key-id> <repo-url>
# Assembles apt repo structure from .deb files using reprepro

DEB_DIR="${1:?Usage: publish-repo.sh <deb-dir> <gpg-key-id> <repo-url>}"
GPG_KEY_ID="${2:?}"
REPO_URL="${3:?}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="apt-repo"

rm -rf "${REPO_DIR}"
mkdir -p "${REPO_DIR}/conf"

# Generate distributions config
sed "s/__GPG_KEY_ID__/${GPG_KEY_ID}/g" \
    "${SCRIPT_DIR}/distributions.template" > "${REPO_DIR}/conf/distributions"

# Include all debs in all supported codenames
CODENAMES="jammy noble bullseye bookworm"
for codename in ${CODENAMES}; do
    for deb in "${DEB_DIR}"/*.deb; do
        reprepro -b "${REPO_DIR}" includedeb "${codename}" "${deb}"
    done
done

# Export public GPG key
gpg --armor --export "${GPG_KEY_ID}" > "${REPO_DIR}/gpg.key"

# Generate install.sh from template
sed "s|__REPO_URL__|${REPO_URL}|g" \
    "${SCRIPT_DIR}/install.sh.template" > "${REPO_DIR}/install.sh"
chmod +x "${REPO_DIR}/install.sh"

echo "APT repo assembled in ${REPO_DIR}/"
echo "Contents:"
find "${REPO_DIR}" -type f | sort
