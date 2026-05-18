# APT Repository Setup

## One-Time Setup: GPG Key

Generate a dedicated GPG key for signing the APT repository:

```bash
# Generate key (no passphrase for CI automation)
gpg --batch --gen-key <<EOF
%no-protection
Key-Type: RSA
Key-Length: 4096
Name-Real: redis_fdw_rs APT Signing Key
Name-Email: redis-fdw-rs@noreply.github.com
Expire-Date: 0
%commit
EOF

# Export private key (add to GitHub Secret: APT_GPG_PRIVATE_KEY)
gpg --armor --export-secret-keys redis-fdw-rs@noreply.github.com

# Export public key (for verification)
gpg --armor --export redis-fdw-rs@noreply.github.com
```

## GitHub Repository Settings

1. **Enable GitHub Pages:**
   - Settings > Pages > Source: "Deploy from a branch"
   - Branch: `gh-pages` / root

2. **Add Secrets:**
   - `APT_GPG_PRIVATE_KEY`: The full armored private key output from above

## Triggering a Release

Push a version tag to trigger the APT release:

```bash
git tag v0.3.0
git push origin v0.3.0
```

The workflow builds debs for all PG versions x architectures and publishes to GitHub Pages.

## Verifying the Repository

After first release, verify:

```bash
# Check repo metadata is accessible
curl -I https://isdaniel.github.io/redis_fdw_rs/dists/noble/main/binary-amd64/Packages.gz

# Check GPG key is accessible
curl -fsSL https://isdaniel.github.io/redis_fdw_rs/gpg.key | gpg --show-keys
```
