# Wilopo production deployment

Production deployment is an explicit, manually published GitHub Release: publish a non-prerelease release whose tag begins `release-production-` in `wilopo-cargo/open-webui`. Pushes, pull requests, and releases in forks do not deploy. The workflow does not create or publish releases. It builds the standard Dockerfile once and deploys the immutable resolved commit SHA to the configured production matrix. It is single-instance, with controlled downtime while the app restarts; Open WebUI startup may apply database migrations.

## Required GitHub setup

Create the `production` GitHub Environment and configure:

- Variable `CREDENTIALS_APP_ID` and secret `CREDENTIALS_APP_PRIVATE_KEY` for a GitHub App installed on `wilopo-cargo/wilopo-credentials` with read-only Contents access.
- Secret `GPG_SECRET_PASSPHRASE`, used to decrypt the server matrix in Actions and the per-region runtime environment on each host.
- One environment secret per server-matrix `key` value. Each secret contains that server's private SSH key.

The credentials repository is checked out at one captured commit SHA for all workflow jobs. Store the following encrypted repository files there; the repository key must be accessible to the GitHub App:

- `open-webui/production-servers-matrix.json.gpg`
- `open-webui/<region>.production.env.gpg` for every matrix region.

The decrypted server matrix must be a nonempty JSON array. Each record contains exactly:

- `region`: unique lowercase letters/digits/hyphens, used as the environment filename stem.
- `ip`: an IPv4 address or DNS hostname, with no shell syntax. Bare IPv6 literals are not supported by the pinned SSH action.
- `username`: a safe SSH login name.
- `key`: the name of the GitHub Environment SSH-key secret (`[A-Z_][A-Z0-9_]*`). It is a secret identifier, not key material.
- `port`: integer SSH port from 1 through 65535.
- `path`: unique, absolute, shell-safe dedicated directory ending in `/open-webui`. Traversal, `/open-webui` itself, and any `wilopo-service` path are rejected. Do not point this at a shared service or repository directory.
- `fingerprint`: pinned OpenSSH host-key fingerprint in `SHA256:<base64>` form. Verify it through your trusted server inventory before entering it; do not use an unauthenticated first-contact fingerprint.

Provision and verify these files and secrets before publishing a production release. Missing configuration fails the workflow; it never falls back to another application's credentials.

## Server and application prerequisites

Provision each host and its dedicated `/.../open-webui` directory before deployment. The SSH account must be able to run Docker and Docker Compose, and the host must have `gpg`, `docker`, and the Docker Compose plugin installed. Create the external Docker network named `proxy` and ensure the existing Traefik proxy watches it, accepts `websecure`, and has the `cloudflare` certificate resolver configured. The deployment script checks that `proxy` exists before pulling or stopping the app. The Compose service exposes port 8080 only to that network; it publishes no host port.

Create a dedicated PostgreSQL database and login for this Open WebUI installation. The encrypted runtime environment must define nonempty `WEBUI_SECRET_KEY`, `DATABASE_URL`, `WEBUI_URL`, and `APP_HOST` values. `WEBUI_SECRET_KEY` must be a long-lived stable key; changing it can invalidate sessions and tokens. `DATABASE_URL` must use the dedicated PostgreSQL database. `WEBUI_URL` must be the application's HTTPS URL and `APP_HOST` its DNS host. Keep this environment file private; deployment never prints its contents. The deployment script decrypts it under `umask 077`, validates required settings and Compose configuration, then stores it as mode `0600` before any existing app is stopped.

For an existing installation, preserve its effective signing key, database, and data volume rather than treating this as a fresh install. An existing persisted WebUI URL can override the environment default: set the production URL through Admin → Settings before using Composio callbacks. This workflow does not move or change the laptop installation.

Compose uses project name `wilopo-open-webui-prod`, one `app` service, and the dedicated named `open_webui_data` volume. It does not start Ollama, run a separate migration service, or remove volumes/images. Do not share this project name or data volume with another application.

## Deployment and recovery behavior

After local setup is complete, publishing the matching production Release automatically runs the workflow. There is no deploy-on-push or scheduled deployment. The workflow validates the release/repository guard, builds `ghcr.io/wilopo-cargo/open-webui:<resolved-commit-sha>` with the `production-buildcache` cache reference, and deploys only that immutable image. The encrypted environment and deployment files are copied to the dedicated server directory using the same pinned SSH fingerprint for SCP and SSH.

The host logs in to GHCR using the workflow token, pulls the image before stopping the current app, then stops the single app and starts it with Compose `--wait` (up to 300 seconds). Health requires both `/ready` and `/health/db`; the container allows a five-minute startup grace period for migrations. If pull or validation fails, the running app is not stopped. If startup/readiness fails after replacement, the workflow reports failure but does not restore the old binary or roll back database changes. Inspect service logs and assess migration/database compatibility first; a manual previous-image redeploy is only safe after that assessment. No automatic database backup, destructive cleanup, or rollback is performed.

## Verification

Before publishing a release, run `actionlint .github/workflows/production-deployment.yml`, `shellcheck deployment/deploy.sh`, and `bash -n deployment/deploy.sh`.

The implementation was smoke-tested without a production deployment:

- The actual matrix validator accepted valid IPv4 and DNS targets and rejected eight unsafe or malformed variants, including unsupported bare IPv6.
- A disposable Compose project using the production health check became healthy when both endpoints succeeded, and failed readiness when `/ready` succeeded but `/health/db` returned HTTP 503. Its network and volume were removed afterward.
- Real GPG decryption and Compose configuration validation were exercised with throwaway credentials. A controlled Docker command harness checked pull-before-stop, no stop on pull failure, nonzero exit without rollback on readiness failure, private environment-file permissions, and cleanup. This is not evidence of live SSH or registry deployment.

Live GitHub App access, encrypted production configuration, SSH host verification, image publishing, and server rollout require the real operator-provisioned settings above. Verify those on the first approved release; never use production data for disposable smoke tests.
