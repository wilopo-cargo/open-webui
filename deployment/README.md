# Wilopo deployment and image publishing

## Active image publishing

`.github/workflows/docker.yaml` is the only active GitHub Actions workflow. It builds one standard `linux/amd64` image on `blacksmith-4vcpu-ubuntu-2404` and publishes only to `ghcr.io/wilopo-cargo/open-webui`, with branch/release tags and immutable `git-<commit-sha>` tags. Manual dispatch and pushes to `main`, `dev`, and `v*` tags are retained. ARM, CUDA, Ollama, and slim variants are not built; PyPI and Docker Hub publishing are not enabled.

The Blacksmith GitHub App must be enabled and installed for this repository before the job can run. Runner availability has not been verified, and this workflow has no GitHub-hosted runner fallback. The Docker frontend build uses an 8 GiB V8 heap. For pre-merge verification, dispatch `docker.yaml` on a feature branch; it publishes that branch's image to GHCR.

## Disabled production deployment reference

The production deployment workflow is disabled at `.github/workflows/production-deployment.yml.disabled`; it does not run automatically or deploy. The setup and recovery guidance below describes how that workflow was designed and is reference material only. Re-enabling production deployment requires deliberately restoring an active workflow and reviewing its configuration.

### GitHub setup for the disabled workflow

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

These settings apply only if the disabled production workflow is deliberately re-enabled. Missing configuration fails that workflow; it never falls back to another application's credentials.

## Disabled workflow: server and application prerequisites

Provision each host and its dedicated `/.../open-webui` directory before deployment. The SSH account must be able to run Docker and Docker Compose, and the host must have `gpg`, `docker`, and the Docker Compose plugin installed. Create the external Docker network named `proxy` and ensure the existing Traefik proxy watches it, accepts `websecure`, and has the `cloudflare` certificate resolver configured. The deployment script checks that `proxy` exists before pulling or stopping the app. The app listens on `PORT` from the encrypted runtime environment (default `8081`; set `PORT=8082` to change it). The app listener, exposed container port, Traefik backend, and readiness checks all use that value. It publishes no host port; users connect through Traefik HTTPS.

Create a dedicated PostgreSQL database and login for this Open WebUI installation. The encrypted runtime environment must define nonempty `WEBUI_SECRET_KEY`, `DATABASE_URL`, `WEBUI_URL`, and `APP_HOST` values. `WEBUI_SECRET_KEY` must be a long-lived stable key; changing it can invalidate sessions and tokens. `DATABASE_URL` must use the dedicated PostgreSQL database. `WEBUI_URL` must be the application's HTTPS URL and `APP_HOST` its DNS host. Keep this environment file private; deployment never prints its contents. The deployment script decrypts it under `umask 077`, validates required settings and Compose configuration, then stores it as mode `0600` before any existing app is stopped.

For an existing installation, preserve its effective signing key, database, and data volume rather than treating this as a fresh install. An existing persisted WebUI URL can override the environment default: set the production URL through Admin → Settings before using Composio callbacks. This workflow does not move or change the laptop installation.

Open WebUI 0.13.0 includes database migrations for resource comments, persistent tool enabled states, and notifications. Before upgrading, take and verify a restorable backup of the database and all associated application data, including the named volume and any external file, object, or vector stores in use. Stop every instance and worker before starting the upgraded image. Upgrade all instances together; mixed-version rolling updates are unsupported and can fail because the schemas differ. If rollback is necessary after migrations run, restore the matching database and associated data backup before starting the previous image.

Compose uses project name `wilopo-open-webui-prod`, one `app` service, and the dedicated named `open_webui_data` volume. It does not start Ollama, run a separate migration service, or remove volumes/images. Do not share this project name or data volume with another application.

## Disabled workflow: deployment and recovery reference

If deliberately re-enabled after review and setup, publishing the matching production Release runs the workflow. There is no deploy-on-push or scheduled deployment. It validates the release/repository guard, builds `ghcr.io/wilopo-cargo/open-webui:<resolved-commit-sha>` with the `production-buildcache` cache reference, and deploys only that immutable image. The encrypted environment and deployment files are copied to the dedicated server directory using the same pinned SSH fingerprint for SCP and SSH.

The host logs in to GHCR using the workflow token, pulls the image before stopping the current app, then stops the single app and starts it with Compose `--wait` (up to 300 seconds). Health requires both `/ready` and `/health/db`; the container allows a five-minute startup grace period for migrations. If pull or validation fails, the running app is not stopped. If startup/readiness fails after replacement, the workflow reports failure but does not restore the old binary or roll back database changes. Inspect service logs and assess migration/database compatibility first; a manual previous-image redeploy is only safe after that assessment. No automatic database backup, destructive cleanup, or rollback is performed. Restore the matching pre-upgrade database and associated data backup before redeploying the previous image if migrations ran; never assume an image-only rollback is safe.

## Fork upgrade procedure

Start from the current fork `wilopo/main`. In the maintainer checkout, `origin` points to `open-webui/open-webui`; its local `main` is not the Wilopo fork base. Fetch the exact stable tag and merge it with ancestry so future upgrades share the upstream history:

```bash
git fetch wilopo main
git fetch origin tag v0.13.0
git worktree add -b chore/upgrade-openwebui-v0.13.0 ../open-webui-upgrade-v0.13.0 wilopo/main
git -C ../open-webui-upgrade-v0.13.0 merge --no-ff --no-commit v0.13.0
```

Resolve conflicts without dropping per-user Composio sessions and policies, ERP MCP static OAuth and owned current-turn CSV handoff, or deferred Tool Search. Keep upstream security fixes and MCP session isolation. Review added upstream workflow files as well as conflicts: `docker.yaml` must remain the sole active workflow, publishing only the standard GHCR `linux/amd64` image on Blacksmith. Keep production deployment, releases, and all other workflows disabled.

Use separate installed environments for the new worktree. Do not update another checkout's dependencies, load its private environment files, or use its application data for verification. Build the frontend, run the integration suite below, parse the locale JSON files, and check the workflow policy. Start the built application against fresh disposable SQLite data and against a separate copy seeded by the previous fork version. Check `/health`, `/ready`, `/health/db`, `/api/version`, and frontend assets, then check the interface in a browser. Keep migration testing separate from real databases and external accounts.

After verification, update this document with results and limitations and commit the merge locally. Pushing, opening or merging a PR, publishing an image, and deploying are separate operator actions.

## Verification

### Open WebUI 0.13.0 upgrade

This fork merges upstream [v0.13.0](https://github.com/open-webui/open-webui/releases/tag/v0.13.0), commit `c38885a1a737da4968f157092f304f3b5481d0bf`, onto Wilopo fork main `8ec2e4d7f7b440528db73b67eba32011f2ceb510`. Native Composio connections retain their verified-user sessions, fixed provider endpoints, TLS verification, and scoped dependency-log filtering. The custom HTTP client factory now follows upstream's isolated MCP session task so failed MCP requests do not cancel the chat task.

Local pre-merge verification on 2026-11-06 passed:

- `npm ci --no-audit --no-fund` and `npm run build` using Node 22.23.3. The encryption artifact check, Pyodide preparation, and production frontend build completed.
- All 232 Composio/ERP integration checks, including loopback MCP HTTP transport, disconnected transport isolation, untrusted TLS rejection, per-user OAuth, deferred dispatch, and CSV ownership/security cases. Run them in a fresh process with `PYTHONPATH=backend PYTHON_DOTENV_DISABLED=1 .venv/bin/python -m pytest -q backend/tests/test_composio_tool_router.py` from the repository root. Test providers and saved tool-call fixtures use the 0.13.0 MCP and execution contracts. Five dependency deprecation warnings remain.
- Parsing all 64 locale files and structurally checking that the unchanged `docker.yaml` is the only enabled workflow, with the Blacksmith runner, one `linux/amd64` build, and GHCR-only login.
- Starting the application with a fresh isolated SQLite database and migrating a separate disposable 0.12.4 database through `0bd252b206fb` → `c41f8d9e2a10` → `6f8c2a91d4e7` → `e42c8f710a36`. The upgraded copy passed SQLite integrity checks. Login, user ID, chat/message content, tool source and its enabled default, disabled Composio/ERP configurations, and deferred Tool Search settings survived the upgrade.
- Both fresh and migrated applications returned HTTP 200 from `/health`, `/ready`, and `/health/db`, and `/api/version` reported `0.13.0`. The built frontend HTML and all 81 referenced assets returned HTTP 200; both application entry modules were nonempty. This is HTTP smoke evidence; interactive browser verification is a separate check.

These checks used a cloned Python 3.11 environment and disposable local data with dotenv loading, model APIs, telemetry, and remote model downloads disabled. They do not verify production PostgreSQL migration or backup restoration, real Composio/ERP consent or tool execution, model inference, image publication, or production rollout. Production deployment remains disabled.


For the disabled workflow only, run `actionlint .github/workflows/production-deployment.yml.disabled`, `shellcheck deployment/deploy.sh`, and `bash -n deployment/deploy.sh` before deliberately re-enabling it.

The disabled workflow implementation was smoke-tested without a production deployment:

- The actual matrix validator accepted valid IPv4 and DNS targets and rejected eight unsafe or malformed variants, including unsupported bare IPv6.
- A disposable Compose project using the production health check became healthy when both endpoints succeeded, and failed readiness when `/ready` succeeded but `/health/db` returned HTTP 503. Its network and volume were removed afterward.
- Real GPG decryption and Compose configuration validation were exercised with throwaway credentials. A controlled Docker command harness checked pull-before-stop, no stop on pull failure, nonzero exit without rollback on readiness failure, private environment-file permissions, and cleanup. This is not evidence of live SSH or registry deployment.

Live GitHub App access, encrypted production configuration, SSH host verification, image publishing, and server rollout require the real operator-provisioned settings above. This is reference material for the disabled workflow, not evidence of current deployment capability. Never use production data for disposable smoke tests.
