#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

DEPLOY_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
ENV_FILE="$DEPLOY_DIR/.env"
ENCRYPTED_ENV="$DEPLOY_DIR/.env.gpg"
TEMP_ENV=""
TEMP_OUTPUT=""
TEMP_DOCKER_CONFIG=""
LOGGED_IN=false

cleanup() {
  status=$?
  trap - EXIT
  if [[ -n "$TEMP_ENV" ]]; then rm -f -- "$TEMP_ENV" || true; fi
  if [[ -n "$TEMP_OUTPUT" ]]; then rm -f -- "$TEMP_OUTPUT" || true; fi
  rm -f -- "$ENCRYPTED_ENV" || true
  if [[ "$LOGGED_IN" == true ]]; then docker logout ghcr.io >/dev/null 2>&1 || true; fi
  if [[ -n "$TEMP_DOCKER_CONFIG" ]]; then rm -rf -- "$TEMP_DOCKER_CONFIG" || true; fi
  exit "$status"
}
trap cleanup EXIT

fail() { printf 'Deployment failed: %s\n' "$1" >&2; exit 1; }

[[ "$DEPLOY_DIR" == */open-webui ]] || fail 'deployment directory must end in /open-webui'
[[ -n "${GPG_SECRET_PASSPHRASE:-}" ]] || fail 'GPG_SECRET_PASSPHRASE is required'
[[ -n "${GITHUB_TOKEN:-}" ]] || fail 'GITHUB_TOKEN is required'
[[ "${IMAGE_REPO:-}" == 'ghcr.io/wilopo-cargo/open-webui' ]] || fail 'IMAGE_REPO is not the production Open WebUI image'
[[ "${IMAGE_SHA:-}" =~ ^[0-9a-f]{40}$ ]] || fail 'IMAGE_SHA must be a resolved commit SHA'
for command in gpg docker grep awk mktemp mv rm; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is missing: $command"
done
[[ -f "$DEPLOY_DIR/docker-compose.yml" ]] || fail 'docker-compose.yml is missing'
[[ -f "$ENCRYPTED_ENV" ]] || fail 'encrypted production environment file is missing'

TEMP_ENV="$(mktemp "$DEPLOY_DIR/.env.XXXXXX")"
if ! printf '%s\n' "$GPG_SECRET_PASSPHRASE" | gpg --quiet --batch --yes --no-tty --pinentry-mode loopback \
  --passphrase-fd 0 --decrypt --output "$TEMP_ENV" "$ENCRYPTED_ENV"; then
  fail 'production environment decryption failed'
fi
chmod 0600 "$TEMP_ENV"
for name in WEBUI_SECRET_KEY DATABASE_URL WEBUI_URL APP_HOST; do
  [[ "$(grep -Ec "^${name}=" "$TEMP_ENV" || true)" == 1 ]] || fail "decrypted environment must define ${name} exactly once"
  grep -Eq "^${name}=.+$" "$TEMP_ENV" || fail "decrypted environment is missing required ${name}"
done
grep -Eq '^WEBUI_URL=https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[^[:space:]]*)?$' "$TEMP_ENV" || fail 'WEBUI_URL must be an absolute HTTPS URL'
grep -Eq '^APP_HOST=[A-Za-z0-9.-]+$' "$TEMP_ENV" || fail 'APP_HOST must be a DNS hostname'
grep -Eq '^DATABASE_URL=postgres(ql)?://[^[:space:]]+$' "$TEMP_ENV" || fail 'DATABASE_URL must name the dedicated PostgreSQL database'

# The release image is injected at deploy time; never trust a value in the encrypted env.
TEMP_OUTPUT="$(mktemp "$DEPLOY_DIR/.env.XXXXXX")"
awk '$0 !~ /^[[:space:]]*APP_IMAGE[[:space:]]*=/' "$TEMP_ENV" > "$TEMP_OUTPUT"
printf 'APP_IMAGE=%s:%s\n' "$IMAGE_REPO" "$IMAGE_SHA" >> "$TEMP_OUTPUT"
chmod 0600 "$TEMP_OUTPUT"
mv -f -- "$TEMP_OUTPUT" "$ENV_FILE"
TEMP_OUTPUT=""
rm -f -- "$TEMP_ENV"
TEMP_ENV=""
rm -f -- "$ENCRYPTED_ENV"

unset APP_HOST WEBUI_URL
export APP_IMAGE="${IMAGE_REPO}:${IMAGE_SHA}"
docker compose --env-file "$ENV_FILE" --project-name wilopo-open-webui-prod --file "$DEPLOY_DIR/docker-compose.yml" config --quiet || fail 'Compose configuration is invalid'
docker network inspect proxy >/dev/null 2>&1 || fail 'external proxy network is missing'
TEMP_DOCKER_CONFIG="$(mktemp -d)"
export DOCKER_CONFIG="$TEMP_DOCKER_CONFIG"
LOGGED_IN=true
printf '%s' "$GITHUB_TOKEN" | docker login ghcr.io --username wilopo-cargo --password-stdin || fail 'GHCR login failed'
docker compose --env-file "$ENV_FILE" --project-name wilopo-open-webui-prod --file "$DEPLOY_DIR/docker-compose.yml" pull app || fail 'image pull failed; running app was not stopped'
docker compose --env-file "$ENV_FILE" --project-name wilopo-open-webui-prod --file "$DEPLOY_DIR/docker-compose.yml" stop app || fail 'could not stop the existing app'
docker compose --env-file "$ENV_FILE" --project-name wilopo-open-webui-prod --file "$DEPLOY_DIR/docker-compose.yml" up -d --wait --wait-timeout 300 app || fail 'new app failed readiness; inspect logs and assess database migration compatibility before any rollback'
printf 'Production Open WebUI deployment is ready (%s).\n' "$IMAGE_SHA"
