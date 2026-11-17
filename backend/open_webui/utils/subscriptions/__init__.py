"""User-scoped subscription credentials; never part of the shared model registry."""

import asyncio
import base64
import copy
import hashlib
import importlib
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlsplit

from fastapi import HTTPException
from open_webui.internal.db import get_async_db_context
from open_webui.models.access_grants import AccessGrant, AccessGrants
from open_webui.models.groups import Group, Groups
from open_webui.models.models import Models
from open_webui.models.oauth_sessions import OAuthSession, OAuthSessions
from open_webui.models.users import User
from sqlalchemy import delete, select, text

PROVIDERS = ('codex', 'claude')
SESSION_PROVIDERS = tuple(f'subscription:{provider}' for provider in PROVIDERS)
PENDING_PROVIDERS = tuple(f'subscription-pending:{provider}' for provider in PROVIDERS)
RESOURCE_TYPE = 'subscription_connection'


def get_provider(provider):
    if provider not in PROVIDERS:
        raise HTTPException(400, 'Unsupported subscription provider')
    return importlib.import_module(f'open_webui.utils.subscriptions.{provider}')


def is_subscription_model(model_id):
    return isinstance(model_id, str) and model_id.startswith('subscription:')


def parse_model_id(model_id):
    parts = model_id.split(':', 2) if is_subscription_model(model_id) else []
    if len(parts) != 3 or not parts[1] or not parts[2]:
        raise HTTPException(404, 'Subscription model not found')
    return parts[1], parts[2]


def subscription_base_id(model_id, model_infos):
    """Resolve presets using server records, not client model_item metadata."""
    seen = set()
    while model_id and model_id not in seen:
        if is_subscription_model(model_id):
            return model_id
        seen.add(model_id)
        info = model_infos.get(model_id)
        model_id = info.base_model_id if info else None
    return None


def _metadata(token):
    return token.get('connection') or {}


def _can_manage(row, token, user):
    return row.user_id == user.id or (_metadata(token).get('scope') == 'shared' and user.role == 'admin')


async def _can_use(db, row, token, user, group_ids=None):
    if row.user_id == user.id:
        return True
    if _metadata(token).get('scope') != 'shared':
        return False
    if group_ids is None:
        group_ids = {group.id for group in await Groups.get_groups_by_member_id(user.id, include_inherited=True)}
    grants = (await db.execute(select(AccessGrant).filter_by(resource_type=RESOURCE_TYPE, resource_id=row.id))).scalars()
    # Deliberately do not use generic has_access: it accepts public user:* grants.
    return any(
        grant.permission == 'read'
        and grant.principal_id != '*'
        and (
            (grant.principal_type == 'user' and grant.principal_id == user.id)
            or (grant.principal_type == 'group' and grant.principal_id in group_ids)
        )
        for grant in grants
    )


async def validate_grants(db, grants):
    normalized = []
    seen = set()
    for grant in grants:
        grant = dict(grant)
        kind, principal, permission = (grant.get(key) for key in ('principal_type', 'principal_id', 'permission'))
        if kind not in ('user', 'group') or not principal or principal == '*' or permission != 'read':
            raise HTTPException(400, 'Sharing requires explicit users or groups with read permission')
        table = User if kind == 'user' else Group
        if await db.scalar(select(table.id).filter_by(id=principal)) is None:
            raise HTTPException(400, 'Sharing principal does not exist')
        if (kind, principal) not in seen:
            normalized.append({'principal_type': kind, 'principal_id': principal, 'permission': 'read'})
            seen.add((kind, principal))
    return normalized


async def _summary(db, row, token, user):
    connection = _metadata(token)
    manager = _can_manage(row, token, user)
    grants = []
    if manager:
        grants = [
            {'principal_type': grant.principal_type, 'principal_id': grant.principal_id, 'permission': grant.permission}
            for grant in (await db.execute(select(AccessGrant).filter_by(resource_type=RESOURCE_TYPE, resource_id=row.id))).scalars()
        ]
    return {
        'id': row.id,
        'provider': row.provider.split(':', 1)[1],
        'name': connection['name'],
        'scope': connection['scope'],
        'enabled': connection.get('enabled', True),
        'status': 'reauth_required' if connection.get('reauth_required') else 'connected',
        'expires_at': row.expires_at,
        'models': [{'id': model['id'], 'name': model['name']} for model in connection.get('models', [])],
        'can_manage': manager,
        'access_grants': grants,
    }


def _save(row, token):
    row.token = OAuthSessions._encrypt_token(token)
    row.expires_at = token['expires_at']
    row.updated_at = int(time.time())


def _normalize_token(result, previous=None):
    if not isinstance(result, dict) or not isinstance(result.get('access_token'), str) or not result['access_token']:
        raise HTTPException(502, 'Provider returned no access token; reconnect the subscription')
    token = {**(previous or {}), **result}
    if previous and not result.get('refresh_token'):
        token['refresh_token'] = previous.get('refresh_token')
    try:
        token['expires_at'] = int(result['expires_at']) if result.get('expires_at') else int(time.time()) + int(result['expires_in'])
    except (KeyError, TypeError, ValueError, OverflowError):
        raise HTTPException(502, 'Provider returned no valid token expiry; reconnect the subscription') from None
    if token['expires_at'] <= int(time.time()):
        raise HTTPException(502, 'Provider returned an expired token; reconnect the subscription')
    if previous:
        token['connection'] = previous['connection']
    return token


@asynccontextmanager
async def _transaction():
    async with get_async_db_context() as db:
        if db.get_bind().dialect.name == 'sqlite':
            # ponytail: SQLite serializes ALL connection writes during refresh; use PostgreSQL row locks for production throughput.
            await db.execute(text('BEGIN IMMEDIATE'))
        try:
            yield db
        except BaseException:
            await db.rollback()
            raise


@asynccontextmanager
async def _locked_connection(connection_id, user, *, manage=False, require_enabled=True):
    async with _transaction() as db:
        row = (await db.execute(select(OAuthSession).filter(
            OAuthSession.id == connection_id, OAuthSession.provider.in_(SESSION_PROVIDERS)
        ).with_for_update())).scalars().first()
        if row is None:
            raise HTTPException(404, 'Subscription connection not found')
        token = OAuthSessions._decrypt_token(row.token)
        allowed = _can_manage(row, token, user) if manage else await _can_use(db, row, token, user)
        if not allowed:
            raise HTTPException(404, 'Subscription connection not found')
        if require_enabled and not _metadata(token).get('enabled', True):
            raise HTTPException(409, 'Subscription connection is disabled')
        yield db, row, token


async def _valid_token(db, row, token):
    connection = _metadata(token)
    if connection.get('reauth_required'):
        raise HTTPException(401, 'Subscription requires reconnection')
    if row.expires_at > time.time() + 60:
        return token
    if not token.get('refresh_token'):
        connection['reauth_required'] = True
        _save(row, token)
        await db.commit()
        raise HTTPException(401, 'Subscription requires reconnection')
    try:
        refreshed = await get_provider(row.provider.split(':', 1)[1]).refresh_token(token['refresh_token'])
        token = _normalize_token(refreshed, token)
    except HTTPException as exc:
        if exc.status_code in (400, 401, 403):
            connection['reauth_required'] = True
            _save(row, token)
            await db.commit()
            raise HTTPException(401, 'Subscription requires reconnection') from None
        raise HTTPException(502, 'Subscription token refresh failed; try again later') from None
    except Exception:
        raise HTTPException(502, 'Subscription token refresh failed; try again later') from None
    _save(row, token)
    # The caller holds the row lock until this rotated token is committed.
    await db.flush()
    return token


async def _connection_token(connection_id, user, *, manage=False, require_enabled=True):
    async def refresh_and_commit():
        async with _locked_connection(connection_id, user, manage=manage, require_enabled=require_enabled) as (db, row, token):
            token = await _valid_token(db, row, token)
            await db.commit()
            return row.provider.split(':', 1)[1], token

    # OAuth refresh can spend the old token before cancellation arrives. Keep
    # the lock, exchange, and durable commit together, then propagate chat Stop.
    operation = asyncio.create_task(refresh_and_commit())
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(operation)
            break
        except asyncio.CancelledError:
            if operation.cancelled():
                raise
            cancelled = True
        except Exception:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
    if cancelled:
        raise asyncio.CancelledError
    return result


async def authorize(user, provider, name, scope, grants):
    module = get_provider(provider)
    if scope == 'shared' and user.role != 'admin':
        raise HTTPException(403, 'Only administrators can create shared connections')
    if scope == 'personal' and grants:
        raise HTTPException(400, 'Personal connections cannot be shared')
    verifier = secrets.token_urlsafe(48)
    state = secrets.token_urlsafe(32)
    expires_at = int(time.time()) + 600
    async with _transaction() as db:
        grants = await validate_grants(db, grants)
        await db.execute(delete(OAuthSession).filter(OAuthSession.provider.in_(PENDING_PROVIDERS), OAuthSession.expires_at <= int(time.time())))
        db.add(OAuthSession(
            id=state, user_id=user.id, provider=f'subscription-pending:{provider}',
            token=OAuthSessions._encrypt_token({'verifier': verifier, 'state': state, 'expires_at': expires_at, 'connection': {'name': name, 'scope': scope, 'enabled': True, 'models': []}, 'access_grants': grants}),
            expires_at=expires_at, created_at=int(time.time()), updated_at=int(time.time()),
        ))
        await db.commit()
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
    return {'state': state, 'authorization_url': module.authorization_url(state, challenge), 'redirect_uri': module.OAUTH['redirect_uri'], 'expires_at': expires_at}


def _parse_callback(callback, redirect_uri):
    callback = callback.strip()
    if '://' in callback:
        returned = urlsplit(callback)
        expected = urlsplit(redirect_uri)
        if (returned.scheme, returned.netloc, returned.path) != (expected.scheme, expected.netloc, expected.path):
            raise HTTPException(400, 'Callback URL does not match the provider redirect')
        params = parse_qs(returned.query, keep_blank_values=True)
        if params.get('error'):
            raise HTTPException(400, 'Provider authorization was declined; start again')
        code, state = params.get('code', []), params.get('state', [])
        if len(code) != 1 or len(state) != 1:
            raise HTTPException(400, 'Paste the full callback URL including code and state')
        return code[0], state[0]
    parts = callback.split('#')
    if len(parts) == 2 and all(parts):
        return tuple(parts)
    raise HTTPException(400, 'Paste the full callback URL or code#state')


async def complete(user, state, callback):
    # Lock plus conditional delete makes PKCE consumption single-use across workers.
    async with _transaction() as db:
        pending = (await db.execute(select(OAuthSession).filter(
            OAuthSession.id == state, OAuthSession.user_id == user.id, OAuthSession.provider.in_(PENDING_PROVIDERS),
            OAuthSession.expires_at > int(time.time()),
        ).with_for_update())).scalars().first()
        if pending is None or pending.expires_at <= int(time.time()):
            raise HTTPException(400, 'Authorization expired or already used; start again')
        provider = pending.provider.split(':', 1)[1]
        module = get_provider(provider)
        code, returned_state = _parse_callback(callback, module.OAUTH['redirect_uri'])
        token = OAuthSessions._decrypt_token(pending.token)
        if not code or not secrets.compare_digest(returned_state.encode(), state.encode()) or not secrets.compare_digest(token['state'].encode(), state.encode()):
            raise HTTPException(400, 'Authorization state does not match')
        if token['connection']['scope'] == 'shared' and user.role != 'admin':
            raise HTTPException(403, 'Only administrators can create shared connections')
        grants = await validate_grants(db, token['access_grants'])
        consumed = await db.execute(delete(OAuthSession).filter_by(id=state, user_id=user.id, provider=pending.provider))
        if consumed.rowcount != 1:
            raise HTTPException(400, 'Authorization expired or already used; start again')
        await db.commit()
    try:
        result = await module.exchange_code(code, state, token['verifier'])
        connected = _normalize_token(result)
    except Exception:
        raise HTTPException(502, 'Provider authorization exchange failed; start a new connection') from None
    connected['connection'] = token['connection']
    connection_id = str(uuid.uuid4())
    async with _transaction() as db:
        row = OAuthSession(id=connection_id, user_id=user.id, provider=f'subscription:{provider}', token='', expires_at=connected['expires_at'], created_at=int(time.time()), updated_at=int(time.time()))
        _save(row, connected)
        db.add(row)
        await AccessGrants.replace_access_grants(db, RESOURCE_TYPE, row.id, grants)
        await db.commit()
    # Persist credentials before discovery: a catalog outage must not discard valid consent.
    try:
        return await refresh_models(user, connection_id)
    except HTTPException as exc:
        if exc.status_code == 401:
            raise
        raise HTTPException(502, {'message': 'Subscription connected, but model discovery failed. Refresh models in Connections.', 'connection_id': connection_id}) from None


async def list_connections(user):
    groups = {group.id for group in await Groups.get_groups_by_member_id(user.id, include_inherited=True)}
    async with get_async_db_context() as db:
        rows = (await db.execute(select(OAuthSession).filter(OAuthSession.provider.in_(SESSION_PROVIDERS)).order_by(OAuthSession.created_at))).scalars()
        result = []
        for row in rows:
            token = OAuthSessions._decrypt_token(row.token)
            if _can_manage(row, token, user) or await _can_use(db, row, token, user, groups):
                result.append(await _summary(db, row, token, user))
        return result


async def update_connection(user, connection_id, changes):
    async with _locked_connection(connection_id, user, manage=True, require_enabled=False) as (db, row, token):
        connection = _metadata(token)
        if 'access_grants' in changes:
            if connection['scope'] != 'shared':
                raise HTTPException(400, 'Personal connections cannot be shared')
            grants = await validate_grants(db, changes['access_grants'])
            await AccessGrants.replace_access_grants(db, RESOURCE_TYPE, row.id, grants)
        for key in ('name', 'enabled'):
            if key in changes:
                connection[key] = changes[key]
        _save(row, token)
        await db.commit()
        return await _summary(db, row, token, user)


async def delete_connection(user, connection_id):
    async with _locked_connection(connection_id, user, manage=True, require_enabled=False) as (db, row, token):
        await db.execute(delete(AccessGrant).filter_by(resource_type=RESOURCE_TYPE, resource_id=row.id))
        await db.delete(row)
        await db.commit()
    return {'status': True}


async def refresh_models(user, connection_id):
    provider, token = await _connection_token(connection_id, user, manage=True, require_enabled=False)
    module = get_provider(provider)
    try:
        catalog = await module.list_models(token)
        if not isinstance(catalog, list) or not catalog:
            raise ValueError('empty catalog')
        models = []
        for model in catalog:
            if not isinstance(model, dict) or not isinstance(model.get('id'), str) or not model['id']:
                raise ValueError('invalid catalog')
            capabilities = model.get('capabilities') or {}
            models.append({'id': model['id'], 'name': model.get('name') or model['id'], 'capabilities': {'vision': bool(capabilities.get('vision', False)), 'builtin_tools': True, 'usage': True}})
    except HTTPException as exc:
        if exc.status_code in (401, 403):
            async with _locked_connection(connection_id, user, manage=True, require_enabled=False) as (db, row, current):
                if current['access_token'] == token['access_token']:
                    _metadata(current)['reauth_required'] = True
                    _save(row, current)
                    await db.commit()
            raise HTTPException(401, 'Subscription requires reconnection') from None
        raise HTTPException(502, 'Provider model discovery failed; refresh models in Connections') from None
    except Exception:
        raise HTTPException(502, 'Provider model discovery failed; refresh models in Connections') from None
    async with _locked_connection(connection_id, user, manage=True, require_enabled=False) as (db, row, token):
        _metadata(token)['models'] = models
        _save(row, token)
        await db.commit()
        return await _summary(db, row, token, user)


def _model(row, token, catalog_model):
    raw_id = catalog_model['id']
    return {
        'id': f'subscription:{row.id}:{raw_id}', 'name': f'{_metadata(token)["name"]} · {catalog_model["name"]}',
        'object': 'model', 'created': row.created_at, 'owned_by': 'subscription', 'connection_type': 'external',
        'subscription': {'connection_id': row.id, 'provider': row.provider.split(':', 1)[1], 'model': raw_id},
        'info': {'meta': {'capabilities': {**catalog_model.get('capabilities', {}), 'builtin_tools': True}}},
    }


async def get_subscription_models(user):
    groups = {group.id for group in await Groups.get_groups_by_member_id(user.id, include_inherited=True)}
    async with get_async_db_context() as db:
        rows = (await db.execute(select(OAuthSession).filter(OAuthSession.provider.in_(SESSION_PROVIDERS)))).scalars()
        models = []
        for row in rows:
            token = OAuthSessions._decrypt_token(row.token)
            connection = _metadata(token)
            if connection.get('enabled', True) and not connection.get('reauth_required') and await _can_use(db, row, token, user, groups):
                models.extend(_model(row, token, item) for item in connection.get('models', []))
        return models


async def resolve_subscription_model(user, model_id):
    connection_id, raw_id = parse_model_id(model_id)
    async with _locked_connection(connection_id, user) as (db, row, token):
        if _metadata(token).get('reauth_required'):
            raise HTTPException(401, 'Subscription requires reconnection')
        item = next((item for item in _metadata(token).get('models', []) if item['id'] == raw_id), None)
        if item is None:
            raise HTTPException(404, 'Subscription model not found; refresh models in Connections')
        return _model(row, token, item)


async def _check_preset_access(user, model_id, infos):
    from open_webui.config import BYPASS_ADMIN_ACCESS_CONTROL
    from open_webui.env import BYPASS_MODEL_ACCESS_CONTROL

    bypass = BYPASS_MODEL_ACCESS_CONTROL or (user.role == 'admin' and BYPASS_ADMIN_ACCESS_CONTROL)
    seen = set()
    while model_id and not is_subscription_model(model_id):
        if model_id in seen or model_id not in infos:
            raise HTTPException(404, 'Subscription preset not found')
        seen.add(model_id)
        info = infos[model_id]
        if not info.is_active or (not bypass and user.id != info.user_id and not await AccessGrants.has_access(
            user.id, 'model', info.id, 'read'
        )):
            raise HTTPException(404, 'Subscription preset not found')
        model_id = info.base_model_id


async def assert_model_access(user, model_id, infos=None):
    if infos is None:
        infos = {}
        current = model_id
        while current and not is_subscription_model(current) and current not in infos:
            info = await Models.get_model_by_id(current)
            if info is None:
                break
            infos[current] = info
            current = info.base_model_id
    base_id = subscription_base_id(model_id, infos)
    if base_id is None:
        return None
    await _check_preset_access(user, model_id, infos)
    return await resolve_subscription_model(user, base_id)


async def get_model_overlay(request, user):
    """A fresh local dictionary, including presets; no user catalog enters app/Redis state."""
    from open_webui.utils.chat_variables import get_chat_variables_schema
    from open_webui.utils.models import public_model_params

    models = dict(request.app.state.MODELS.items())
    models.update((model['id'], model) for model in await get_subscription_models(user))
    infos = {info.id: info for info in await Models.get_all_models()}
    for model_id, info in infos.items():
        base_id = subscription_base_id(model_id, infos)
        if not base_id:
            continue
        base = models.get(base_id)
        models.pop(model_id, None)
        if not info.is_active or base is None:
            continue
        try:
            await _check_preset_access(user, model_id, infos)
        except HTTPException:
            continue
        value = copy.deepcopy(base)
        value.update(id=info.id, name=info.name, preset=True, info=info.model_dump())
        provider_capabilities = base['info']['meta']['capabilities']
        value['info']['meta'] = {**(value['info'].get('meta') or {}), 'capabilities': {**((value['info'].get('meta') or {}).get('capabilities') or {}), **provider_capabilities}}
        value['info']['params'] = public_model_params(info.params.model_dump())
        schema = get_chat_variables_schema(info.params.model_dump().get('system'))
        if schema:
            value['info']['meta']['chat_variables_schema'] = schema
        models[model_id] = value
    if getattr(request.state, 'direct', False) and hasattr(request.state, 'model'):
        direct_model = request.state.model
        if subscription_base_id(direct_model.get('id'), infos) is None:
            models[direct_model['id']] = direct_model
    return models


async def generate_chat_completion(request, form_data, user, model, *, bypass_system_prompt=False):
    from open_webui.utils.payload import apply_model_params_to_body_openai, apply_system_prompt_to_body

    # Metadata owns live MCP/tool handles; copy only the JSON provider payload.
    metadata = form_data.get('metadata') or {}
    payload = copy.deepcopy({key: value for key, value in form_data.items() if key != 'metadata'})
    infos = {info.id: info for info in await Models.get_all_models()}
    selected = payload['model']
    base_id = subscription_base_id(selected, infos)
    if base_id is None:
        raise HTTPException(404, 'Subscription model not found')
    # Every dispatch rechecks durable ownership, grants, enabled state, and catalog membership.
    resolved = await assert_model_access(user, selected, infos)
    info = infos.get(selected)
    if info:
        params = info.params.model_dump()
        system = params.pop('system', None)
        payload = apply_model_params_to_body_openai(params, payload)
        if not bypass_system_prompt:
            payload = await apply_system_prompt_to_body(system, payload, metadata, user)
    connection_id, raw_id = parse_model_id(base_id)
    _, token = await _connection_token(connection_id, user)
    payload['model'] = raw_id
    payload.pop('model_item', None)
    payload.pop('params', None)
    session_id = str(metadata.get('chat_id') or metadata.get('session_id') or request.headers.get('x-request-id') or uuid.uuid4())
    module = get_provider(resolved['subscription']['provider'])
    try:
        return await module.generate_chat_completion(payload, token, session_id)
    except HTTPException as exc:
        if exc.status_code in (401, 403):
            async with _locked_connection(connection_id, user, require_enabled=False) as (db, row, current):
                if current['access_token'] == token['access_token']:
                    _metadata(current)['reauth_required'] = True
                    _save(row, current)
                    await db.commit()
            raise HTTPException(401, 'Subscription requires reconnection') from None
        raise
