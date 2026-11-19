"""Disposable-DB subscription usage authorization and loopback HTTP smoke.

Run from the repository root with the configured test interpreter:
  /Users/macbook/Desktop/repositories/open-webui-upgrade-v0.13.3/.venv/bin/python backend/tests/test_subscription_usage.py
No credentials, app lifespan, or external provider calls.
"""

import asyncio
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def test_subscription_usage():
    subprocess.run([sys.executable, str(Path(__file__).resolve())], check=True)


async def _check():
    import httpx
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from fastapi import FastAPI, HTTPException, Request
    from open_webui.internal.db import async_engine, get_async_db_context
    from open_webui.models.access_grants import AccessGrant
    from open_webui.models.oauth_sessions import OAuthSession, OAuthSessions
    from open_webui.models.users import User, UserModel
    from open_webui.routers import subscriptions as router
    from open_webui.utils.auth import get_verified_user
    from open_webui.utils.session_pool import close_session
    from open_webui.utils.subscriptions import _save, claude, codex

    async with async_engine.begin() as db:
        await db.run_sync(lambda connection: User.metadata.create_all(
            connection, tables=[User.__table__, AccessGrant.__table__, OAuthSession.__table__]
        ))

    users = {
        name: UserModel(id=name, email=f'{name}@example.test', name=name, role=role,
                        last_active_at=0, updated_at=0, created_at=0)
        for name, role in [('alice', 'user'), ('bob', 'user'), ('owner-admin', 'admin'), ('other-admin', 'admin')]
    }
    async with get_async_db_context() as db:
        db.add_all([User(id=user.id, email=user.email, name=user.name, role=user.role,
                         last_active_at=0, updated_at=0, created_at=0) for user in users.values()])
        await db.commit()

    app = FastAPI()
    async def authenticated(request: Request):
        user = users.get(request.headers.get('x-test-user'))
        if user is None:
            raise HTTPException(401, 'Unauthenticated')
        return user

    app.dependency_overrides[get_verified_user] = authenticated
    app.include_router(router.router, prefix='/api/v1/subscriptions')

    claims_a = base64.urlsafe_b64encode(json.dumps({
        'https://api.openai.com/auth': {'chatgpt_account_id': 'acct-secret-one'},
    }).encode()).decode().rstrip('=')
    claims_b = base64.urlsafe_b64encode(json.dumps({
        'https://api.openai.com/auth': {'chatgpt_account_id': 'acct-secret-two'},
    }).encode()).decode().rstrip('=')
    wire = []
    responses = {'codex': (200, {'rate_limit': {'primary_window': {'used_percent': 0}}}),
                 'claude': (200, {'five_hour': {'utilization': 0}})}
    redirects = 0
    redirect_target_hits = 0

    async def usage_endpoint(request):
        nonlocal redirects
        provider = request.match_info['provider']
        assert not request.headers.get('Cookie')
        assert request.headers['Authorization'] in ('Bearer access-one', 'Bearer access-two', 'Bearer claude-access')
        if provider == 'codex':
            account = {'Bearer access-one': 'acct-secret-one', 'Bearer access-two': 'acct-secret-two'}[request.headers['Authorization']]
            assert request.headers['ChatGPT-Account-ID'] == account
        else:
            assert request.headers['Anthropic-Beta'] == 'oauth-2025-04-20'
        wire.append((provider, request.headers['Authorization'], request.headers.get('ChatGPT-Account-ID'),
                     request.headers.get('Cookie')))
        status, payload = responses[provider]
        if payload == 'redirect':
            redirects += 1
            raise web.HTTPFound('/redirect-target')
        if payload == 'invalid-json':
            return web.Response(body=b'{not-json', status=status)
        if payload == 'oversized':
            return web.Response(body=b' ' * (1024 * 1024 + 1), status=status)
        if payload == 'non-object':
            return web.Response(text='[]', status=status, content_type='application/json')
        if isinstance(payload, bytes):
            return web.Response(body=payload, status=status, content_type='application/json')
        return web.json_response(payload, status=status, headers={'Set-Cookie': 'provider-session=loopback-secret; Path=/'})

    async def redirect_target(request):
        nonlocal redirect_target_hits
        redirect_target_hits += 1
        return web.json_response({'five_hour': {'utilization': 99}})
    provider_app = web.Application()
    provider_app.router.add_get('/{provider}/usage', usage_endpoint)
    provider_app.router.add_get('/redirect-target', redirect_target)

    sequence = 0
    async def add_connection(owner, provider='codex', scope='personal', *, enabled=True, token=None, grants=()):
        nonlocal sequence
        sequence += 1
        connection_id = f'connection-{sequence}'
        credential = token or {'access_token': 'access-one', 'expires_at': int(time.time()) + 3600}
        if provider == 'codex':
            credential['id_token'] = 'header.' + (claims_b if credential['access_token'] == 'access-two' else claims_a) + '.signature'
        credential['connection'] = {'name': f'connection name {sequence}', 'scope': scope,
                                    'enabled': enabled, 'priority': sequence, 'models': [{'id': 'catalog-secret'}]}
        async with get_async_db_context() as db:
            row = OAuthSession(id=connection_id, user_id=owner, provider=f'subscription:{provider}', token='',
                               expires_at=credential['expires_at'], created_at=0, updated_at=0)
            _save(row, credential)
            db.add(row)
            for principal in grants:
                db.add(AccessGrant(id=f'grant-{connection_id}-{principal}', resource_type='subscription_connection',
                                   resource_id=connection_id, principal_type='user', principal_id=principal,
                                   permission='read', created_at=0))
            await db.commit()
        return connection_id

    async with TestServer(provider_app) as server:
        def local_url(provider):
            return str(server.make_url(f'/{provider}/usage')).replace('127.0.0.1', 'localhost')
        codex.USAGE_URL = local_url('codex')
        claude.USAGE_URL = local_url('claude')
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://webui.test') as client:
            async def get(connection_id, user):
                response = await client.get(f'/api/v1/subscriptions/{connection_id}/usage', headers={'x-test-user': user})
                assert all(secret not in response.text for secret in ('access-one', 'access-two', 'claude-access',
                                                                       'acct-secret-one', 'acct-secret-two', 'catalog-secret', connection_id))
                return response

            personal = await add_connection('alice')
            personal_owner = await get(personal, 'alice')
            assert personal_owner.status_code == 200 and personal_owner.json()['status'] == 'available'
            assert personal_owner.json()['windows'] == [{'id': 'primary_window', 'label': 'Session', 'used_percent': 0, 'resets_at': None}]
            for user in ('bob', 'owner-admin', 'other-admin'):
                assert (await get(personal, user)).status_code == 404

            shared = await add_connection('owner-admin', scope='shared', grants=('bob',))
            assert (await get(shared, 'owner-admin')).status_code == 200
            assert (await get(shared, 'other-admin')).status_code == 200
            assert (await get(shared, 'bob')).status_code == 404

            disabled = await add_connection('alice', enabled=False)
            assert (await get(disabled, 'alice')).status_code == 200

            # Provider methods make genuine HTTP requests and derive account headers from each JWT.
            second_token = await add_connection('alice', token={'access_token': 'access-two', 'expires_at': int(time.time()) + 3600})
            assert (await get(second_token, 'alice')).status_code == 200
            assert wire[-1][2] == 'acct-secret-two' and wire[-1][3] is None
            claude_id = await add_connection('alice', provider='claude', token={'access_token': 'claude-access', 'expires_at': int(time.time()) + 3600})
            responses['claude'] = (200, {'five_hour': {'utilization': 0, 'resets_at': 1700000000},
                                         'seven_day': {'utilization': 10, 'reset_at': 1700000000000},
                                         'seven_day_sonnet': {'utilization': 20, 'resetAt': '2025-01-02T03:04:05Z'},
                                         'seven_day_opus': {'utilization': 30},
                                         'seven_day_oauth_apps': {'utilization': 40}})
            claude_result = (await get(claude_id, 'alice')).json()
            assert [w['id'] for w in claude_result['windows']] == ['five_hour', 'seven_day', 'seven_day_sonnet', 'seven_day_opus', 'seven_day_oauth_apps']
            assert claude_result['windows'][0]['resets_at'] == 1700000000
            assert claude_result['windows'][1]['resets_at'] == 1700000000
            assert claude_result['windows'][2]['resets_at'] == 1735787045
            assert claude_result['windows'][3]['resets_at'] is None
            assert wire[-1][1] == 'Bearer claude-access' and wire[-1][3] is None

            # Codex primary/secondary aliases and safe percentage/reset normalization.
            responses['codex'] = (200, {'rate_limits_by_limit_id': {'codex': {
                'primary': {'percent_used': 12.5, 'resetAt': '2025-01-02T03:04:05Z'},
                'secondary': {'used_percent': 100, 'reset_at': None},
            }}})
            windows = (await get(personal, 'alice')).json()['windows']
            assert windows == [
                {'id': 'primary_window', 'label': 'Session', 'used_percent': 12.5, 'resets_at': 1735787045},
                {'id': 'secondary_window', 'label': 'Weekly', 'used_percent': 100, 'resets_at': None},
            ]
            for invalid in (True, '0', float('inf'), -1, 10**400):
                responses['codex'] = (200, {'rate_limit': {'primary_window': {'used_percent': invalid}}})
                assert (await get(personal, 'alice')).json()['status'] == 'unreported'
            responses['codex'] = (200, {})
            assert (await get(personal, 'alice')).json()['status'] == 'unreported'

            # Provider failures stay sanitized and don't change credentials, priority, or catalog.
            async def snapshot(connection_id):
                async with get_async_db_context() as db:
                    row = await db.get(OAuthSession, connection_id)
                    return row.token, row.expires_at
            before = await snapshot(personal)
            for status, body in ((503, {'error': 'SECRET-provider-detail'}), (200, 'invalid-json'),
                                 (200, 'non-object'), (200, 'oversized'), (200, 'redirect')):
                responses['codex'] = (status, body)
                failed = await get(personal, 'alice')
                assert failed.status_code == 502, failed.text
                assert 'SECRET-provider-detail' not in failed.text
                assert 'temporarily unavailable' in failed.text or 'usage' in failed.text.lower()
                assert await snapshot(personal) == before
            assert redirects == 1 and redirect_target_hits == 0
            async with get_async_db_context() as db:
                row = await db.get(OAuthSession, personal)
                stored = OAuthSessions._decrypt_token(row.token)
                assert stored['connection']['priority'] == 1
                assert stored['connection']['models'] == [{'id': 'catalog-secret'}]
                assert not stored['connection'].get('reauth_required', False)
            assert (await get(personal, 'alice')).status_code == 502
            responses['codex'] = (200, {})
            assert (await get(personal, 'alice')).json()['status'] == 'unreported'

    await close_session()
    await async_engine.dispose()
    print('subscription usage authorization, normalization, loopback isolation, and safe failure smoke passed')


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    with tempfile.TemporaryDirectory(prefix='open-webui-subscription-usage-') as directory:
        os.environ.update({
            'PYTHON_DOTENV_DISABLED': '1', 'FROM_INIT_PY': 'false', 'ENABLE_DB_MIGRATIONS': 'false',
            'DATA_DIR': directory, 'STATIC_DIR': str(Path(directory) / 'static'),
            'DATABASE_URL': f'sqlite:///{directory}/test.db', 'DATABASE_SCHEMA': 'main',
            'DATABASE_TYPE': '', 'DATABASE_USER': '', 'DATABASE_PASSWORD': '', 'DATABASE_HOST': '',
            'DATABASE_PORT': '', 'DATABASE_NAME': '', 'DATABASE_ENABLE_IAM_TOKEN_AUTH': 'false',
            'WEBUI_AUTH': 'true', 'WEBUI_SECRET_KEY': os.urandom(32).hex(),
            'OAUTH_SESSION_TOKEN_ENCRYPTION_KEY': os.urandom(32).hex(), 'WEBUI_JWT_SECRET_KEY': '',
            'LICENSE_BLOB_PATH': '', 'ENV': 'test', 'OFFLINE_MODE': 'true', 'USE_SLIM_DOCKER': 'true',
            'ENABLE_FUNCTIONS': 'false', 'ENABLE_TOOLS': 'false', 'REDIS_URL': '',
            'WEBSOCKET_REDIS_URL': '', 'WEBSOCKET_MANAGER': '',
        })
        asyncio.run(_check())
