"""Check image routing, personal OAuth selection, and uploads without an external account."""

import asyncio
import base64
import io
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


def test_subscription_images():
    subprocess.run([sys.executable, str(Path(__file__).resolve())], check=True)


async def _check():
    import httpx
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from fastapi import FastAPI, HTTPException, Request
    from open_webui.internal.db import async_engine, get_async_db_context
    from open_webui.models.config import Config
    from open_webui.models.files import File, Files
    from open_webui.models.groups import Group, GroupMember
    from open_webui.models.oauth_sessions import OAuthSession, OAuthSessions
    from open_webui.routers import files, images
    from open_webui.utils import subscriptions
    from open_webui.utils.session_pool import close_session
    from open_webui.utils.subscriptions import codex
    from PIL import Image

    async with async_engine.begin() as db:
        await db.run_sync(lambda connection: Config.metadata.create_all(
            connection, tables=[table.__table__ for table in (Config, File, Group, GroupMember, OAuthSession)]
        ))

    app = FastAPI()
    app.include_router(images.router, prefix='/images')
    app.include_router(files.router, prefix='/files')
    users = {
        name: SimpleNamespace(id=name, role=role, email=f'{name}@example.test', name=name)
        for name, role in [('alice', 'user'), ('admin', 'admin')]
    }

    async def current_user(request: Request):
        return users[request.headers.get('X-User', 'alice')]

    app.dependency_overrides.update({images.get_verified_user: current_user, images.get_admin_user: current_user})
    await Config.upsert({
        'image_generation.enable': False,
        'image_generation.engine': 'openai',
        'image_generation.model': 'previous-model',
        'image_generation.size': 'auto',
        'image_generation.steps': 19,
        'image_generation.openai.api_key': 'SECRET-api-key',
        'user.permissions': {'features': {'image_generation': False}},
    })

    claims = {'https://api.openai.com/auth': {'chatgpt_account_id': 'alice-account'}}
    jwt = 'test.' + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip('=') + '.test'
    now = int(time.time())

    async def add_connection(connection_id, *, owner='alice', provider='codex', scope='personal',
                             priority=0, created=1, enabled=True, reauth=False, expires_at=now + 3600):
        token = {
            'access_token': connection_id, 'refresh_token': f'refresh-{connection_id}',
            'id_token': jwt, 'expires_at': expires_at,
            'connection': {'scope': scope, 'priority': priority, 'enabled': enabled, 'reauth_required': reauth},
        }
        async with get_async_db_context() as db:
            db.add(OAuthSession(id=connection_id, provider=f'subscription:{provider}', user_id=owner,
                                token=OAuthSessions._encrypt_token(token), expires_at=expires_at,
                                created_at=created, updated_at=created))
            await db.commit()

    await add_connection('another-owner', owner='bob')
    await add_connection('shared-alice', scope='shared')
    await add_connection('shared-admin', owner='admin', scope='shared')
    await add_connection('disabled', enabled=False)
    await add_connection('needs-reauth', reauth=True)
    await add_connection('claude', provider='claude')
    await add_connection('older-low-priority', priority=20)
    await add_connection('later-same-priority', priority=10, created=3)
    await add_connection('z-tie', priority=10, created=2)
    await add_connection('a-winner', priority=10, created=2, expires_at=now - 1)

    png = io.BytesIO()
    Image.new('RGB', (2, 3)).save(png, format='PNG')
    png_bytes = png.getvalue()
    encoded = base64.b64encode(png_bytes).decode()
    calls = []
    mode = 'ok'

    async def generate(request):
        calls.append(await request.json())
        assert request.headers['Authorization'] == 'Bearer SECRET-rotated'
        assert request.headers['ChatGPT-Account-ID'] == 'alice-account'
        assert request.headers['Accept-Encoding'] == 'identity'
        async with get_async_db_context() as db:
            stored = await db.get(OAuthSession, 'a-winner')
            assert OAuthSessions._decrypt_token(stored.token)['refresh_token'] == 'SECRET-new-refresh'
        if mode == 'rate-limit':
            return web.Response(status=429, text='SECRET-provider-body')
        if mode == 'wrong-type':
            return web.Response(body=png_bytes, content_type='image/png')
        if mode == 'compressed':
            return web.Response(body=b'not gzip', content_type='application/json', headers={'Content-Encoding': 'gzip'})
        if mode == 'large':
            response = web.StreamResponse(headers={'Content-Type': 'application/json'})
            await response.prepare(request)
            await response.write(b' ' * (128 * 1024))
            return response
        if mode == 'redirect':
            return web.Response(status=307, headers={'Location': '/redirected'})
        return web.json_response({'data': [{'b64_json': encoded if mode == 'ok' else 'SECRET-invalid-base64!'}]})

    async def redirected(_):
        calls.append('redirect')
        return web.Response(status=500)

    provider = web.Application()
    provider.router.add_post('/images', generate)
    provider.router.add_post('/redirected', redirected)
    refresh = AsyncMock(return_value={
        'access_token': 'SECRET-rotated', 'refresh_token': 'SECRET-new-refresh', 'expires_in': 3600,
    })
    form = {'prompt': 'A simple circle', 'model': 'ignored-model', 'size': 'ignoredxsize'}
    admin = {'X-User': 'admin'}

    assert codex._IMAGE_GENERATIONS_URL == 'https://chatgpt.com/backend-api/codex/images/generations'
    try:
        async with TestServer(provider) as server, httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url='http://testserver'
        ) as client:
            with (
                patch.object(codex, '_IMAGE_GENERATIONS_URL', str(server.make_url('/images'))),
                patch.object(codex, '_MAX_IMAGE_B64_BYTES', 1024),
                patch.object(codex, 'refresh_token', refresh),
                patch.object(images, 'publish_event', AsyncMock()),
            ):
                config = (await client.get('/images/config', headers=admin)).json()
                config.update(IMAGE_GENERATION_ENGINE='openai_subscription', IMAGE_GENERATION_MODEL='ignored',
                              IMAGE_STEPS=-1, IMAGES_OPENAI_API_KEY='ignored', IMAGE_EDIT_MODEL='holiday-edit')
                save = await client.post('/images/config/update', json=config, headers=admin)
                assert save.status_code == 200, save.text
                assert save.json()['IMAGE_GENERATION_MODEL'] == 'previous-model'
                assert save.json()['IMAGE_SIZE'] == 'auto'
                assert save.json()['IMAGE_STEPS'] == 19
                assert save.json()['IMAGES_OPENAI_API_KEY'] == 'SECRET-api-key'
                assert save.json()['IMAGE_EDIT_MODEL'] == 'holiday-edit'
                assert (await client.get('/images/models')).json() == [{'id': 'gpt-image-2', 'name': 'GPT-IMAGE 2'}]
                assert (await client.post('/images/generations', json=form, headers=admin)).status_code == 403

                await Config.upsert({'image_generation.enable': True})
                assert (await client.post('/images/generations', json=form)).status_code == 403
                try:
                    await images.image_generations(
                        Request({'type': 'http', 'app': app}), images.CreateImageForm(**form), user=users['alice']
                    )
                except HTTPException as exc:
                    assert exc.status_code == 403
                else:
                    raise AssertionError('Internal image generation bypassed authorization')
                missing = await client.post('/images/generations', json=form, headers=admin)
                assert missing.status_code == 409 and 'personal Codex subscription' in missing.json()['detail']
                assert not calls and refresh.await_count == 0

                await Config.upsert({'user.permissions': {'features': {'image_generation': True}}})
                success = await client.post('/images/generations', json=form)
                assert success.status_code == 200, success.text
                [file] = success.json()
                stored_file = await Files.get_file_by_id(file['id'])
                assert stored_file.user_id == 'alice'
                assert Path(stored_file.path).read_bytes() == png_bytes
                assert file['content_type'] == 'image/png'
                assert calls == [{'model': 'gpt-image-2', 'prompt': form['prompt'], 'n': 1,
                                  'quality': 'low', 'size': '1024x1024'}]
                refresh.assert_awaited_once_with('refresh-a-winner')

                for mode in ('rate-limit', 'wrong-type', 'compressed', 'large', 'redirect', 'bad-base64'):
                    response = await client.post('/images/generations', json=form)
                    assert response.status_code == (429 if mode == 'rate-limit' else 502), response.text
                    assert 'SECRET' not in response.text
                assert 'redirect' not in calls and len(calls) == 7

                connection_token = subscriptions._connection_token

                async def change_scope_after_selection(connection_id, user, **kwargs):
                    assert connection_id == 'a-winner'
                    async with get_async_db_context() as db:
                        row = await db.get(OAuthSession, connection_id)
                        token = OAuthSessions._decrypt_token(row.token)
                        token['connection']['scope'] = 'shared'
                        row.token = OAuthSessions._encrypt_token(token)
                        await db.commit()
                    return await connection_token(connection_id, user, **kwargs)

                with patch.object(subscriptions, '_connection_token', change_scope_after_selection):
                    raced = await client.post('/images/generations', json=form)
                assert raced.status_code == 409 and len(calls) == 7
    finally:
        await close_session()
        await async_engine.dispose()


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    with tempfile.TemporaryDirectory(prefix='open-webui-subscription-images-') as directory:
        os.environ.update(
            PYTHON_DOTENV_DISABLED='1', FROM_INIT_PY='false', ENABLE_DB_MIGRATIONS='false',
            DATA_DIR=directory, STATIC_DIR=str(Path(directory) / 'static'), STORAGE_PROVIDER='local',
            DATABASE_URL=f'sqlite:///{directory}/test.db', DATABASE_SCHEMA='main',
            DATABASE_TYPE='', DATABASE_USER='', DATABASE_PASSWORD='', DATABASE_HOST='', DATABASE_PORT='',
            DATABASE_NAME='', DATABASE_ENABLE_IAM_TOKEN_AUTH='false', WEBUI_AUTH='true',
            WEBUI_SECRET_KEY=os.urandom(32).hex(), OAUTH_SESSION_TOKEN_ENCRYPTION_KEY=os.urandom(32).hex(),
            WEBUI_JWT_SECRET_KEY='', LICENSE_BLOB_PATH='', ENV='test', OFFLINE_MODE='true',
            USE_SLIM_DOCKER='true', ENABLE_PERSISTENT_CONFIG='true', REDIS_URL='',
            NO_PROXY='localhost,127.0.0.1,::1', no_proxy='localhost,127.0.0.1,::1',
        )
        asyncio.run(_check())
