"""Codex voice bridge regression. Run: python backend/tests/test_subscription_realtime.py"""

import asyncio
import base64
import json
import os
import subprocess
import sys
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SDP = 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendrecv\r\n'
DELEGATION = {
    'type': 'delegation.created',
    'item': {
        'type': 'delegation',
        'target': 'client',
        'id': 'delegation-1',
        'content': [{'type': 'input_text', 'text': 'Look up the order.'}],
    },
}


def test_subscription_realtime():
    subprocess.run([sys.executable, str(Path(__file__).resolve())], check=True)


async def _check():
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from fastapi import HTTPException, WebSocketDisconnect
    from open_webui.routers.audio import realtime, realtime_subscription
    from open_webui.utils.subscriptions import codex

    user = SimpleNamespace(id='local-owner', role='user')
    config = {'audio.realtime.enabled': True, 'audio.realtime.provider': 'openai_subscription'}
    access = {'call': True, 'preset': True, 'personal': True}
    base = {
        'id': 'subscription:shared:local-model',
        'subscription': {'connection_id': 'shared', 'provider': 'codex', 'model': 'local-model'},
    }
    personal = {
        'id': 'subscription:personal:local-model',
        'subscription': {'connection_id': 'personal', 'provider': 'codex', 'model': 'local-model'},
    }
    model_checks, token_checks = [], []
    calls, redirects = [], []
    sidebands = asyncio.Queue()
    redirect = None
    claims = {'https://api.openai.com/auth': {'chatgpt_account_id': 'local-account'}}
    bearer = 'e30.' + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip('=') + '.local'

    async def verified(token, redis):
        return user if token == 'local-webui-session' else None

    async def permission(user_id, name, permissions):
        assert (user_id, name) == (user.id, 'chat.call')
        return access['call']

    async def chat_by_id(chat_id):
        return SimpleNamespace(user_id=user.id if chat_id == 'own-chat' else 'another-user')

    async def chat_by_owner(chat_id, user_id):
        return (await chat_by_id(chat_id)).user_id == user_id

    async def get_config(key):
        return config.get(key)

    async def get_config_many(*keys):
        return {key: config[key] for key in keys if key in config}

    async def assert_access(current, model_id, *, prefer_personal=False):
        assert current is user
        model_checks.append((model_id, prefer_personal))
        if model_id == 'preset':
            if not access['preset']:
                raise HTTPException(404)
            return personal if prefer_personal and access['personal'] else base
        if model_id == personal['id']:
            if not access['personal']:
                raise HTTPException(404)
            return personal
        if model_id == 'claude':
            return {'subscription': {'provider': 'claude'}}

    async def connection_token(connection_id, current, *, raw_id):
        assert current is user
        token_checks.append((connection_id, raw_id))
        return 'codex', {'access_token': bearer, 'refresh_token': 'never-send-refresh-token'}

    def check_headers(request):
        assert request.headers['Authorization'] == f'Bearer {bearer}'
        assert request.headers['ChatGPT-Account-ID'] == 'local-account'
        assert request.headers['OpenAI-Alpha'] == 'quicksilver=v2'
        assert 'originator' not in request.headers and 'x-oai-attestation' not in request.headers
        assert 'api-key' not in request.headers and 'Cookie' not in request.headers
        assert 'Codex Desktop' not in request.headers.get('User-Agent', '')

    async def signal(request):
        check_headers(request)
        calls.append(await request.json())
        if redirect == 'signal':
            raise web.HTTPTemporaryRedirect(location='/redirected')
        # Use the call ID, not the path or origin in Location.
        response = web.Response(text=SDP, status=201, headers={'Location': '/redirected/rtc_local?opaque=1'})
        response.set_cookie('provider_cookie', 'not-forwarded')
        return response

    async def sideband(request):
        assert request.match_info['call_id'] == 'rtc_local'
        check_headers(request)
        if redirect == 'sideband':
            raise web.HTTPTemporaryRedirect(location='/redirected')
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.send_json({'type': 'session.started', 'session': {'id': 'local'}})
        commands, closed = asyncio.Queue(), asyncio.Event()
        sidebands.put_nowait((socket, commands, closed))
        async for message in socket:
            commands.put_nowait(message.json())
        closed.set()
        return socket

    async def redirected(request):
        redirects.append(request.path)
        return web.Response(status=401, text='provider detail must never reach the browser')

    provider = web.Application()
    provider.router.add_post('/signal', signal)
    provider.router.add_get('/live/{call_id}', sideband)
    provider.router.add_route('*', '/redirected', redirected)
    provider.router.add_route('*', '/redirected/{call_id}', redirected)

    class Browser:
        app = SimpleNamespace(state=SimpleNamespace())

        def __init__(self):
            self.commands, self.events = asyncio.Queue(), asyncio.Queue()
            self.closed = False

        async def accept(self):
            pass

        async def receive_text(self):
            raw = await self.commands.get()
            if raw is None:
                raise WebSocketDisconnect(1000)
            return raw

        async def send_json(self, event):
            self.events.put_nowait(event)

        async def close(self):
            self.closed = True

        def send(self, event):
            self.commands.put_nowait(json.dumps(event, ensure_ascii=False))

        async def ready(self):
            assert await self.events.get() == {'type': 'subscription.answer', 'sdp': SDP}
            assert await self.events.get() == {
                'type': 'bridge.ready', 'model': 'gpt-live-1-codex', 'voice': 'sol', 'sample_rate': 24000
            }
            return await sidebands.get()

        async def error(self, detail):
            event = await self.events.get()
            assert event['type'] == 'bridge.error' and detail in event['message'], event

    @asynccontextmanager
    async def call(*, token='local-webui-session', model='preset', chat='own-chat', offer=SDP):
        browser = Browser()
        browser.send({'type': 'auth', 'token': token, 'model_id': model, 'chat_id': chat})
        browser.send({'type': 'subscription.offer', 'sdp': offer})
        task = asyncio.create_task(realtime.realtime_call(browser))
        try:
            yield browser
        finally:
            browser.commands.put_nowait(None)
            await task
            assert browser.closed

    async def assert_context(commands, expected_text, fields):
        assembled = ''
        while len(assembled) < len(expected_text):
            command = await commands.get()
            content = command.pop('content')
            assert command == fields
            assert len(content) == 1 and content[0]['type'] == 'input_text'
            text = content[0]['text']
            assert 0 < len(text.encode()) <= 500
            assembled += text
        assert assembled == expected_text

    async with TestServer(provider) as server, asyncio.timeout(20):
        with (
            patch.multiple(
                realtime,
                Config=SimpleNamespace(get=get_config, get_many=get_config_many),
                Chats=SimpleNamespace(get_chat_by_id=chat_by_id, get_chat_by_id_and_user_id=chat_by_owner),
                get_verified_user_by_token=verified,
                has_permission=permission,
                assert_subscription_model_access=assert_access,
            ),
            patch.object(codex, '_connection_token', connection_token),
            patch.multiple(
                realtime_subscription,
                _SIGNALING_URL=str(server.make_url('/signal')),
                _SIDEBAND_URL=str(server.make_url('/live/').with_scheme('ws')),
            ),
        ):
            for kwargs, detail in (
                ({'token': 'invalid'}, 'Authentication expired or invalid'),
                ({'chat': 'someone-elses-chat'}, 'Chat not found'),
                ({'model': 'api-key-model'}, 'Select a Codex'),
                ({'model': 'claude'}, 'Select a Codex'),
                ({'offer': SDP + 'm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n'}, 'audio-only'),
            ):
                async with call(**kwargs) as browser:
                    await browser.error(detail)
            for permission_name, detail in (('call', 'Call permission denied'), ('preset', 'Chat model access denied')):
                access[permission_name] = False
                async with call() as browser:
                    await browser.error(detail)
                access[permission_name] = True
            assert not calls and not token_checks

            for redirect, detail in (('signal', 'signaling failed'), ('sideband', 'redirected')):
                async with call() as browser:
                    await browser.error(detail)
                assert not redirects and sidebands.empty()
            redirect = None

            model_checks.clear()
            async with call() as browser:
                socket, commands, closed = await browser.ready()
                assert model_checks == [('preset', True), ('preset', False), (personal['id'], False)]
                assert token_checks[-1] == ('personal', 'local-model')
                payload = calls[-1]
                assert payload['sdp'] == SDP
                assert payload['session']['model'] == 'gpt-live-1-codex'
                assert payload['session']['audio'] == {'output': {'voice': 'sol'}}
                assert payload['session']['delegation'] == {'type': 'client'}
                instructions = payload['session']['instructions']
                assert 'built-in client delegation' in instructions and 'generate_chat_completion' not in instructions
                assert 'spoken agreement never authorizes a tool' in instructions
                browser.send({'type': 'bridge.ping'})
                assert await browser.events.get() == {'type': 'bridge.pong'}
                text = 'a' * 484 + '🙂注文' * 126
                browser.send({'type': 'subscription.context', 'text': text})
                await assert_context(commands, 'Chat history:\n' + text, {
                    'type': 'session.context.append', 'channel': 'commentary'
                })
                await socket.send_json({'type': 'output_audio.delta', 'audio': 'media-is-webrtc-only'})
                await socket.send_json(DELEGATION)
                assert await browser.events.get() == DELEGATION
                result = {'type': 'subscription.result', 'call_id': 'delegation-1', 'text': text}
                browser.send(result)
                await assert_context(commands, 'Chat result:\n' + text, {
                    'type': 'delegation.context.append', 'delegation_item_id': 'delegation-1'
                })
                await socket.send_json(DELEGATION)  # Repeated provider IDs do not reopen authorization.
                turn = {'type': 'turn.done', 'turn': {'role': 'assistant', 'transcript': 'Done.'}}
                await socket.send_json(turn)
                assert await browser.events.get() == turn
                browser.send(result)
                await browser.error('Unknown or resolved voice delegation')
            assert await commands.get() == {'type': 'session.close'}
            await closed.wait()

            for event, detail in (
                ({'type': 'subscription.result', 'call_id': 'never-issued', 'text': 'invented'}, 'Unknown or resolved'),
                ({'type': 'subscription.context', 'text': 'a' * 100_001}, 'Invalid call context'),
                ({'type': 'subscription.context', 'text': '🙂' * 132_000}, 'Call event is too large'),
                ({'type': 'session.update', 'session': {}}, 'Unsupported call command'),
            ):
                async with call() as browser:
                    _, commands, closed = await browser.ready()
                    browser.send(event)
                    await browser.error(detail)
                assert await commands.get() == {'type': 'session.close'}
                await closed.wait()

            config['audio.realtime.prompt_template'] = 'Use short sentences.'
            async with call(chat=None) as browser:
                _, commands, closed = await browser.ready()
                assert calls[-1]['session']['instructions'].startswith('Use short sentences.\n')
            assert await commands.get() == {'type': 'session.close'}
            await closed.wait()
            del config['audio.realtime.prompt_template']

            async with call() as browser:
                socket, commands, closed = await browser.ready()
                await socket.send_json({'type': 'error', 'message': 'private provider details'})
                assert await browser.events.get() == {
                    'type': 'bridge.error', 'message': 'Voice subscription provider rejected the request'
                }
            assert await commands.get() == {'type': 'session.close'}
            await closed.wait()

            with patch.object(realtime, '_AUTH_RECHECK_SECONDS', 0.02):
                for revoked in ('preset', 'personal'):
                    async with call() as browser:
                        _, commands, closed = await browser.ready()
                        access[revoked] = False
                        await browser.error('Chat model access revoked')
                    assert await commands.get() == {'type': 'session.close'}
                    await closed.wait()
                    access[revoked] = True


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    with tempfile.TemporaryDirectory(prefix='open-webui-subscription-realtime-') as directory:
        os.environ.update({
            'PYTHON_DOTENV_DISABLED': '1', 'FROM_INIT_PY': 'false', 'ENABLE_DB_MIGRATIONS': 'false',
            'DATA_DIR': directory, 'STATIC_DIR': str(Path(directory) / 'static'),
            'DATABASE_URL': f'sqlite:///{directory}/test.db', 'DATABASE_SCHEMA': 'main',
            'WEBUI_AUTH': 'true', 'WEBUI_SECRET_KEY': os.urandom(32).hex(), 'WEBUI_JWT_SECRET_KEY': '',
            'OAUTH_SESSION_TOKEN_ENCRYPTION_KEY': os.urandom(32).hex(), 'LICENSE_BLOB_PATH': '',
            'ENV': 'test', 'OFFLINE_MODE': 'true', 'USE_SLIM_DOCKER': 'true',
            'REDIS_URL': '', 'WEBSOCKET_REDIS_URL': '', 'WEBSOCKET_MANAGER': '',
        })
        asyncio.run(_check())
