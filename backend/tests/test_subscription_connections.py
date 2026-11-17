"""Disposable-DB lifecycle/security check and real loopback provider HTTP smoke.

Run from the repository root: python backend/tests/test_subscription_connections.py
No credentials, .env files, app lifespan, real consent, or external provider calls.
For an operator's separate real-account smoke: start a LOCAL WebUI with a disposable
DATA_DIR and encryption key, connect using Settings > Connections, paste the FULL
callback URL, select its model (or a preset), then exercise a saved chat with an
approved tool and knowledge. Check streaming, title/follow-up, disconnect/reconnect,
and a second user's denied personal access. Never paste provider tokens. That
opt-in smoke is not performed by this regression and does not establish provider
approval or subscription entitlement.
"""

import asyncio
import base64
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit


def test_subscription_connections():
    subprocess.run([sys.executable, str(Path(__file__).resolve())], check=True)


async def _check():
    import httpx
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from fastapi import Depends, FastAPI, HTTPException, Request
    from open_webui import events
    from open_webui.internal.db import async_engine, get_async_db_context
    from open_webui.models.access_grants import AccessGrant, AccessGrants
    from open_webui.models.chat_messages import ChatMessage, ChatMessages
    from open_webui.models.chats import Chat
    from open_webui.models.config import Config
    from open_webui.models.groups import Group, GroupMember
    from open_webui.models.models import Model
    from open_webui.models.oauth_sessions import OAuthSession, OAuthSessions
    from open_webui.models.users import User, UserModel
    from open_webui.routers import subscriptions as router
    from open_webui.routers.auths import delete_oauth_session_by_provider
    from open_webui.routers.chats import CompactChatForm, compact_chat_by_id
    from open_webui.routers.users import get_user_oauth_sessions_by_id
    from open_webui.utils import subscriptions as core
    from open_webui.utils.audit import AuditLevel, AuditLoggingMiddleware
    from open_webui.utils.auth import get_verified_user
    from open_webui.utils.chat import generate_chat_completion
    from open_webui.utils.models import check_model_access
    from open_webui.utils.oauth import OAuthManager, get_system_oauth_token
    from open_webui.utils.session_pool import close_session
    from open_webui.utils.subscriptions import claude, codex
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession

    tables = [User.__table__, Group.__table__, GroupMember.__table__, Model.__table__,
              AccessGrant.__table__, OAuthSession.__table__, Config.__table__, Chat.__table__, ChatMessage.__table__]
    async with async_engine.begin() as db:
        await db.run_sync(lambda connection: User.metadata.create_all(connection, tables=tables))

    users = {name: UserModel(id=name, email=f'{name}@example.test', name=name, role=role,
                            last_active_at=0, updated_at=0, created_at=0)
             for name, role in [('alice', 'user'), ('bob', 'user'), ('admin', 'admin'), ('admin2', 'admin')]}
    async with get_async_db_context() as db:
        db.add_all([User(id=user.id, email=user.email, name=user.name, role=user.role,
                         last_active_at=0, updated_at=0, created_at=0) for user in users.values()])
        db.add(Group(id='team', user_id='admin', name='Team', description='', created_at=0, updated_at=0))
        db.add(GroupMember(id='member-bob', group_id='team', user_id='bob'))
        await db.commit()

    counts = Counter()
    challenges = {}
    catalog_failures = set()
    refresh_failures = set()
    refresh_without_rotation = set()
    spent = set()
    wire = []
    fail_inference_once = set()

    async def token_endpoint(request):
        provider = request.match_info['provider']
        assert not request.headers.get('Cookie')
        body = await request.json() if request.content_type == 'application/json' else dict(await request.post())
        grant = body['grant_type']
        counts[(provider, grant)] += 1
        if grant == 'authorization_code':
            code = body['code']
            expected = challenges[code]
            challenge = base64.urlsafe_b64encode(hashlib.sha256(body['code_verifier'].encode()).digest()).rstrip(b'=').decode()
            assert challenge == expected['code_challenge'][0]
            assert body['redirect_uri'] == (codex if provider == 'codex' else claude).OAUTH['redirect_uri']
            if provider == 'claude':
                assert body['state'] == expected['state'][0]
            response = web.json_response({'access_token': f'SECRET-access-{code}', 'refresh_token': f'SECRET-refresh-{code}', 'expires_in': 3600})
            response.set_cookie('provider-session', 'SECRET-ambient-cookie')
            return response
        refresh = body['refresh_token']
        counts[('refresh', refresh)] += 1
        await asyncio.sleep(0.03)
        if refresh in spent or refresh in refresh_failures:
            return web.json_response({'error': 'invalid_grant', 'error_description': 'SECRET-must-not-escape'}, status=400)
        value = {'access_token': f'SECRET-rotated-{refresh}', 'expires_in': 3600}
        if refresh not in refresh_without_rotation:
            spent.add(refresh)
            value['refresh_token'] = refresh + '-rotated'
        return web.json_response(value)

    async def models_endpoint(request):
        provider = request.match_info['provider']
        counts[(provider, 'models')] += 1
        assert request.headers['Authorization'].startswith('Bearer SECRET-')
        assert not request.headers.get('Cookie')
        if provider in catalog_failures:
            return web.json_response({'error': 'SECRET-provider-detail'}, status=503)
        if provider == 'codex':
            return web.json_response({'models': [{'slug': 'local-codex', 'display_name': 'Local Codex', 'input_modalities': ['text', 'image']}]})
        return web.json_response({'data': [{'id': 'local-claude', 'display_name': 'Local Claude'}], 'has_more': False})

    def sse(events):
        return web.Response(text=''.join('data: ' + json.dumps(event) + '\n\n' for event in events), content_type='text/event-stream')

    async def inference_endpoint(request):
        provider = request.match_info['provider']
        counts[(provider, 'inference')] += 1
        assert request.headers['Authorization'].startswith('Bearer SECRET-')
        assert not request.headers.get('Cookie')
        payload = await request.json()
        wire.append((provider, payload))
        if provider in fail_inference_once:
            fail_inference_once.remove(provider)
            return web.Response(status=503, headers={'Retry-After': '0'})
        if provider == 'codex':
            assert payload['store'] is False and payload['stream'] is True
            has_result = any(item.get('type') == 'function_call_output' for item in payload['input'])
            tool = bool(payload.get('tools')) and not has_result
            item = ({'type': 'function_call', 'id': 'fc-local', 'call_id': 'call-local', 'name': 'lookup', 'arguments': '{"value":7}'}
                    if tool else {'type': 'message', 'id': 'msg-local', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': 'local answer'}]})
            result = {'id': 'resp-local', 'model': payload['model'], 'created_at': 0, 'status': 'completed', 'output': [item],
                      'usage': {'input_tokens': 10, 'output_tokens': 2, 'total_tokens': 12}}
            events = [{'type': 'response.created', 'response': {'id': 'resp-local', 'model': payload['model'], 'created_at': 0}}]
            if tool:
                events.extend([
                    {'type': 'response.output_item.added', 'output_index': 0, 'item': {**item, 'arguments': ''}},
                    {'type': 'response.function_call_arguments.delta', 'output_index': 0, 'item_id': 'fc-local', 'delta': item['arguments']},
                ])
            else:
                events.extend([
                    {'type': 'response.output_item.added', 'output_index': 0, 'item': {**item, 'content': []}},
                    {'type': 'response.output_text.delta', 'output_index': 0, 'content_index': 0, 'item_id': 'msg-local', 'delta': 'local answer'},
                ])
            events.extend([{'type': 'response.output_item.done', 'output_index': 0, 'item': item}, {'type': 'response.completed', 'response': result}])
            return sse(events)
        has_result = any(block.get('type') == 'tool_result' for message in payload['messages'] for block in message.get('content', []) if isinstance(block, dict))
        tool = bool(payload.get('tools')) and not has_result
        content = ([{'type': 'tool_use', 'id': 'call-local', 'name': 'lookup', 'input': {'value': 7}}]
                   if tool else [{'type': 'text', 'text': 'local answer'}])
        message = {'id': 'msg-local', 'type': 'message', 'role': 'assistant', 'model': payload['model'], 'content': content,
                   'stop_reason': 'tool_use' if tool else 'end_turn', 'usage': {'input_tokens': 10, 'output_tokens': 2}}
        if not payload['stream']:
            return web.json_response(message)
        return sse([
            {'type': 'message_start', 'message': {**message, 'content': [], 'stop_reason': None, 'usage': {'input_tokens': 10, 'output_tokens': 0}}},
            {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}},
            {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'local answer'}},
            {'type': 'content_block_stop', 'index': 0},
            {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn'}, 'usage': {'output_tokens': 2}},
            {'type': 'message_stop'},
        ])

    provider_app = web.Application()
    provider_app.router.add_post('/{provider}/token', token_endpoint)
    provider_app.router.add_get('/{provider}/models', models_endpoint)
    provider_app.router.add_post('/{provider}/chat', inference_endpoint)

    app = FastAPI()
    app.state.MODELS = {}
    app.state.OPENAI_MODELS = {}
    app.state.redis = None

    async def authenticated(request: Request):
        user = users.get(request.headers.get('x-test-user'))
        if user is None:
            raise HTTPException(401, 'Unauthenticated')
        request.state.user = user
        return user

    app.dependency_overrides[get_verified_user] = authenticated
    app.include_router(router.router, prefix='/api/v1/subscriptions')

    captured_audit = []
    audited_app = AuditLoggingMiddleware(app, audit_level=AuditLevel.REQUEST_RESPONSE)
    audited_app._should_skip_auditing = lambda request: False

    async def capture_audit(request, context):
        captured_audit.append((bytes(context.request_body), bytes(context.response_body), context.metadata))

    audited_app._log_audit_entry = capture_audit
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=audited_app), base_url='http://webui.test') as client:
        response = await client.post('/api/v1/subscriptions/complete', headers={'x-test-user': 'alice'},
                                     json={'state': 'missing', 'callback': 'SECRET-code#missing'})
        assert response.status_code == 400
    assert captured_audit[0][0:2] == (b'', b'')
    assert captured_audit[0][2]['response_status_code'] == 400

    class LiveToolHandle:
        def __deepcopy__(self, memo):
            raise AssertionError('Live MCP/tool metadata must not be copied or sent upstream')

    @app.post('/test/completions')
    async def completion(request: Request, body: dict, user=Depends(authenticated)):
        # This is the real central handler used for model-tool-model and auxiliary calls.
        body['metadata'] = {'live_tool_handle': LiveToolHandle()}
        return await generate_chat_completion(request, body, user)

    def request_for(name, cookie=None):
        headers = [(b'cookie', f'oauth_session_id={cookie}'.encode())] if cookie else []
        return Request({'type': 'http', 'asgi': {'version': '3.0'}, 'method': 'POST', 'path': '/', 'query_string': b'',
                        'headers': headers, 'scheme': 'http', 'server': ('test', 80), 'client': ('127.0.0.1', 1), 'app': app})

    async def denied(awaitable, status):
        try:
            await awaitable
        except HTTPException as error:
            assert error.status_code == status, (error.status_code, error.detail)
        else:
            raise AssertionError('Unauthorized operation unexpectedly succeeded')

    async def expire(connection_id, *, refresh=None):
        async with get_async_db_context() as db:
            row = await db.get(OAuthSession, connection_id)
            token = OAuthSessions._decrypt_token(row.token)
            token['expires_at'] = int(time.time()) - 5
            if refresh is not None:
                token['refresh_token'] = refresh
            core._save(row, token)
            await db.commit()
            return token['refresh_token']

    sequence = 0
    async with TestServer(provider_app) as server:
        def local_url(path):
            # A normal CookieJar accepts localhost cookies, unlike numeric IP origins.
            return str(server.make_url(path)).replace('127.0.0.1', 'localhost')

        for provider, module in [('codex', codex), ('claude', claude)]:
            module.OAUTH['authorize_url'] = local_url(f'/{provider}/authorize')
            module.OAUTH['token_url'] = local_url(f'/{provider}/token')
            module.MODELS_URL = local_url(f'/{provider}/models')
        codex.RESPONSES_URL = local_url('/codex/chat')
        claude.MESSAGES_URL = local_url('/claude/chat')

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://webui.test') as client:
            async def api(method, path, name='alice', data=None):
                response = await client.request(method, '/api/v1/subscriptions' + path, headers={'x-test-user': name}, json=data)
                # OAuth codes/verifiers/tokens never appear in success/error lifecycle responses.
                assert 'SECRET-' not in response.text and 'code_verifier' not in response.text
                return response

            async def begin(provider='codex', name='alice', scope='personal', grants=None):
                nonlocal sequence
                sequence += 1
                response = await api('POST', '/authorize', name, {'provider': provider, 'name': f'{provider} {sequence}', 'scope': scope, 'access_grants': grants or []})
                assert response.status_code == 200, response.text
                pending = response.json()
                code = f'code-{sequence}'
                challenges[code] = parse_qs(urlsplit(pending['authorization_url']).query)
                assert challenges[code]['state'] == [pending['state']]
                assert challenges[code]['code_challenge_method'] == ['S256']
                return pending, code

            async def finish(pending, code, name='alice'):
                callback = pending['redirect_uri'] + '?' + urlencode({'code': code, 'state': pending['state']})
                return await api('POST', '/complete', name, {'state': pending['state'], 'callback': callback})

            assert (await client.get('/api/v1/subscriptions/')).status_code == 401
            assert (await api('POST', '/authorize', 'bob', {'provider': 'codex', 'name': 'No', 'scope': 'shared'})).status_code == 403
            for grant in [dict(principal_type='user', principal_id='*', permission='read'),
                          dict(principal_type='anyone', principal_id='*', permission='read'),
                          dict(principal_type='user', principal_id='bob', permission='write'),
                          dict(principal_type='group', principal_id='missing', permission='read')]:
                response = await api('POST', '/authorize', 'admin', {'provider': 'codex', 'name': 'No', 'scope': 'shared', 'access_grants': [grant]})
                assert response.status_code in (400, 422)

            pending, code = await begin()
            assert (await finish(pending, code, 'bob')).status_code == 400
            assert (await api('POST', '/complete', data={'state': pending['state'], 'callback': code + '#wrong-state'})).status_code == 400
            assert (await api('POST', '/complete', data={'state': pending['state'], 'callback': code + '#wrong-🔒'})).status_code == 400
            before = counts[('codex', 'authorization_code')]
            results = await asyncio.gather(finish(pending, code), finish(pending, code))
            assert sorted(response.status_code for response in results) == [200, 400]
            assert counts[('codex', 'authorization_code')] == before + 1
            personal = next(response.json() for response in results if response.status_code == 200)
            assert personal['access_grants'] == [] and personal['can_manage'] is True
            assert (await finish(pending, code)).status_code == 400
            personal_id = personal['id']
            raw = f'subscription:{personal_id}:local-codex'
            assert (await api('GET', '/', 'bob')).json() == []
            assert (await api('GET', '/', 'admin')).json() == []
            await denied(core.resolve_subscription_model(users['bob'], raw), 404)
            await denied(core.resolve_subscription_model(users['admin'], raw), 404)
            assert (await api('PATCH', '/' + personal_id, 'bob', {'name': 'stolen'})).status_code == 404
            assert (await api('DELETE', '/' + personal_id, 'admin')).status_code == 404
            assert (await api('PATCH', '/' + personal_id, data={'scope': 'shared'})).status_code == 422
            assert (await api('PATCH', '/' + personal_id, data={'access_grants': [{'principal_type': 'user', 'principal_id': 'bob', 'permission': 'read'}]})).status_code == 400

            # Existing OAuth consumers do not expose or select either namespace.
            assert await OAuthSessions.get_session_by_id(personal_id) is None
            assert await OAuthSessions.get_session_by_id_and_user_id(personal_id, 'alice') is None
            assert await OAuthSessions.get_sessions_by_user_id('alice') == []
            assert await OAuthSessions.update_session_by_id(personal_id, {'expires_at': 1}) is None
            await denied(get_user_oauth_sessions_by_id('alice', users['admin'], None), 400)
            await denied(delete_oauth_session_by_provider(request_for('alice'), 'subscription:codex', users['alice'], None), 404)
            manager = OAuthManager(app)
            app.state.oauth_manager = manager
            assert await manager.get_oauth_token('alice', personal_id) is None
            assert await get_system_oauth_token(request_for('alice', personal_id), users['alice']) is None
            assert await get_system_oauth_token(request_for('alice'), users['alice']) is None

            expired, expired_code = await begin()
            async with get_async_db_context() as db:
                row = await db.get(OAuthSession, expired['state'])
                row.expires_at = int(time.time()) - 1
                await db.commit()
            assert (await finish(expired, expired_code)).status_code == 400

            # A model-list outage retains the valid connected credential and can be retried explicitly.
            failed, failed_code = await begin('claude')
            catalog_failures.add('claude')
            error = await finish(failed, failed_code)
            assert error.status_code == 502 and error.json()['detail']['connection_id']
            claude_id = error.json()['detail']['connection_id']
            catalog_failures.clear()
            fixed = await api('POST', '/' + claude_id + '/models')
            assert fixed.status_code == 200 and fixed.json()['models'][0]['id'] == 'local-claude'

            pending, code = await begin('claude', 'admin', 'shared', [{'principal_type': 'group', 'principal_id': 'team', 'permission': 'read'}])
            shared = (await finish(pending, code, 'admin')).json()
            shared_id = shared['id']
            shared_raw = f'subscription:{shared_id}:local-claude'
            assert [row['id'] for row in (await api('GET', '/', 'bob')).json()] == [shared_id]
            assert (await api('GET', '/', 'bob')).json()[0]['access_grants'] == []
            assert (await api('GET', '/', 'admin2')).json()[0]['can_manage'] is True
            await denied(core.resolve_subscription_model(users['admin2'], shared_raw), 404)
            await denied(delete_oauth_session_by_provider(request_for('admin'), 'subscription:claude', users['admin'], None), 404)
            assert (await core.resolve_subscription_model(users['bob'], shared_raw))['subscription']['connection_id'] == shared_id
            await AccessGrants.grant_access(core.RESOURCE_TYPE, shared_id, 'user', '*', 'read')
            await denied(core.resolve_subscription_model(users['alice'], shared_raw), 404)
            assert (await api('PATCH', '/' + shared_id, 'bob', {'enabled': False})).status_code == 404
            assert (await api('POST', '/' + shared_id + '/models', 'bob')).status_code == 404
            assert (await api('PATCH', '/' + shared_id, 'admin2', {'access_grants': []})).status_code == 200
            assert (await api('GET', '/', 'bob')).json() == []
            await denied(core.resolve_subscription_model(users['bob'], shared_raw), 404)
            assert (await api('PATCH', '/' + shared_id, 'admin', {'access_grants': [{'principal_type': 'user', 'principal_id': 'bob', 'permission': 'read'}]})).status_code == 200
            assert (await core.resolve_subscription_model(users['bob'], shared_raw))['subscription']['connection_id'] == shared_id

            # A saved preset keeps WebUI-only tools/knowledge/controls, without global-model leakage.
            async with get_async_db_context() as db:
                db.add(Model(id='preset-local', user_id='alice', base_model_id=raw, name='Knowledge preset', is_active=True,
                             params={'system': 'Saved system context', 'model_controls': {}},
                             meta={'toolIds': ['attached-tool'], 'knowledge': [{'id': 'attached-knowledge'}]}, created_at=0, updated_at=0))
                await AccessGrants.replace_access_grants(db, 'model', 'preset-local', [{'principal_type': 'user', 'principal_id': 'bob', 'permission': 'read'}])
                await db.commit()
            alice_models = await core.get_model_overlay(request_for('alice'), users['alice'])
            bob_models = await core.get_model_overlay(request_for('bob'), users['bob'])
            assert alice_models['preset-local']['info']['meta']['knowledge'][0]['id'] == 'attached-knowledge'
            assert alice_models['preset-local']['info']['meta']['toolIds'] == ['attached-tool']
            assert 'preset-local' not in bob_models and raw not in bob_models
            assert app.state.MODELS == {} and app.state.OPENAI_MODELS == {}
            await check_model_access(users['alice'], alice_models['preset-local'])
            await denied(check_model_access(users['bob'], alice_models['preset-local']), 404)

            # Manual compaction uses the same saved preset without registering its private base globally.
            await Config.upsert({'chat.context_compaction.enable': True, 'chat.context_compaction.model': ''})
            async with get_async_db_context() as db:
                for owner in ('alice', 'bob'):
                    chat_id = f'compact-{owner}'
                    user_id, answer_id = f'{chat_id}-user', f'{chat_id}-answer'
                    history = {
                        user_id: {'id': user_id, 'role': 'user', 'content': 'Earlier question', 'parentId': None, 'childrenIds': [answer_id]},
                        answer_id: {'id': answer_id, 'role': 'assistant', 'content': 'Recent answer', 'parentId': user_id, 'childrenIds': [], 'model': 'preset-local'},
                    }
                    db.add(Chat(id=chat_id, user_id=owner, title='Local compaction', created_at=0, updated_at=0,
                                current_message_id=answer_id, chat={'title': 'Local compaction', 'models': ['preset-local'], 'history': {'currentId': answer_id, 'messages': history}}))
                    for message_id, message in history.items():
                        await ChatMessages.upsert_message(message_id, chat_id, owner, message, db=db)
                await db.commit()
            ordinary_registry = {'ordinary': {'id': 'ordinary', 'owned_by': 'openai'}}
            app.state.MODELS = ordinary_registry
            sinks = events.EVENT_SINKS
            events.EVENT_SINKS = []  # This isolated smoke has no notification/webhook destinations.
            try:
                compacted = await compact_chat_by_id(request_for('alice'), 'compact-alice', CompactChatForm(model='preset-local'), users['alice'], None)
                assert compacted['compacted'] is True
                persisted_messages = await ChatMessages.get_messages_map_by_chat_id('compact-alice')
                assert persisted_messages['compact-alice-answer']['contextSummary'] == 'local answer'
                await denied(compact_chat_by_id(request_for('bob'), 'compact-bob', CompactChatForm(model='preset-local'), users['bob'], None), 404)
                assert app.state.MODELS == ordinary_registry and len(app.state.MODELS) == 1
            finally:
                app.state.MODELS = {}
                events.EVENT_SINKS = sinks

            # Expired access tokens survive cleanup and refresh exactly once with rotation across concurrent calls.
            refresh = await expire(personal_id)
            await begin('codex')  # Opportunistic pending cleanup must not remove durable expired rows.
            responses = await asyncio.gather(*[api('POST', '/' + personal_id + '/models') for _ in range(5)])
            assert all(response.status_code == 200 for response in responses)
            assert counts[('refresh', refresh)] == 1
            async with get_async_db_context() as db:
                stored = await db.get(OAuthSession, personal_id)
                assert 'SECRET-' not in stored.token
                assert OAuthSessions._decrypt_token(stored.token)['refresh_token'] == refresh + '-rotated'
            retained = 'SECRET-retained-refresh'
            refresh_without_rotation.add(retained)
            await expire(personal_id, refresh=retained)
            assert (await api('POST', '/' + personal_id + '/models')).status_code == 200
            async with get_async_db_context() as db:
                assert OAuthSessions._decrypt_token((await db.get(OAuthSession, personal_id)).token)['refresh_token'] == retained

            # Stop after the provider spends a refresh token, but before its DB commit.
            # Both chat dispatch and catalog refresh must retain the replacement token.
            for operation_kind in ('chat', 'models'):
                refresh = await expire(personal_id, refresh=f'SECRET-cancel-{operation_kind}')
                commit_reached = asyncio.Event()
                allow_commit = asyncio.Event()
                original_commit = AsyncSession.commit

                async def delayed_commit(db):
                    for row in list(db.identity_map.values()):
                        if isinstance(row, OAuthSession) and row.id == personal_id:
                            if OAuthSessions._decrypt_token(row.token).get('refresh_token') == refresh + '-rotated':
                                commit_reached.set()
                                await allow_commit.wait()
                    await original_commit(db)

                AsyncSession.commit = delayed_commit
                operation = None
                cancelled = False
                before_inference = counts[('codex', 'inference')]
                before_models = counts[('codex', 'models')]
                try:
                    operation = asyncio.create_task(
                        core.refresh_models(users['alice'], personal_id)
                        if operation_kind == 'models' else core.generate_chat_completion(
                            request_for('alice'), {'model': raw, 'messages': [{'role': 'user', 'content': 'hello'}]},
                            users['alice'], alice_models[raw],
                        )
                    )
                    await asyncio.wait_for(commit_reached.wait(), 5)
                    assert counts[('refresh', refresh)] == 1 and refresh in spent
                    operation.cancel()
                    await asyncio.sleep(0)
                    assert not operation.done()
                    operation.cancel()  # Repeated Stop must not cancel the protected commit either.
                    await asyncio.sleep(0)
                    allow_commit.set()
                    try:
                        await operation
                    except asyncio.CancelledError:
                        cancelled = True
                finally:
                    allow_commit.set()
                    if operation is not None:
                        await asyncio.gather(operation, return_exceptions=True)
                    AsyncSession.commit = original_commit
                assert cancelled
                assert counts[('codex', 'inference')] == before_inference
                assert counts[('codex', 'models')] == before_models
                async with get_async_db_context() as db:
                    persisted = OAuthSessions._decrypt_token((await db.get(OAuthSession, personal_id)).token)
                    assert persisted['refresh_token'] == refresh + '-rotated'
                    assert persisted['expires_at'] > time.time()

            # Real adapter + HTTP transport: retry2 happens before output and cannot re-run the local tool.
            for provider, model_id in [('codex', raw), ('claude', f'subscription:{claude_id}:local-claude')]:
                messages = [{'role': 'system', 'content': 'Knowledge context'}, {'role': 'user', 'content': 'Look up seven'}]
                tools = [{'type': 'function', 'function': {'name': 'lookup', 'description': 'Read a number', 'parameters': {'type': 'object', 'properties': {'value': {'type': 'integer'}}}}}]
                fail_inference_once.add(provider)
                before = counts[(provider, 'inference')]
                response = await client.post('/test/completions', headers={'x-test-user': 'alice'}, json={'model': model_id, 'messages': messages, 'tools': tools, 'stream': False})
                assert response.status_code == 200, response.text
                call = response.json()['choices'][0]['message']['tool_calls'][0]
                assert call['id'] == 'call-local' and json.loads(call['function']['arguments']) == {'value': 7}
                assert counts[(provider, 'inference')] == before + 2
                executions = Counter()

                def lookup(value):
                    executions['lookup'] += 1
                    assert value == 7
                    return 'seven'

                tool_result = lookup(**json.loads(call['function']['arguments']))
                messages.extend([response.json()['choices'][0]['message'], {'role': 'tool', 'tool_call_id': call['id'], 'content': tool_result}])
                followup = await client.post('/test/completions', headers={'x-test-user': 'alice'}, json={'model': model_id, 'messages': messages, 'tools': tools, 'stream': False})
                assert followup.status_code == 200, followup.text
                assert followup.json()['choices'][0]['message']['content'] == 'local answer'
                assert executions['lookup'] == 1 and counts[(provider, 'inference')] == before + 3
                streamed = await client.post('/test/completions', headers={'x-test-user': 'alice'}, json={'model': model_id, 'messages': [{'role': 'user', 'content': 'hello'}], 'stream': True})
                assert streamed.status_code == 200 and 'local answer' in streamed.text and '[DONE]' in streamed.text
                assert 'SECRET-' not in streamed.text
            assert any(item.get('type') == 'function_call_output' and item.get('call_id') == 'call-local' for provider, body in wire if provider == 'codex' for item in body['input'])
            assert any(block.get('type') == 'tool_result' and block.get('tool_use_id') == 'call-local' for provider, body in wire if provider == 'claude' for message in body['messages'] for block in message['content'] if isinstance(block, dict))

            assert (await api('PATCH', '/' + personal_id, data={'enabled': False})).status_code == 200
            assert raw not in {model['id'] for model in await core.get_subscription_models(users['alice'])}
            await denied(core.resolve_subscription_model(users['alice'], raw), 409)
            assert (await api('PATCH', '/' + personal_id, data={'enabled': True, 'name': 'Renamed'})).json()['name'] == 'Renamed'
            invalid = 'SECRET-invalid-refresh'
            refresh_failures.add(invalid)
            await expire(personal_id, refresh=invalid)
            assert (await api('POST', '/' + personal_id + '/models')).status_code == 401
            assert next(row for row in (await api('GET', '/')).json() if row['id'] == personal_id)['status'] == 'reauth_required'
            assert (await api('DELETE', '/' + shared_id, 'admin2')).status_code == 200
            async with get_async_db_context() as db:
                assert await db.get(OAuthSession, shared_id) is None
                assert (await db.execute(select(AccessGrant).filter_by(resource_type=core.RESOURCE_TYPE, resource_id=shared_id))).scalars().first() is None
            await denied(core.resolve_subscription_model(users['bob'], shared_raw), 404)
    await close_session()
    await async_engine.dispose()
    print('subscription lifecycle, ownership, rotation, and real loopback provider smoke passed')


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    with tempfile.TemporaryDirectory(prefix='open-webui-subscriptions-') as directory:
        os.environ.update({
            'PYTHON_DOTENV_DISABLED': '1', 'FROM_INIT_PY': 'false', 'ENABLE_DB_MIGRATIONS': 'false',
            'DATA_DIR': directory, 'STATIC_DIR': str(Path(directory) / 'static'),
            'DATABASE_URL': f'sqlite:///{directory}/test.db', 'DATABASE_SCHEMA': 'main',
            'DATABASE_TYPE': '', 'DATABASE_USER': '', 'DATABASE_PASSWORD': '', 'DATABASE_HOST': '',
            'DATABASE_PORT': '', 'DATABASE_NAME': '', 'DATABASE_ENABLE_IAM_TOKEN_AUTH': 'false',
            'WEBUI_AUTH': 'true', 'WEBUI_SECRET_KEY': os.urandom(32).hex(), 'OAUTH_SESSION_TOKEN_ENCRYPTION_KEY': os.urandom(32).hex(),
            'WEBUI_JWT_SECRET_KEY': '', 'LICENSE_BLOB_PATH': '', 'ENV': 'test', 'OFFLINE_MODE': 'true',
            'USE_SLIM_DOCKER': 'true', 'ENABLE_FUNCTIONS': 'false', 'ENABLE_TOOLS': 'false',
            'REDIS_URL': '', 'WEBSOCKET_REDIS_URL': '', 'WEBSOCKET_MANAGER': '',
        })
        asyncio.run(_check())
