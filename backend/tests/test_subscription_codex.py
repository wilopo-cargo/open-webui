"""Codex protocol checks against disposable HTTP endpoints, without provider accounts."""

import asyncio
import base64
import json
import os
import subprocess
import sys
import tempfile
from collections import Counter
from copy import deepcopy
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from aiohttp import web
from aiohttp.test_utils import TestServer


async def _check():
    from fastapi import HTTPException
    from open_webui.utils.session_pool import close_session
    from open_webui.utils.subscriptions import codex

    counts = Counter()
    received = []
    claims = base64.urlsafe_b64encode(json.dumps({
        'https://api.openai.com/auth': {'chatgpt_account_id': 'acct-test'},
    }).encode()).decode().rstrip('=')
    token = {'access_token': 'test-access', 'id_token': f'test.{claims}.test'}
    second_claims = base64.urlsafe_b64encode(json.dumps({
        'https://api.openai.com/auth': {'chatgpt_account_id': 'acct-second'},
    }).encode()).decode().rstrip('=')
    second_token = {'access_token': 'second-access', 'id_token': f'test.{second_claims}.test'}
    image = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jR1kAAAAASUVORK5CYII='
    tool = {
        'type': 'function',
        'function': {'name': 'lookup', 'description': 'Look up a value', 'parameters': {'type': 'object', 'properties': {'x': {'type': 'integer'}}}},
    }
    reasoning = {'type': 'reasoning', 'id': 'rs_test', 'summary': [{'type': 'summary_text', 'text': 'Checking.'}], 'encrypted_content': 'ciphertext-only-for-test'}
    call = {'type': 'function_call', 'id': 'fc_test', 'status': 'completed', 'call_id': 'call_new', 'name': 'lookup', 'arguments': '{"x":2}'}
    usage = {'input_tokens': 17, 'output_tokens': 5, 'total_tokens': 22, 'input_tokens_details': {'cached_tokens': 4}, 'output_tokens_details': {'reasoning_tokens': 2}}

    async def oauth(request):
        counts['oauth'] += 1
        assert 'Cookie' not in request.headers
        if request.content_type == 'application/x-www-form-urlencoded':
            body = await request.post()
            assert dict(body) == {
                'grant_type': 'authorization_code', 'client_id': codex.OAUTH['client_id'],
                'code': 'test-code', 'redirect_uri': 'http://localhost:1455/auth/callback', 'code_verifier': 'test-verifier',
            }
            return web.json_response({**token, 'refresh_token': 'refresh-one', 'expires_in': 3600}, headers={'Set-Cookie': 'codex_cookie=oauth; Path=/'})
        assert request.content_type == 'application/json'
        body = await request.json()
        assert body['grant_type'] == 'refresh_token' and body['client_id'] == codex.OAUTH['client_id']
        if body['refresh_token'] == 'revoked':
            return web.json_response({'error': 'invalid_grant', 'error_description': 'test-secret-must-not-escape'}, status=400)
        assert body['refresh_token'] == 'refresh-one'
        return web.json_response({**token, 'refresh_token': 'refresh-two', 'expires_in': 3600}, headers={'Set-Cookie': 'codex_cookie=refresh; Path=/'})

    def check_headers(request):
        assert 'Cookie' not in request.headers
        assert request.headers['ChatGPT-Account-ID'] == {
            'Bearer test-access': 'acct-test', 'Bearer second-access': 'acct-second',
        }[request.headers['Authorization']]
        assert request.headers['originator'] == 'codex_cli_rs'
        assert request.headers['version'] == codex.CLIENT_VERSION
        assert request.headers['User-Agent'] == f'codex_cli_rs/{codex.CLIENT_VERSION}'

    async def models(request):
        counts['models'] += 1
        check_headers(request)
        assert request.query['client_version'] == codex.CLIENT_VERSION
        return web.json_response({'models': [
            {'slug': 'codex-test', 'display_name': 'Live test model', 'input_modalities': ['text', 'image']},
            {'id': 'unsupported-image-model', 'type': 'image'},
        ]}, headers={'Set-Cookie': 'codex_cookie=catalog; Path=/'})

    async def provider(request):
        case = request.match_info['case']
        counts[case] += 1
        check_headers(request)
        assert request.headers['session_id'] == 'session-test'
        body = await request.json()
        received.append(body)
        assert body['stream'] is True and body['store'] is False and body['prompt_cache_key'] == 'session-test'
        assert not ({'max_tokens', 'max_completion_tokens', 'max_output_tokens', 'temperature', 'previous_response_id', 'metadata'} & body.keys())
        if case == 'retry' and counts[case] == 1:
            return web.json_response({'error': 'transient'}, status=503, headers={'Retry-After': '0'})
        if case == 'unauthorized':
            return web.json_response({'error': 'test-secret-must-not-escape'}, status=401)
        response = {'id': 'resp_test', 'model': body['model'], 'created_at': 1, 'status': 'in_progress', 'output': []}
        events = [{'type': 'response.created', 'response': response}]
        if case == 'sse-error':
            events.append({'type': 'error', 'message': 'test-secret-must-not-escape'})
        elif any(item.get('type') == 'function_call_output' and item.get('call_id') == 'call_new' for item in body['input']):
            assert any(item.get('type') == 'reasoning' and item.get('encrypted_content') == reasoning['encrypted_content'] for item in body['input'])
            output = {'type': 'message', 'id': 'msg_answer', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': 'The answer is 2.'}]}
            events.extend([
                {'type': 'response.output_text.delta', 'output_index': 0, 'content_index': 0, 'delta': 'The answer is 2.'},
                {'type': 'response.output_item.done', 'output_index': 0, 'item': output},
                {'type': 'response.completed', 'response': {**response, 'status': 'completed', 'output': [output], 'usage': usage}},
            ])
        else:
            events.extend([
                {'type': 'response.output_item.added', 'output_index': 0, 'item': {'type': 'reasoning', 'id': 'rs_test', 'summary': []}},
                {'type': 'response.reasoning_summary_text.delta', 'output_index': 0, 'summary_index': 0, 'delta': 'Checking.'},
                {'type': 'response.output_item.done', 'output_index': 0, 'item': reasoning},
                {'type': 'response.output_item.added', 'output_index': 1, 'item': {**call, 'status': 'in_progress', 'arguments': ''}},
                {'type': 'response.function_call_arguments.delta', 'output_index': 1, 'delta': '{"x":'},
                {'type': 'response.function_call_arguments.delta', 'output_index': 1, 'delta': '2}'},
                {'type': 'response.output_item.done', 'output_index': 1, 'item': call},
            ])
            if case == 'failed-after-call':
                events.append({'type': 'response.failed', 'response': {**response, 'status': 'failed', 'error': {'message': 'test-secret-must-not-escape'}}})
            elif case != 'truncated':
                incomplete = case == 'incomplete-after-call'
                final = {**response, 'status': 'incomplete' if incomplete else 'completed', 'usage': usage}
                if incomplete:
                    final['incomplete_details'] = {'reason': 'max_output_tokens'}
                if case != 'items-only':
                    final['output'] = [reasoning, call]
                events.append({'type': 'response.incomplete' if incomplete else 'response.completed', 'response': final})
        wire = ''.join(f'event: {event["type"]}\r\ndata: {json.dumps(event)}\r\n\r\n' for event in events)
        return web.Response(body=wire.encode(), content_type='text/event-stream', headers={'Set-Cookie': 'codex_cookie=inference; Path=/'})

    async def expect_error(awaitable, status):
        try:
            await awaitable
        except HTTPException as exc:
            assert exc.status_code == status, (exc.status_code, exc.detail)
            assert 'test-secret-must-not-escape' not in str(exc.detail)
        else:
            raise AssertionError(f'Expected HTTP {status}')

    app = web.Application()
    app.router.add_post('/oauth', oauth)
    app.router.add_get('/models', models)
    app.router.add_post('/responses/{case}', provider)
    try:
        async with TestServer(app) as server:
            # localhost (not an IP literal) makes a normal aiohttp cookie jar accept these cookies.
            # The production provider sessions must discard them between accounts and operations.
            codex.OAUTH['token_url'] = str(server.make_url('/oauth').with_host('localhost'))
            codex.MODELS_URL = str(server.make_url(f'/models?client_version={codex.CLIENT_VERSION}').with_host('localhost'))
            params = parse_qs(urlsplit(codex.authorization_url('bound-state', 'challenge')).query)
            assert params['redirect_uri'] == ['http://localhost:1455/auth/callback']
            assert params['state'] == ['bound-state'] and params['code_challenge_method'] == ['S256']
            assert params['code_challenge'] == ['challenge'] and params['codex_cli_simplified_flow'] == ['true']
            assert (await codex.exchange_code('test-code', 'bound-state', 'test-verifier'))['refresh_token'] == 'refresh-one'
            assert (await codex.refresh_token('refresh-one'))['refresh_token'] == 'refresh-two'
            await expect_error(codex.refresh_token('revoked'), 400)
            assert counts['oauth'] == 3
            catalog = await codex.list_models(token)
            assert catalog == [{'id': 'codex-test', 'name': 'Live test model', 'capabilities': {'vision': True, 'builtin_tools': True}, 'responses_lite': False}]
            assert await codex.list_models(second_token) == catalog
            payload = {
                'model': 'codex-test', 'stream': False, 'max_tokens': 12, 'temperature': 0.2,
                'previous_response_id': 'resp_stale', 'metadata': {'must_not_forward': True},
                'messages': [
                    {'role': 'system', 'content': 'System instructions.'},
                    {'role': 'system', 'content': [{'type': 'text', 'text': 'Knowledge context.'}]},
                    {'role': 'user', 'content': [{'type': 'text', 'text': 'Use this image.'}, {'type': 'image_url', 'image_url': {'url': image, 'detail': 'high'}}]},
                    {'role': 'assistant', 'content': 'First lookup.', 'tool_calls': [{'id': 'call_old', 'type': 'function', 'function': {'name': 'lookup', 'arguments': '{"x":1}'}}]},
                    {'role': 'tool', 'tool_call_id': 'call_old', 'content': [{'type': 'text', 'text': 'First result.'}, {'type': 'input_image', 'image_url': image}]},
                    {'role': 'user', 'content': 'Now look up 2.'},
                ],
                'tools': [tool], 'tool_choice': {'type': 'function', 'function': {'name': 'lookup'}},
                'reasoning_effort': 'high', 'response_format': {'type': 'json_object'},
            }
            unchanged = deepcopy(payload)

            async def send(case, body=payload, credential=token):
                codex.RESPONSES_URL = str(server.make_url('/responses/' + case).with_host('localhost'))
                return await codex.generate_chat_completion(body, credential, 'session-test')

            result = await send('complete')
            assert payload == unchanged
            message = result['choices'][0]['message']
            assert message['tool_calls'] == [{'id': 'call_new', 'type': 'function', 'function': {'name': 'lookup', 'arguments': '{"x":2}'}}]
            assert message['reasoning_content'] == 'Checking.'
            assert message['reasoning_details'][0]['data'] == reasoning['encrypted_content']
            assert result['choices'][0]['finish_reason'] == 'tool_calls'
            assert result['usage']['prompt_tokens'] == 17 and result['usage']['completion_tokens'] == 5
            assert result['usage']['prompt_tokens_details']['cached_tokens'] == 4
            assert result['usage']['completion_tokens_details']['reasoning_tokens'] == 2
            body = received[-1]
            assert body['instructions'] == 'System instructions.\n\nKnowledge context.'
            assert body['reasoning']['effort'] == 'high' and 'reasoning.encrypted_content' in body['include']
            assert body['tool_choice'] == {'type': 'function', 'name': 'lookup'}
            assert body['tools'][0]['name'] == 'lookup' and body['text']['format'] == {'type': 'json_object'}
            assert any(item.get('type') == 'function_call' and item['call_id'] == 'call_old' for item in body['input'])
            prior_result = next(item for item in body['input'] if item.get('type') == 'function_call_output')
            assert prior_result['call_id'] == 'call_old' and prior_result['output'][0]['text'] == 'First result.'
            assert prior_result['output'][1]['image_url'] == image
            assert body['input'][0]['content'][1]['image_url'] == image
            assert (await send('items-only'))['choices'][0]['message']['tool_calls'] == message['tool_calls']

            streamed = await send('stream', {**payload, 'stream': True})
            chunks = [json.loads(line[6:]) async for line in streamed.body_iterator if line.startswith('data: ') and '[DONE]' not in line]
            assert {chunk['id'] for chunk in chunks} == {'resp_test'}
            deltas = [chunk['choices'][0]['delta'] for chunk in chunks]
            assert ''.join(delta.get('reasoning_content', '') for delta in deltas) == 'Checking.'
            calls = [call for delta in deltas for call in delta.get('tool_calls', [])]
            assert calls[0]['id'] == 'call_new' and calls[0]['function']['name'] == 'lookup'
            assert ''.join(call['function'].get('arguments', '') for call in calls) == '{"x":2}'
            assert any(delta.get('reasoning_details', [{}])[0].get('data') == reasoning['encrypted_content'] for delta in deltas)
            assert chunks[-1]['usage']['prompt_tokens'] == 17 and chunks[-1]['choices'][0]['finish_reason'] == 'tool_calls'

            follow_up = {**payload, 'tool_choice': 'auto', 'messages': [*payload['messages'], message, {'role': 'tool', 'tool_call_id': 'call_new', 'content': '2'}]}
            completed = await send('follow-up', follow_up)
            assert completed['choices'][0]['message']['content'] == 'The answer is 2.'
            assert completed['choices'][0]['finish_reason'] == 'stop'
            await send('second-user', credential=second_token)
            await send('retry')
            assert counts['retry'] == 2
            await expect_error(send('unauthorized'), 401)
            assert counts['unauthorized'] == 1
            await expect_error(send('sse-error'), 502)
            await expect_error(send('truncated'), 502)
            assert counts['sse-error'] == counts['truncated'] == 1
            for case in ('truncated', 'failed-after-call'):
                before = counts[case]
                interrupted = await send(case, {**payload, 'stream': True})
                wire = ''.join([chunk async for chunk in interrupted.body_iterator])
                assert counts[case] == before + 1  # Never replay after exposing tool arguments.
                assert 'codex_stream_error' in wire and wire.endswith('data: [DONE]\n\n')
                assert 'test-secret-must-not-escape' not in wire
                chunks = [json.loads(line[6:]) for line in wire.splitlines() if line.startswith('data: ') and '[DONE]' not in line]
                choices = [chunk['choices'][0] for chunk in chunks if chunk.get('choices')]
                assert any(choice['delta'].get('tool_calls') for choice in choices)
                assert choices[-1]['finish_reason'] == 'failed' and not choices[-1]['delta']
                assert {chunk['id'] for chunk in chunks if chunk.get('choices')} == {'resp_test'}
            incomplete = await send('incomplete-after-call', {**payload, 'stream': True})
            chunks = [json.loads(line[6:]) async for line in incomplete.body_iterator if line.startswith('data: ') and '[DONE]' not in line]
            assert chunks[-1]['choices'][0]['finish_reason'] == 'length'
            assert (await send('incomplete-after-call'))['choices'][0]['finish_reason'] == 'length'
            await expect_error(send('failed-after-call'), 502)
            for bad in ({'stop': ['END']}, {'tools': [{'type': 'mcp'}]}, {'tool_choice': 'invalid'}):
                await expect_error(send('must-not-send', {**payload, **bad}), 400)
            assert not counts['must-not-send']
            for url in ('http://127.0.0.1/image.png', 'http://[::1]/image.png', 'file:///etc/passwd', str(server.make_url('/private-image'))):
                await expect_error(codex._inline_image(url), 400)
            assert (await codex._inline_image(image)) == image
            lite, is_lite = codex._prepare_payload({**payload, 'model': 'gpt-6-sol', 'reasoning_effort': 'high'}, token, 'session-test')
            assert is_lite and lite['input'][0]['type'] == 'additional_tools' and lite['input'][0]['tools'][0]['name'] == 'lookup'
            assert lite['tools'] is None and lite['reasoning']['context'] == 'all_turns'
            assert codex._headers(token, 'session-test', lite=True)['x-openai-internal-codex-responses-lite'] == 'true'
        print('Codex: OAuth/refresh/catalog, cross-account cookie isolation, forced SSE + streaming tools/reasoning/usage/vision, tool-result replay, retry2, error sanitization and SSRF guards passed (synthetic endpoints).')
    finally:
        await close_session()


def test_subscription_codex():
    subprocess.run([sys.executable, str(Path(__file__).resolve())], check=True)


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    with tempfile.TemporaryDirectory(prefix='open-webui-subscription-codex-') as directory:
        os.environ.update(
            PYTHON_DOTENV_DISABLED='1', FROM_INIT_PY='false', WEBUI_AUTH='true',
            WEBUI_SECRET_KEY=os.urandom(32).hex(), DATA_DIR=directory,
            STATIC_DIR=str(Path(directory) / 'static'), LICENSE_BLOB_PATH='',
            DATABASE_URL=f'sqlite:///{directory}/test.db',
            NO_PROXY='localhost,127.0.0.1,::1', no_proxy='localhost,127.0.0.1,::1',
        )
        asyncio.run(_check())
