"""Claude wire/translation regression using disposable loopback HTTP, never real accounts."""

import asyncio
import copy
import json
import os
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from aiohttp import web
from aiohttp.test_utils import TestServer
from fastapi import HTTPException


async def _check():
    from open_webui.utils.misc import convert_output_to_messages
    from open_webui.utils.session_pool import close_session, get_session
    from open_webui.utils.subscriptions import claude
    from open_webui.utils.tool_calling import final_answer_payload

    token = {'access_token': 'test-only'}
    counts = Counter()
    received = []
    disconnected = asyncio.Event()
    native = {
        'id': 'msg_test_native', 'type': 'message', 'role': 'assistant', 'model': 'claude-sonnet-4-6',
        'content': [
            {'type': 'thinking', 'thinking': 'Need the weather.', 'signature': 'real-test-signature'},
            {'type': 'redacted_thinking', 'data': 'opaque-test-thinking'},
            {'type': 'text', 'text': 'Checking café weather 🔧'},
            {'type': 'tool_use', 'id': 'toolu_weather', 'name': 'weather', 'input': {'city': 'Jakarta', 'unit': '°C'}},
            {'type': 'tool_use', 'id': 'toolu_clock', 'name': 'clock', 'input': {}},
        ],
        'stop_reason': 'tool_use', 'stop_sequence': None,
        'usage': {'input_tokens': 2, 'cache_read_input_tokens': 3, 'cache_creation_input_tokens': 5,
                  'cache_creation': {'ephemeral_5m_input_tokens': 5, 'ephemeral_1h_input_tokens': 0},
                  'output_tokens': 11},
    }

    def events(message=native):
        start = {key: value for key, value in message.items() if key not in ('content', 'stop_reason', 'stop_sequence', 'usage')}
        start.update({'content': [], 'stop_reason': None,
                      'usage': {**message['usage'], 'output_tokens': 1}})
        output = [{'type': 'message_start', 'message': start}, {'type': 'ping'}]
        for index, block in enumerate(message['content']):
            beginning = dict(block)
            deltas = []
            if block['type'] == 'text':
                beginning['text'] = ''
                deltas = [{'type': 'text_delta', 'text': block['text']}]
            elif block['type'] == 'thinking':
                beginning.update({'thinking': '', 'signature': ''})
                signature = block['signature']
                middle = len(signature) // 2
                deltas = [{'type': 'thinking_delta', 'thinking': block['thinking']},
                          {'type': 'signature_delta', 'signature': signature[:middle]},
                          {'type': 'signature_delta', 'signature': signature[middle:]}]
            elif block['type'] == 'tool_use':
                beginning['input'] = {}
                if block['input']:
                    arguments = json.dumps(block['input'], ensure_ascii=False)
                    middle = len(arguments) // 2
                    deltas = [{'type': 'input_json_delta', 'partial_json': arguments[:middle]},
                              {'type': 'input_json_delta', 'partial_json': arguments[middle:]}]
            output.append({'type': 'content_block_start', 'index': index, 'content_block': beginning})
            output.extend({'type': 'content_block_delta', 'index': index, 'delta': delta} for delta in deltas)
            output.append({'type': 'content_block_stop', 'index': index})
        output.extend([
            {'type': 'message_delta', 'delta': {'stop_reason': message['stop_reason'], 'stop_sequence': message['stop_sequence'],
                                               **({'stop_details': message['stop_details']} if 'stop_details' in message else {})},
             'usage': {'output_tokens': message['usage']['output_tokens']}},
            {'type': 'message_stop'},
        ])
        return output

    def encode(items):
        return ''.join('event: ' + item['type'] + '\r\ndata: ' + json.dumps(item, ensure_ascii=False) + '\r\n\r\n'
                       for item in items).encode()

    async def oauth(request):
        counts['token'] += 1
        body = await request.json()
        assert body['client_id'] == claude.OAUTH['client_id']
        assert not body.get('client_secret')
        if body['grant_type'] == 'authorization_code':
            assert body['code'] == 'test-code'
            assert body['state'] == 'state-test'
            assert body['code_verifier'] == 'test-verifier'
            assert body['redirect_uri'] == claude.OAUTH['redirect_uri']
        else:
            assert body['grant_type'] == 'refresh_token'
            if body['refresh_token'] == 'revoked':
                return web.json_response({'error': 'invalid_grant', 'description': 'test-only-secret'}, status=400)
            if body['refresh_token'] == 'unavailable':
                return web.json_response({'error': 'test-only-secret'}, status=503)
            assert body['refresh_token'] == 'test-refresh'
        return web.json_response({'access_token': 'test-only', 'refresh_token': 'rotated-test', 'expires_in': 3600})

    async def models(request):
        counts['models'] += 1
        auth = request.headers['Authorization']
        assert request.headers['Anthropic-Version'] == '2023-06-01'
        assert 'oauth-2025-04-20' in request.headers['Anthropic-Beta']
        if auth == 'Bearer empty':
            return web.json_response({'data': [], 'has_more': False})
        assert auth == 'Bearer test-only'
        assert request.query['limit'] == '1000'
        if request.query.get('after_id'):
            assert request.query['after_id'] == 'claude-sonnet-4-6'
            return web.json_response({'data': [{'id': 'claude-haiku-4-5', 'display_name': 'Live Haiku'}], 'has_more': False})
        return web.json_response({'data': [{'id': 'claude-sonnet-4-6', 'display_name': 'Live Sonnet'}],
                                  'has_more': True, 'last_id': 'claude-sonnet-4-6'})

    async def inference(request):
        case = request.match_info['case']
        counts[case] += 1
        assert request.headers['Authorization'] == 'Bearer test-only'
        assert request.headers['Anthropic-Version'] == '2023-06-01'
        assert 'oauth-2025-04-20' in request.headers['Anthropic-Beta']
        assert not any(key.lower().startswith('x-stainless-') for key in request.headers)
        body = await request.json()
        received.append((case, body))
        assert body['model'] == 'claude-sonnet-4-6'
        assert 'metadata' not in body and 'user' not in body
        if case == 'retry' and counts[case] == 1:
            return web.Response(status=503, text='test-only-secret', headers={'Retry-After': '0'})
        if case == 'rejected':
            return web.Response(status=401, text='test-only-secret')
        if case == 'continued':
            assistant = body['messages'][1]['content']
            assert assistant[0] == native['content'][0]
            assert assistant[1] == native['content'][1]
            assert [block['id'] for block in assistant if block['type'] == 'tool_use'] == ['toolu_weather', 'toolu_clock']
            results = body['messages'][2]['content']
            assert [block['tool_use_id'] for block in results] == ['toolu_weather', 'toolu_clock']
            assert results[0]['content'][0] == {'type': 'text', 'text': 'Sunny'}
            assert results[0]['content'][1]['type'] == 'image'
            assert results[0]['content'][1]['source']['data'] == 'aGVsbG8='
            return web.json_response({**native, 'content': [{'type': 'text', 'text': 'It is sunny.'}], 'stop_reason': 'end_turn'})
        if case == 'json' or case == 'retry':
            return web.json_response(native)
        response = web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
        response.set_cookie('stream_provider_session', 'must-not-survive')
        await response.prepare(request)
        if case == 'cancel':
            await response.write(encode(events()[:1]))
            transport = request.transport
            try:
                for _ in range(200):
                    if transport.is_closing():
                        break
                    await asyncio.sleep(0.01)
            finally:
                # TestServer cancels the handler on disconnect before the next poll.
                if transport.is_closing():
                    disconnected.set()
            return response
        items = events()
        if case == 'stream-error':
            items = items[:-2] + [{'type': 'error', 'error': {'type': 'overloaded_error', 'message': 'test-only-secret'}}]
        elif case == 'truncated':
            items = items[:-2]
        raw = encode(items)
        for offset in range(0, len(raw), 7):
            await response.write(raw[offset:offset + 7])
        return response

    async def output(response):
        raw = ''.join([chunk.decode() if isinstance(chunk, bytes) else chunk async for chunk in response.body_iterator])
        assert raw.endswith('data: [DONE]\n\n')
        return [json.loads(line[6:]) for line in raw.splitlines() if line.startswith('data: ') and line != 'data: [DONE]']

    async def as_async(items):
        for item in items:
            yield item

    def assert_completion(completion):
        assert completion['id'] == 'msg_test_native'
        assert completion['model'] == 'claude-sonnet-4-6'
        choice = completion['choices'][0]
        assert choice['finish_reason'] == 'tool_calls' and choice['stop_reason'] == 'tool_use'
        message = choice['message']
        assert message['content'] == 'Checking café weather 🔧'
        assert message['reasoning_content'] == 'Need the weather.'
        assert message['reasoning_details'][0]['signature'] == 'real-test-signature'
        assert message['reasoning_details'][1]['data'] == 'opaque-test-thinking'
        assert [call['id'] for call in message['tool_calls']] == ['toolu_weather', 'toolu_clock']
        assert json.loads(message['tool_calls'][0]['function']['arguments']) == {'city': 'Jakarta', 'unit': '°C'}
        assert message['tool_calls'][1]['function']['arguments'] == '{}'
        assert completion['usage']['prompt_tokens'] == 10
        assert completion['usage']['completion_tokens'] == 11
        assert completion['usage']['total_tokens'] == 21
        assert completion['usage']['prompt_tokens_details'] == {'cached_tokens': 3, 'cache_creation_tokens': 5}
        assert completion['usage']['cache_creation']['ephemeral_5m_input_tokens'] == 5

    query = parse_qs(urlsplit(claude.authorization_url('state-test', 'test-challenge')).query)
    assert query['redirect_uri'] == ['http://localhost:20128/callback']
    assert query['state'] == ['state-test'] and query['code_challenge'] == ['test-challenge']
    assert query['code_challenge_method'] == ['S256'] and query['code'] == ['true']
    assert query['scope'] == ['org:create_api_key user:profile user:inference']

    body = {
        'model': 'claude-sonnet-4-6', 'stream': False, 'max_completion_tokens': 12000,
        'messages': [
            {'role': 'system', 'content': [{'type': 'text', 'text': 'Use the knowledge context.', 'cache_control': {'type': 'ephemeral'}}]},
            {'role': 'developer', 'content': 'Knowledge: Jakarta is in Indonesia.'},
            {'role': 'user', 'content': [{'type': 'text', 'text': 'Weather?'},
                                         {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,aGVsbG8='}},
                                         {'type': 'image_url', 'image_url': {'url': 'https://example.test/map.png'}}]},
        ],
        'tools': [{'type': 'function', 'function': {'name': 'weather', 'parameters': {'type': 'object', 'properties': {'city': {'type': 'string'}}}}},
                  {'type': 'function', 'function': {'name': 'clock', 'parameters': {'type': 'object', 'properties': {}}}}],
        'tool_choice': 'auto', 'parallel_tool_calls': False, 'reasoning_effort': 'medium',
        'stop': ['END'], 'metadata': {'chat_id': 'private-webui-only'}, 'user': 'not-forwarded',
    }
    before = copy.deepcopy(body)
    converted = claude._payload(body)
    assert body == before
    assert converted['max_tokens'] == 12000
    assert converted['system'][0]['cache_control'] == {'type': 'ephemeral'}
    assert converted['system'][1]['text'] == 'Knowledge: Jakarta is in Indonesia.'
    assert converted['messages'][0]['content'][1]['source']['type'] == 'base64'
    assert converted['messages'][0]['content'][2]['source']['type'] == 'url'
    assert converted['tool_choice'] == {'type': 'auto', 'disable_parallel_tool_use': True}
    assert converted['thinking'] == {'type': 'adaptive', 'display': 'summarized'}
    assert converted['output_config']['effort'] == 'medium'
    assert converted['stop_sequences'] == ['END']
    assert claude._payload({**body, 'tool_choice': 'none'})['tool_choice'] == {'type': 'none'}
    assert claude._payload({**body, 'reasoning_effort': None, 'tool_choice': 'required'})['tool_choice']['type'] == 'any'
    forced = {'type': 'function', 'function': {'name': 'weather'}}
    assert claude._payload({**body, 'reasoning_effort': None, 'tool_choice': forced})['tool_choice']['name'] == 'weather'
    old_model = claude._payload({**body, 'model': 'claude-sonnet-4-5', 'reasoning_effort': 'low'})
    assert old_model['thinking']['budget_tokens'] == 1024
    # Internally generated max effort must leave output room without raising an explicit limit.
    budget_body = {key: value for key, value in body.items() if key != 'max_completion_tokens'}
    budget_body.update({'model': 'claude-opus-4-20250514', 'reasoning_effort': 'max'})
    budget_default = claude._payload(budget_body)
    assert budget_default['max_tokens'] == 64000
    assert budget_default['thinking']['budget_tokens'] == 62976
    for limit in (1500, 12000, 32000):
        budget_explicit = claude._payload({**budget_body, 'max_tokens': limit})
        assert budget_explicit['max_tokens'] == limit
        assert 1024 <= budget_explicit['thinking']['budget_tokens'] < limit
    native_budget = claude._payload({
        **budget_body, 'model': 'claude-opus-4-6', 'reasoning_effort': None,
        'thinking': {'type': 'enabled', 'budget_tokens': 126976},
    })
    assert native_budget['max_tokens'] == 128000 and native_budget['thinking']['budget_tokens'] == 126976
    for family in ('opus', 'sonnet'):
        for separator in ('-', '.'):
            model = f'claude-{family}-4{separator}6'
            assert claude._payload({**body, 'model': model, 'reasoning_effort': 'xhigh'})['output_config']['effort'] == 'high'
            assert claude._payload({**body, 'model': model, 'reasoning_effort': None,
                                    'output_config': {'effort': 'xhigh'}})['output_config']['effort'] == 'high'
    for separator in ('-', '.'):
        for off in ('none', 'off'):
            opus_off = claude._payload({**body, 'model': f'claude-opus-5{separator}5', 'reasoning_effort': off})
            assert opus_off['thinking'] == {'type': 'adaptive', 'display': 'omitted'}
            assert opus_off['output_config']['effort'] == 'low'
            sonnet_off = claude._payload({**body, 'model': f'claude-sonnet-5{separator}5', 'reasoning_effort': off})
            assert sonnet_off['thinking'] == {'type': 'between_tools'}
    sonnet_native_off = claude._payload({
        **body, 'model': 'claude-sonnet-5-5', 'reasoning_effort': None,
        'thinking': {'type': 'disabled'}, 'output_config': {'effort': 'max'},
    })
    assert sonnet_native_off['thinking'] == {'type': 'between_tools'}
    assert sonnet_native_off['output_config']['effort'] == 'high'
    schema = {'type': 'object', 'properties': {'ok': {'type': 'boolean'}}, 'additionalProperties': False}
    assert claude._payload({**body, 'response_format': {'type': 'json_schema', 'json_schema': {'name': 'result', 'schema': schema}}})['output_config']['format']['schema'] == schema
    for unsupported in (
        {'mcp_servers': [{'url': 'https://example.test'}]}, {'tools': [{'type': 'web_search'}]},
        {'response_format': {'type': 'json_object'}}, {'max_completion_tokens': 0},
        {'messages': [{'role': 'user', 'content': [{'type': 'input_audio', 'input_audio': {'data': 'not-audio'}}]}]},
        {'messages': [{'role': 'tool', 'tool_call_id': 'orphan', 'content': 'no call'}]},
        {'model': 'claude-sonnet-5-5', 'reasoning_effort': None, 'tool_choice': 'required'},
        {'model': 'claude-opus-5-5', 'reasoning_effort': None, 'tool_choice': forced},
    ):
        try:
            claude._payload({**body, **unsupported})
        except HTTPException as error:
            assert error.status_code == 400
        else:
            raise AssertionError('Unsupported functional input must fail clearly')

    # Byte-fragmented CRLF SSE, including UTF-8 split across chunks.
    raw = encode(events())
    fragments = (raw[index:index + 3] for index in range(0, len(raw), 3))
    rebuilt = await claude._collect(claude._chat_chunks(claude._events(as_async(fragments)), body['model']))
    assert_completion(rebuilt)
    # Signed thinking with omitted text must survive the real saved-output filter too.
    omitted = copy.deepcopy(native)
    omitted['content'][0]['thinking'] = ''
    omitted_completion = claude._completion(omitted, body['model'])
    omitted_stream = await claude._collect(claude._chat_chunks(claude._events(as_async([encode(events(omitted))])), body['model']))
    for response in (omitted_completion, omitted_stream):
        saved = convert_output_to_messages([{'type': 'reasoning', 'reasoning_details': response['choices'][0]['message']['reasoning_details']}], raw=True)
        assert saved[0]['reasoning_details'][0]['signature'] == 'real-test-signature'
        assert saved[0]['reasoning_details'][1]['data'] == 'opaque-test-thinking'
    for reason, expected in (('max_tokens', 'length'), ('model_context_window_exceeded', 'length'),
                             ('end_turn', 'stop'), ('stop_sequence', 'stop'), ('refusal', 'content_filter'), ('pause_turn', 'stop')):
        response = claude._completion({**native, 'stop_reason': reason, 'stop_sequence': 'END'}, body['model'])
        assert response['choices'][0]['finish_reason'] == expected
        assert response['choices'][0]['stop_sequence'] == 'END'
    explanation = 'I cannot help with that request.'
    for text in ('', explanation, 'Existing visible refusal.'):
        refused = {
            **native, 'content': [{'type': 'text', 'text': text}] if text else [],
            'stop_reason': 'refusal', 'stop_details': {'explanation': explanation},
        }
        refused_json = claude._completion(refused, body['model'])
        refused_stream = await claude._collect(claude._chat_chunks(claude._events(as_async([encode(events(refused))])), body['model']))
        for response in (refused_json, refused_stream):
            assert response['choices'][0]['message']['content'] == (text or explanation)
            assert response['choices'][0]['message']['refusal'] == explanation
            assert response['choices'][0]['finish_reason'] == 'content_filter'
    def saved_replay(completion):
        message = completion['choices'][0]['message']
        items = [{'type': 'reasoning', 'reasoning_details': message.get('reasoning_details', [])}]
        if message.get('content'):
            items.append({'type': 'message', 'content': [{'type': 'output_text', 'text': message['content']}]})
        calls = message.get('tool_calls', [])
        items.extend({'type': 'function_call', 'call_id': call['id'], 'name': call['function']['name'],
                      'arguments': call['function']['arguments'], 'status': 'completed'} for call in calls)
        items.extend({'type': 'function_call_output', 'call_id': call['id'],
                      'output': [{'type': 'input_text', 'text': 'Tool completed'}]} for call in calls)
        return convert_output_to_messages(items, raw=True)

    preserved_body = {**body, 'model': 'claude-fable-5-2'}
    preserved_prefix = claude._payload(preserved_body)
    preserved_native = {
        **native, 'model': preserved_body['model'],
        'content': [
            {'type': 'thinking', 'thinking': '', 'signature': 'opaque-intact-signature'},
            {'type': 'text', 'text': 'Before '}, native['content'][3],
            {'type': 'thinking', 'thinking': 'More reasoning', 'signature': 'another-real-signature'},
            {'type': 'text', 'text': 'after '}, native['content'][4], native['content'][1],
        ],
    }
    preserved_json = claude._completion(preserved_native, preserved_body['model'], preserved_prefix)
    preserved_stream = await claude._collect(claude._chat_chunks(
        claude._events(as_async([encode(events(preserved_native))])), preserved_body['model'], preserved_prefix
    ))
    for completion in (preserved_json, preserved_stream):
        replay = saved_replay(completion)
        current = {**preserved_body, 'messages': [*preserved_body['messages'], *replay]}
        restored = claude._payload(current)
        # The saved-chat helper aggregates text/thinking/tools; native order must still be exact.
        assert restored['messages'][1]['content'] == preserved_native['content']
        assert 'opaque-intact-signature' in json.dumps(restored['messages'][1])
        assert '_claude_' not in json.dumps(restored)
        final = claude._payload(final_answer_payload(current))
        assert final['system'] == preserved_prefix['system']
        assert final['tools'] == preserved_prefix['tools']
        assert final['tool_choice'] == {'type': 'none'}
        assert final['messages'][-1]['role'] == 'system'
        assert final['messages'][1]['content'] == preserved_native['content']
        recached = copy.deepcopy(current)
        recached['messages'][0]['content'][0]['cache_control'] = {'type': 'ephemeral', 'ttl': '1h'}
        assert claude._payload(recached)['messages'][1]['content'] == preserved_native['content']
        for change in ('system', 'tools', 'history', 'arguments'):
            changed = copy.deepcopy(current)
            if change == 'system':
                changed['messages'][0]['content'][0]['text'] += ' Changed.'
            elif change == 'tools':
                changed['tools'][0]['function']['description'] = 'A changed tool definition.'
            elif change == 'history':
                changed['messages'][2]['content'][0]['text'] = 'A changed earlier user turn.'
            else:
                changed['messages'][3]['tool_calls'][0]['function']['arguments'] = '{\"city\":\"Bandung\"}'
            try:
                claude._payload(changed)
            except HTTPException as error:
                assert error.status_code == 400 and 'open tool turn' in error.detail
            else:
                raise AssertionError('Changed open tool-turn prefix must fail locally, never replay invalid thinking')
        # Closing that tool round allows obsolete thinking to be removed after an intentional edit.
        next_response_native = {
            **preserved_native, 'stop_reason': 'end_turn',
            'content': [{'type': 'thinking', 'thinking': '', 'signature': 'next-intact-signature'},
                        {'type': 'text', 'text': 'Final answer.'}],
        }
        next_completion = claude._completion(next_response_native, preserved_body['model'], restored)
        completed = {
            **current,
            'messages': [*current['messages'], *saved_replay(next_completion), {'role': 'user', 'content': 'Continue.'}],
        }
        completed['messages'][0] = {'role': 'system', 'content': 'The operator intentionally changed the context.'}
        cleaned = claude._payload(completed)
        assert not any(block['type'] in ('thinking', 'redacted_thinking')
                       for message in cleaned['messages'] for block in message['content'])
        assert ''.join(block.get('text', '') for block in cleaned['messages'][1]['content']
                       if block['type'] == 'text') == 'Before after '
        assert 'Final answer.' in json.dumps(cleaned)
        # Fresh signatures bind to the edited, cleaned prefix rather than inheriting old invalidation.
        fresh_completion = claude._completion(next_response_native, preserved_body['model'], cleaned)
        fresh = claude._payload({**completed, 'messages': [*completed['messages'], *saved_replay(fresh_completion)]})
        assert fresh['messages'][-1]['content'][0]['signature'] == 'next-intact-signature'
    try:
        async for _ in claude._events(as_async([b'data: ' + b'x' * (claude.MAX_EVENT_BYTES + 1)])):
            pass
    except HTTPException as error:
        assert error.status_code == 502
    else:
        raise AssertionError('Oversized SSE must not be buffered without a bound')

    async def ambient_cookie(request):
        return web.Response(headers={'Set-Cookie': 'provider_session=other-user; Path=/'})

    @web.middleware
    async def isolated_cookies(request, handler):
        if request.path != '/ambient':
            assert 'Cookie' not in request.headers, 'Provider requests inherited another user/account cookie'
        response = await handler(request)
        if request.path != '/ambient' and not response.prepared:
            response.set_cookie('provider_session', 'must-not-survive')
        return response

    app = web.Application(middlewares=[isolated_cookies])
    app.router.add_get('/ambient', ambient_cookie)
    app.router.add_post('/token', oauth)
    app.router.add_get('/models', models)
    app.router.add_post('/messages/{case}', inference)
    original_oauth = dict(claude.OAUTH)
    original_messages = claude.MESSAGES_URL
    original_models = claude.MODELS_URL
    try:
        async with TestServer(app) as server:
            def endpoint(path):
                # CookieJar intentionally rejects IP literals: exercise localhost cookies.
                return str(server.make_url(path).with_host('localhost'))

            shared = await get_session()
            async with shared.get(endpoint('/ambient')) as response:
                await response.read()
            jar_url = server.make_url('/').with_host('localhost')
            assert shared.cookie_jar.filter_cookies(jar_url)['provider_session'].value == 'other-user'
            claude.OAUTH['token_url'] = endpoint('/token')
            claude.MODELS_URL = endpoint('/models')
            exchanged = await claude.exchange_code('test-code#state-test', 'state-test', 'test-verifier')
            assert exchanged['refresh_token'] == 'rotated-test'
            assert (await claude.refresh_token('test-refresh'))['access_token'] == 'test-only'
            count = counts['token']
            try:
                await claude.exchange_code('test-code#other-state', 'state-test', 'test-verifier')
            except HTTPException as error:
                assert error.status_code == 400 and counts['token'] == count
            else:
                raise AssertionError('Mismatched state must never reach token exchange')
            for refresh, status in (('revoked', 400), ('unavailable', 503)):
                count = counts['token']
                try:
                    await claude.refresh_token(refresh)
                except HTTPException as error:
                    assert error.status_code == status and 'test-only-secret' not in error.detail
                    assert counts['token'] == count + 1
                else:
                    raise AssertionError('Token errors must surface without inference retries')
            assert await claude.list_models(token) == [{'id': 'claude-sonnet-4-6', 'name': 'Live Sonnet'}, {'id': 'claude-haiku-4-5', 'name': 'Live Haiku'}]
            try:
                await claude.list_models({'access_token': 'empty'})
            except HTTPException as error:
                assert error.status_code == 502
            else:
                raise AssertionError('An empty live catalog must never become a static success')
            claude.MESSAGES_URL = endpoint('/messages/json')
            completion = await claude.generate_chat_completion(body, token, 'session-test')
            assert_completion(completion)
            assert received[-1][1] == converted
            # Use the existing saved-chat reasoning/tool reconstruction, then actual HTTP continuation.
            output_items = [{'type': 'reasoning', 'reasoning_details': completion['choices'][0]['message']['reasoning_details']}]
            output_items.extend({'type': 'function_call', 'call_id': call['id'], 'name': call['function']['name'],
                                 'arguments': call['function']['arguments'], 'status': 'completed'}
                                for call in completion['choices'][0]['message']['tool_calls'])
            output_items.extend([
                {'type': 'function_call_output', 'call_id': 'toolu_weather', 'output': [
                    {'type': 'input_text', 'text': 'Sunny'}, {'type': 'input_image', 'image_url': 'data:image/png;base64,aGVsbG8='}]},
                {'type': 'function_call_output', 'call_id': 'toolu_clock', 'output': [{'type': 'input_text', 'text': '12:00'}]},
            ])
            replay = convert_output_to_messages(output_items, raw=True)
            claude.MESSAGES_URL = endpoint('/messages/continued')
            continued = await claude.generate_chat_completion({**body, 'messages': [body['messages'][-1], *replay]}, token, 'session-test')
            assert continued['choices'][0]['message']['content'] == 'It is sunny.'
            assert continued['choices'][0]['finish_reason'] == 'stop'
            for case, streaming in (('stream', True), ('forced-stream', False), ('json', True), ('retry', False)):
                claude.MESSAGES_URL = endpoint('/messages/' + case)
                result = await claude.generate_chat_completion({**body, 'stream': streaming}, token, 'session-test')
                if streaming:
                    chunks = await output(result)
                    ids = [call['id'] for chunk in chunks for call in chunk['choices'][0]['delta'].get('tool_calls', []) if 'id' in call]
                    assert ids == ['toolu_weather', 'toolu_clock']
                    result = await claude._collect(as_async(chunks))
                assert_completion(result)
            assert counts['retry'] == 2
            for case in ('stream-error', 'truncated'):
                claude.MESSAGES_URL = endpoint('/messages/' + case)
                chunks = await output(await claude.generate_chat_completion({**body, 'stream': True}, token, 'session-test'))
                assert any('error' in chunk for chunk in chunks)
                assert 'test-only-secret' not in json.dumps(chunks)
                # Both failure paths occur after complete, valid client-tool arguments.
                ids = [call['id'] for chunk in chunks for choice in chunk.get('choices', [])
                       for call in choice.get('delta', {}).get('tool_calls', []) if 'id' in call]
                assert ids == ['toolu_weather', 'toolu_clock']
                arguments = ''.join(call.get('function', {}).get('arguments', '') for chunk in chunks
                                    for choice in chunk.get('choices', [])
                                    for call in choice.get('delta', {}).get('tool_calls', []) if call['index'] == 0)
                assert json.loads(arguments) == {'city': 'Jakarta', 'unit': '°C'}
                finishes = [choice['finish_reason'] for chunk in chunks for choice in chunk.get('choices', [])
                            if choice.get('finish_reason') is not None]
                assert finishes == ['failed']
                assert chunks[-1]['choices'][0]['finish_reason'] == 'failed'
                assert chunks[-1]['id'] == native['id']
                assert counts[case] == 1
            claude.MESSAGES_URL = endpoint('/messages/rejected')
            try:
                await claude.generate_chat_completion(body, token, 'session-test')
            except HTTPException as error:
                assert error.status_code == 401 and 'test-only-secret' not in error.detail
                assert counts['rejected'] == 1
            else:
                raise AssertionError('Inference must not refresh and repeat a rejected credential')
            claude.MESSAGES_URL = endpoint('/messages/cancel')
            response = await claude.generate_chat_completion({**body, 'stream': True}, token, 'session-test')
            first = await anext(response.body_iterator)
            assert 'assistant' in first
            task = asyncio.create_task(anext(response.body_iterator))
            await asyncio.sleep(0.01)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError('Stream cancellation must propagate')
            await response.body_iterator.aclose()
            await asyncio.wait_for(disconnected.wait(), timeout=3)
            assert counts['cancel'] == 1
            assert shared.cookie_jar.filter_cookies(jar_url)['provider_session'].value == 'other-user'
            assert 'stream_provider_session' not in shared.cookie_jar.filter_cookies(jar_url)
    finally:
        claude.OAUTH.clear()
        claude.OAUTH.update(original_oauth)
        claude.MESSAGES_URL = original_messages
        claude.MODELS_URL = original_models
        await close_session()


def test_claude_subscription():
    # Isolate Open WebUI import-time configuration from other tests and real data.
    subprocess.run([sys.executable, str(Path(__file__).resolve())], check=True)


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    with tempfile.TemporaryDirectory(prefix='open-webui-claude-subscription-') as directory:
        os.environ.update(
            PYTHON_DOTENV_DISABLED='1', FROM_INIT_PY='false', WEBUI_AUTH='true',
            WEBUI_SECRET_KEY=os.urandom(32).hex(), DATA_DIR=directory,
            STATIC_DIR=str(Path(directory) / 'static'), LICENSE_BLOB_PATH='',
            DATABASE_URL=f'sqlite:///{directory}/unused.db',
        )
        asyncio.run(_check())
