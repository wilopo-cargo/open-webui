"""Claude subscription OAuth and native Messages ↔ Chat Completions transport.

Protocol and translation adapted from 9router (https://github.com/decolua/9router).
MIT License
Copyright (c) 2024-2026 decolua and contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import re
import time
from urllib.parse import urlencode, urlsplit

import aiohttp
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from open_webui.utils.misc import get_reasoning_details
from open_webui.utils.session_pool import (
    can_retry_model_request,
    cleanup_response,
    get_client_timeout,
    get_provider_session,
    request_model,
    stream_wrapper,
)

PROVIDER_ID = 'claude'
OAUTH = {
    'authorize_url': 'https://claude.ai/oauth/authorize',
    'token_url': 'https://api.anthropic.com/v1/oauth/token',
    'client_id': '9d1c250a-e61b-44d9-88ed-5944d1962f5e',
    # 9router/cli/src/cli/api/client.js:225-230; copy the full redirected URL.
    'redirect_uri': 'http://localhost:20128/callback',
    'scope': 'org:create_api_key user:profile user:inference',
}
MESSAGES_URL = 'https://api.anthropic.com/v1/messages?beta=true'
MODELS_URL = 'https://api.anthropic.com/v1/models'
USAGE_URL = 'https://api.anthropic.com/api/oauth/usage?cedar_ember=1'
ANTHROPIC_VERSION = '2023-06-01'
ANTHROPIC_BETA = (
    'claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,'
    'effort-2025-11-24,structured-outputs-2025-12-15'
)
OAUTH_TIMEOUT = aiohttp.ClientTimeout(total=30)
MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_EVENT_BYTES = 4 * 1024 * 1024
MAX_CONTENT_BLOCKS = 1024


def _invalid(detail):
    raise HTTPException(status_code=400, detail=detail)


def _protocol_error():
    return HTTPException(status_code=502, detail='Claude returned an invalid or incomplete response.')


def _headers(token, stream=False):
    access_token = token.get('access_token') if isinstance(token, dict) else None
    if not isinstance(access_token, str) or not access_token:
        raise HTTPException(status_code=401, detail='Reconnect the Claude subscription connection.')
    return {
        'Authorization': f'Bearer {access_token}',
        'Anthropic-Version': ANTHROPIC_VERSION,
        'Anthropic-Beta': ANTHROPIC_BETA,
        'Accept': 'text/event-stream' if stream else 'application/json',
        'Content-Type': 'application/json',
    }


def _check_status(response, operation):
    if 200 <= response.status < 300:
        return
    status = response.status if 400 <= response.status < 600 else 502
    detail = f'Claude {operation} failed (HTTP {status}).'
    if status in (400, 401, 403) and operation in ('authorization', 'token refresh'):
        detail = 'Claude authorization expired or was rejected. Reconnect the connection.'
    elif status in (401, 403):
        detail = 'Claude rejected this connection. Reconnect or check subscription access.'
    retry_headers = {
        key: value for key, value in response.headers.items()
        if key.lower() in ('retry-after', 'retry-after-ms')
    }
    # Never echo an upstream body: it may contain credentials or submitted content.
    raise HTTPException(status_code=status, detail=detail, headers=retry_headers or None)


def _decode_json(raw, limit=MAX_JSON_BYTES):
    if len(raw) > limit:
        raise HTTPException(status_code=502, detail='Claude response exceeded the size limit.')
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError, RecursionError):
        raise _protocol_error() from None
    if not isinstance(value, dict):
        raise _protocol_error()
    return value


async def _read_json(response, limit=MAX_JSON_BYTES, stream=None):
    # request_model has already buffered non-streaming inference for retry2.
    if stream is None and response.content.at_eof():
        return _decode_json(await response.read(), limit)
    body = bytearray()
    if stream is None:
        stream = response.content.iter_chunked(65536)
    async for chunk in stream:
        if len(body) + len(chunk) > limit:
            raise HTTPException(status_code=502, detail='Claude response exceeded the size limit.')
        body.extend(chunk)
    return _decode_json(body, limit)


async def get_usage(token):
    from open_webui.utils.subscriptions.usage import claude_windows, fetch_usage

    headers = {**_headers(token), 'Anthropic-Beta': 'oauth-2025-04-20'}
    return claude_windows(await fetch_usage(USAGE_URL, headers))


def authorization_url(state: str, challenge: str) -> str:
    return OAUTH['authorize_url'] + '?' + urlencode({
        'code': 'true',
        'client_id': OAUTH['client_id'],
        'response_type': 'code',
        'redirect_uri': OAUTH['redirect_uri'],
        'scope': OAUTH['scope'],
        'code_challenge': challenge,
        'code_challenge_method': 'S256',
        'state': state,
    })


async def _token_request(payload, operation):
    response = None
    session = None
    try:
        session = await get_provider_session()
        response = await session.post(
            OAUTH['token_url'], json=payload,
            headers={'Accept': 'application/json'},
            timeout=OAUTH_TIMEOUT, ssl=True, allow_redirects=False,
        )
        _check_status(response, operation)
        result = await _read_json(response, 1024 * 1024)
        if not isinstance(result.get('access_token'), str) or not result['access_token']:
            raise _protocol_error()
        return result
    except (aiohttp.ClientError, asyncio.TimeoutError):
        raise HTTPException(status_code=502, detail=f'Unable to reach Claude for {operation}.') from None
    finally:
        await cleanup_response(response, session)


async def exchange_code(code: str, state: str, verifier: str) -> dict:
    if not all(isinstance(value, str) for value in (code, state, verifier)):
        _invalid('Claude authorization code, state and PKCE verifier must be strings.')
    if '#' in code:
        code, returned_state = code.split('#', 1)
        if not hmac.compare_digest(returned_state.encode(), state.encode()):
            _invalid('Claude callback state does not match the authorization request.')
    if not code or not state or not verifier:
        _invalid('Claude authorization code, state and PKCE verifier are required.')
    return await _token_request({
        'grant_type': 'authorization_code',
        'client_id': OAUTH['client_id'],
        'code': code,
        'state': state,
        'redirect_uri': OAUTH['redirect_uri'],
        'code_verifier': verifier,
    }, 'authorization')


async def refresh_token(refresh_token: str) -> dict:
    if not isinstance(refresh_token, str) or not refresh_token:
        raise HTTPException(status_code=401, detail='Reconnect the Claude subscription connection.')
    return await _token_request({
        'grant_type': 'refresh_token',
        'client_id': OAUTH['client_id'],
        'refresh_token': refresh_token,
    }, 'token refresh')


async def list_models(token: dict) -> list[dict]:
    models = []
    seen = set()
    after = None
    session = None
    response = None
    try:
        session = await get_provider_session()
        for _ in range(10):
            response = None
            try:
                params = {'limit': 1000}
                if after:
                    params['after_id'] = after
                response = await session.get(
                    MODELS_URL, params=params, headers=_headers(token),
                    timeout=OAUTH_TIMEOUT, ssl=True, allow_redirects=False,
                )
                _check_status(response, 'model discovery')
                page = await _read_json(response, MAX_EVENT_BYTES)
                entries = page.get('data')
                if not isinstance(entries, list) or len(entries) > 1000:
                    raise _protocol_error()
                for entry in entries:
                    if not isinstance(entry, dict) or not isinstance(entry.get('id'), str) or not entry['id']:
                        raise _protocol_error()
                    if entry['id'] not in seen:
                        models.append({'id': entry['id'], 'name': entry.get('display_name') or entry['id']})
                        seen.add(entry['id'])
                if not page.get('has_more'):
                    if not models:
                        raise HTTPException(status_code=502, detail='Claude returned no available subscription models.')
                    return models
                cursor = page.get('last_id')
                if not isinstance(cursor, str) or not cursor or cursor == after or not entries:
                    raise _protocol_error()
                after = cursor
            finally:
                await cleanup_response(response)
        raise HTTPException(status_code=502, detail='Claude model catalog exceeded the pagination limit.')
    except (aiohttp.ClientError, asyncio.TimeoutError):
        raise HTTPException(status_code=502, detail='Unable to reach Claude for model discovery.') from None
    finally:
        await cleanup_response(response, session)


def _cache(source, block):
    if source.get('cache_control') is not None:
        control = source['cache_control']
        if not isinstance(control, dict) or control.get('type') != 'ephemeral' or control.get('ttl', '5m') not in ('5m', '1h'):
            _invalid('Claude cache_control must be ephemeral with a 5m or 1h TTL.')
        block['cache_control'] = dict(control)
    return block


def _source(url, media_types):
    if not isinstance(url, str):
        _invalid('Claude image or document input requires a URL or base64 data URI.')
    if url.startswith('data:'):
        match = re.fullmatch(r'data:([^;,]+);base64,([A-Za-z0-9+/=\r\n]+)', url)
        if not match or match[1].lower() not in media_types:
            _invalid('Claude received an unsupported image or document data URI.')
        data = match[2].replace('\r', '').replace('\n', '')
        if len(data) > MAX_JSON_BYTES:
            _invalid('Claude image or document input exceeded the size limit.')
        try:
            base64.b64decode(data, validate=True)
        except ValueError:
            _invalid('Claude image or document base64 data is invalid.')
        return {'type': 'base64', 'media_type': match[1].lower(), 'data': data}
    try:
        parsed = urlsplit(url)
        valid = parsed.scheme in ('https', 'http') and parsed.hostname and not parsed.username and not parsed.password
    except ValueError:
        valid = False
    if not valid:
        _invalid('Claude image or document URLs must use HTTP or HTTPS without credentials.')
    return {'type': 'url', 'url': url}


def _required_string(value, detail):
    if not isinstance(value, str) or not value:
        _invalid(detail)
    return value


def _tool_use(block):
    if not isinstance(block.get('input', {}), dict):
        _invalid('Claude tool call arguments must be a JSON object.')
    return _cache(block, {
        'type': 'tool_use',
        'id': _required_string(block.get('id'), 'Claude tool calls require an ID.'),
        'name': _required_string(block.get('name'), 'Claude tool calls require a name.'),
        'input': block.get('input', {}),
    })


def _content(content, role, *, result=False):
    if content is None:
        return []
    if isinstance(content, str):
        return [{'type': 'text', 'text': content}] if content else []
    if not isinstance(content, list) or len(content) > MAX_CONTENT_BLOCKS:
        _invalid('Claude message content must be text or a bounded content array.')
    blocks = []
    for part in content:
        if not isinstance(part, dict):
            _invalid('Claude message content blocks must be objects.')
        kind = part.get('type')
        if kind in ('text', 'input_text', 'output_text'):
            if not isinstance(part.get('text'), str):
                _invalid('Claude text blocks require text.')
            if part['text']:
                blocks.append(_cache(part, {'type': 'text', 'text': part['text']}))
        elif kind in ('image_url', 'input_image', 'image') and role == 'user':
            if kind in ('image_url', 'input_image'):
                image = part.get('image_url')
                if isinstance(image, dict) and image.get('detail', 'auto') != 'auto':
                    _invalid('Claude image input does not support OpenAI detail overrides.')
                source = _source(image.get('url') if isinstance(image, dict) else image,
                                 {'image/jpeg', 'image/png', 'image/gif', 'image/webp'})
            else:
                native = part.get('source') or {}
                if not isinstance(native, dict):
                    _invalid('Claude image sources must be objects.')
                if native.get('type') == 'base64':
                    source = _source(f"data:{native.get('media_type')};base64,{native.get('data')}",
                                     {'image/jpeg', 'image/png', 'image/gif', 'image/webp'})
                else:
                    source = _source(native.get('url'), {'image/jpeg', 'image/png', 'image/gif', 'image/webp'})
            blocks.append(_cache(part, {'type': 'image', 'source': source}))
        elif kind == 'file' and role == 'user':
            file = part.get('file') or {}
            if not isinstance(file, dict):
                _invalid('Claude file input requires a base64 PDF object.')
            source = _source(file.get('file_data'), {'application/pdf'})
            if source['type'] != 'base64':
                _invalid('Claude file input requires a base64 PDF, not an uploaded file ID.')
            blocks.append(_cache(part, {'type': 'document', 'source': source}))
        elif kind == 'tool_use' and role == 'assistant' and not result:
            blocks.append(_tool_use(part))
        elif kind == 'tool_result' and role == 'user' and not result:
            blocks.append(_cache(part, {
                'type': 'tool_result',
                'tool_use_id': _required_string(part.get('tool_use_id'), 'Claude tool results require a matching call ID.'),
                'content': _content(part.get('content'), 'user', result=True),
                'is_error': part.get('is_error') is True,
            }))
        elif kind == 'thinking' and role == 'assistant' and not result:
            if not isinstance(part.get('thinking'), str) or not isinstance(part.get('signature'), str) or not part['signature']:
                _invalid('Claude thinking history must retain its provider signature.')
            blocks.append({'type': 'thinking', 'thinking': part['thinking'], 'signature': part['signature']})
        elif kind == 'redacted_thinking' and role == 'assistant' and not result:
            blocks.append({'type': kind, 'data': _required_string(part.get('data'), 'Claude redacted thinking requires data.')})
        else:
            _invalid('Claude does not support one or more supplied content block types.')
    return blocks


def _reasoning_history(message):
    details = get_reasoning_details(message) or []
    if isinstance(details, dict):
        details = [details]
    if not isinstance(details, list):
        _invalid('Claude reasoning history must be a list of signed provider details.')
    blocks = []
    for detail in details:
        if not isinstance(detail, dict):
            _invalid('Claude reasoning history contains an invalid provider detail.')
        if detail.get('format') == 'anthropic-claude-v1' and detail.get('type') == 'reasoning.text':
            blocks.extend(_content([{
                'type': 'thinking', 'thinking': detail.get('text', ''), 'signature': detail.get('signature'),
            }], 'assistant'))
        elif detail.get('format') == 'anthropic-claude-v1-redacted' and detail.get('type') == 'reasoning.encrypted':
            blocks.extend(_content([{'type': 'redacted_thinking', 'data': detail.get('data')}], 'assistant'))
        else:
            _invalid('Claude cannot replay reasoning details from a different provider.')
        for key in ('claude_prefix', 'claude_content'):
            if key in detail:
                blocks[-1]['_' + key] = detail[key]
    # Bare reasoning_content has no Claude signature. It is display-only, not replayable.
    return blocks


def _messages(body):
    messages = body.get('messages')
    if not isinstance(messages, list) or not messages:
        _invalid('Claude requires a nonempty messages array.')
    system = _content(body.get('system'), 'system')
    converted = []
    for message in messages:
        if not isinstance(message, dict):
            _invalid('Claude messages must be objects.')
        if message.get('function_call') is not None or message.get('audio') is not None:
            _invalid('Claude does not support legacy function_call or audio messages.')
        role = message.get('role')
        if role in ('system', 'developer'):
            blocks = _content(message.get('content'), 'system')
            if converted and _mid_system_model(body.get('model', '')):
                if converted[-1]['role'] == 'system':
                    converted[-1]['content'].extend(blocks)
                elif blocks:
                    converted.append({'role': 'system', 'content': blocks})
            else:
                system.extend(blocks)
            continue
        if role == 'tool':
            blocks = [_cache(message, {
                'type': 'tool_result',
                'tool_use_id': _required_string(message.get('tool_call_id'), 'Claude tool results require a matching call ID.'),
                'content': _content(message.get('content'), 'user', result=True),
                'is_error': message.get('is_error') is True or message.get('status') == 'error',
            })]
            role = 'user'
        elif role in ('user', 'assistant'):
            blocks = _content(message.get('content'), role)
            if role == 'assistant':
                history = _reasoning_history(message)
                if history and any(block['type'] in ('thinking', 'redacted_thinking') for block in blocks):
                    _invalid('Claude thinking history was supplied twice.')
                blocks = history + blocks
                calls = message.get('tool_calls', [])
                if calls is None:
                    calls = []
                if not isinstance(calls, list):
                    _invalid('Claude tool_calls must be an array.')
                for call in calls:
                    if not isinstance(call, dict) or call.get('type', 'function') != 'function':
                        _invalid('Claude supports function tool calls only.')
                    function = call.get('function') or {}
                    if not isinstance(function, dict):
                        _invalid('Claude function calls require a function object.')
                    arguments = function.get('arguments', '{}')
                    if isinstance(arguments, str):
                        if len(arguments) > MAX_EVENT_BYTES:
                            _invalid('Claude tool call arguments exceeded the size limit.')
                        try:
                            arguments = json.loads(arguments)
                        except (ValueError, RecursionError):
                            _invalid('Claude tool call arguments must be valid JSON.')
                    blocks.append(_tool_use({'id': call.get('id'), 'name': function.get('name'), 'input': arguments}))
        else:
            _invalid('Claude does not support the supplied message role.')
        if blocks:
            if converted and converted[-1]['role'] == role:
                converted[-1]['content'].extend(blocks)
            else:
                converted.append({'role': role, 'content': blocks})
    if not converted:
        _invalid('Claude requires at least one user or assistant message.')
    pending = set()
    for index, message in enumerate(converted):
        blocks = message['content']
        if len(blocks) > MAX_CONTENT_BLOCKS:
            _invalid('Claude message content exceeded the block limit.')
        if message['role'] == 'assistant':
            if pending:
                _invalid('Claude tool history is missing a tool result.')
            ids = [block['id'] for block in blocks if block['type'] == 'tool_use']
            if len(ids) != len(set(ids)):
                _invalid('Claude tool call IDs must be unique within a turn.')
            pending = set(ids)
        elif message['role'] == 'system':
            if pending or index == 0 or converted[index - 1]['role'] != 'user' or (
                index + 1 < len(converted) and converted[index + 1]['role'] == 'user'
            ):
                _invalid('Claude late system instructions must follow a user/tool-result turn and precede the answer.')
        else:
            results = [block for block in blocks if block['type'] == 'tool_result']
            ids = [block['tool_use_id'] for block in results]
            if len(ids) != len(set(ids)) or set(ids) != pending:
                _invalid('Claude tool results must match the preceding assistant tool calls.')
            pending.clear()
            # Consecutive tool messages form one user turn, results before any new text.
            message['content'] = results + [block for block in blocks if block['type'] != 'tool_result']
    if pending:
        _invalid('Claude tool history is missing a tool result.')
    return system, converted


def _preserved_thinking(model):
    return bool(re.match(
        r'^claude-(?:(?:opus|sonnet|haiku)-5[-.]5|(?:fable|mythos)-5[-.][12])(?:-|$)', model
    ))


def _mid_system_model(model):
    return _preserved_thinking(model) or bool(re.match(
        r'^claude-(?:opus-(?:4[-.]8|5)|(?:fable|mythos)-5)(?:-|$)', model
    ))


def _prefix_value(block):
    # Ignore cache markers on content blocks, never a schema/input property named cache_control.
    value = {key: item for key, item in block.items()
             if key != 'cache_control' and not key.startswith('_claude_')}
    if isinstance(value.get('content'), list):
        value['content'] = [_prefix_value(item) for item in value['content']]
    return value


def _prefix_hash(payload, messages=None):
    messages = payload['messages'] if messages is None else messages
    prefix = {
        'system': [_prefix_value(block) for block in payload.get('system', [])],
        'tools': [{key: value for key, value in tool.items() if key != 'cache_control'}
                  for tool in payload.get('tools', [])],
        'messages': [{'role': message['role'], 'content': [_prefix_value(block) for block in message['content']]}
                     for message in messages],
    }
    return hashlib.sha256(json.dumps(
        prefix, sort_keys=True, separators=(',', ':'), ensure_ascii=False
    ).encode()).hexdigest()


def _content_surface(blocks):
    return (
        ''.join(block.get('text', '') for block in blocks if block.get('type') == 'text'),
        [_prefix_value(block) for block in blocks if block.get('type') == 'tool_use'],
        [_prefix_value(block) for block in blocks if block.get('type') in ('thinking', 'redacted_thinking')],
    )


def _replay_bound_history(payload):
    messages = payload['messages']
    last_assistant = max((index for index, message in enumerate(messages) if message['role'] == 'assistant'), default=-1)
    restored = []
    preserved = _preserved_thinking(payload['model'])
    for index, message in enumerate(messages):
        blocks = message['content']
        thinking = [block for block in blocks if block['type'] in ('thinking', 'redacted_thinking')]
        if thinking:
            metadata = next((block for block in thinking if '_claude_content' in block), {})
            snapshot = metadata.get('_claude_content')
            unchanged = False
            if isinstance(snapshot, list):
                original = _content(snapshot, 'assistant')
                unchanged = _content_surface(original) == _content_surface(blocks)
                if unchanged:
                    blocks = original
            matches = unchanged and metadata.get('_claude_prefix') == _prefix_hash(payload, restored)
            if preserved and not matches:
                if index == last_assistant and any(block['type'] == 'tool_use' for block in blocks):
                    _invalid(
                        'Claude cannot continue this open tool turn because its system, tools, or earlier history changed. '
                        'Restore that turn’s original context or start a new conversation.'
                    )
                # A changed completed prefix invalidates this reasoning and any later bound chain.
                # Newly generated reasoning fingerprints the resulting history, so it remains replayable.
                blocks = [block for block in blocks if block['type'] not in ('thinking', 'redacted_thinking')]
        blocks = [{key: value for key, value in block.items() if not key.startswith('_claude_')} for block in blocks]
        if blocks:
            restored.append({'role': message['role'], 'content': blocks})
    payload['messages'] = restored


def _reasoning_binding(payload, content):
    return {'claude_prefix': _prefix_hash(payload), 'claude_content': content}


def _adaptive_thinking(model):
    return bool(re.match(
        r'^claude-(?:(?:opus|sonnet)-4[-.][678](?:-|$)|(?:opus|sonnet|fable|mythos)-5(?:[-.]|$)|'
        r'haiku-5[-.]5(?:-|$))', model
    ))


def _output_ceiling(model):
    # 9router capabilities: budget families default to 64k; these families allow 128k.
    return 128000 if _adaptive_thinking(model) or re.match(r'^claude-(?:fable|mythos)-', model) else 64000


def _thinking(body, payload, explicit_max):
    thinking = body.get('thinking')
    model = payload['model']
    generated_budget = thinking is None
    always_on = bool(re.match(r'^claude-(?:opus-5[-.]5|(?:fable|mythos)-5(?:[-.][12])?)(?:-|$)', model))
    between_tools = bool(re.match(r'^claude-sonnet-5[-.]5(?:-|$)', model))
    no_xhigh = bool(re.match(r'^claude-(?:opus|sonnet)-4[-.]6(?:-|$)', model))
    forced_unsupported = bool(re.match(r'^claude-(?:opus|sonnet)-5[-.]5(?:-|$)', model))
    if forced_unsupported and payload.get('tool_choice', {}).get('type') in ('any', 'tool'):
        _invalid('This Claude model does not support a forced tool choice.')
    reasoning = body.get('reasoning') or {}
    if not isinstance(reasoning, dict) or set(reasoning) - {'effort', 'summary'}:
        _invalid('Claude does not support the supplied reasoning configuration.')
    effort = body.get('reasoning_effort', reasoning.get('effort'))
    if effort is not None and not isinstance(effort, str):
        _invalid('Claude reasoning_effort must be a string.')
    if thinking is not None and effort is not None:
        _invalid('Specify either Claude thinking or reasoning_effort, not both.')
    if thinking is not None:
        if not isinstance(thinking, dict) or set(thinking) - {'type', 'budget_tokens', 'display'}:
            _invalid('Claude thinking configuration is invalid.')
        thinking = dict(thinking)
    elif effort is not None:
        budgets = {'minimal': 1024, 'low': 1024, 'medium': 8192, 'high': 24576, 'xhigh': 32768, 'max': 128000}
        adaptive = _adaptive_thinking(model)
        if effort in ('none', 'off'):
            thinking = {'type': 'disabled'}
        elif effort in budgets or effort == 'auto':
            if adaptive:
                thinking = {'type': 'adaptive'}
                payload.setdefault('output_config', {})['effort'] = 'high' if effort == 'auto' else 'low' if effort == 'minimal' else effort
            else:
                thinking = {'type': 'enabled', 'budget_tokens': budgets.get(effort, 8192)}
        else:
            _invalid('Claude reasoning_effort is not supported.')
    config = payload.get('output_config', {})
    if config.get('effort') is not None:
        if config['effort'] not in ('low', 'medium', 'high', 'xhigh', 'max'):
            _invalid('Claude output_config.effort is not supported.')
        if no_xhigh and config['effort'] == 'xhigh':
            config['effort'] = 'high'
    if thinking is None:
        if reasoning:
            _invalid('Claude reasoning summary requires an effort or thinking configuration.')
        return
    if thinking.get('type') not in ('enabled', 'adaptive', 'disabled', 'between_tools'):
        _invalid('Claude thinking.type is invalid.')
    if 'budget_tokens' in thinking and thinking['type'] != 'enabled':
        _invalid('Claude budget_tokens is only supported for enabled thinking.')
    if thinking['type'] == 'enabled' and (
        _preserved_thinking(model) or re.match(
            r'^claude-(?:opus-(?:4[-.][78]|5)|sonnet-5|(?:fable|mythos)-5)(?:[-.]|$)', model
        )
    ):
        _invalid('This Claude model requires adaptive thinking instead of a fixed thinking budget.')
    if thinking['type'] == 'disabled' and always_on:
        # Always-on models cannot disable reasoning: select the lowest effort and omit its display.
        thinking = {'type': 'adaptive', 'display': 'omitted'}
        payload.setdefault('output_config', {})['effort'] = 'low'
    elif thinking['type'] == 'disabled' and between_tools:
        thinking = {'type': 'between_tools'}
    if thinking['type'] == 'between_tools':
        if not between_tools:
            _invalid('This Claude model does not support between_tools thinking.')
        if payload.get('output_config', {}).get('effort') in ('xhigh', 'max'):
            payload['output_config']['effort'] = 'high'
    summary = reasoning.get('summary')
    if summary is not None:
        if summary not in ('auto', 'concise', 'detailed', 'none'):
            _invalid('Claude reasoning summary is invalid.')
        if thinking['type'] in ('enabled', 'adaptive'):
            thinking.setdefault('display', 'omitted' if summary == 'none' else 'summarized')
    elif effort not in (None, 'none', 'off') and thinking['type'] in ('enabled', 'adaptive'):
        thinking.setdefault('display', 'summarized')
    if thinking.get('display', 'summarized') not in ('summarized', 'omitted'):
        _invalid('Claude thinking.display must be summarized or omitted.')
    if thinking['type'] == 'enabled':
        budget = thinking.get('budget_tokens')
        if not isinstance(budget, int) or isinstance(budget, bool) or budget < 1024:
            _invalid('Claude enabled thinking requires budget_tokens of at least 1024.')
        if not explicit_max:
            payload['max_tokens'] = min(max(payload['max_tokens'], budget + 1024), _output_ceiling(model))
        if generated_budget:
            budget = min(budget, max(1024, payload['max_tokens'] - 1024))
            thinking['budget_tokens'] = budget
        if budget >= payload['max_tokens']:
            _invalid('Claude max_tokens must be greater than the thinking budget.')
    elif 'budget_tokens' in thinking:
        _invalid('Claude budget_tokens is only supported for enabled thinking.')
    if thinking['type'] not in ('disabled', 'between_tools'):
        if 'temperature' in payload and payload['temperature'] != 1:
            _invalid('Claude thinking requires temperature=1 or an omitted temperature.')
        if payload.get('tool_choice', {}).get('type') in ('any', 'tool'):
            _invalid('Claude thinking does not support a forced tool choice.')
    payload['thinking'] = thinking


def _payload(body):
    supported = {
        'model', 'messages', 'system', 'stream', 'max_tokens', 'max_completion_tokens',
        'temperature', 'top_p', 'top_k', 'stop', 'stop_sequences', 'tools', 'tool_choice',
        'parallel_tool_calls', 'thinking', 'reasoning', 'reasoning_effort', 'output_config',
        'response_format', 'service_tier', 'metadata', 'user', 'stream_options',
        'n', 'frequency_penalty', 'presence_penalty', 'logprobs',
    }
    if any(key not in supported and value is not None for key, value in body.items()):
        _invalid('Claude does not support one or more requested parameters.')
    for key, neutral in (('n', 1), ('frequency_penalty', 0), ('presence_penalty', 0), ('logprobs', False)):
        if body.get(key) is not None and body[key] != neutral:
            _invalid('Claude does not support the requested sampling or logprob parameter.')
    model = _required_string(body.get('model'), 'Claude requires a model ID.')
    max_tokens = body.get('max_completion_tokens', body.get('max_tokens', 4096))
    if not isinstance(max_tokens, int) or isinstance(max_tokens, bool) or max_tokens < 1:
        _invalid('Claude max_tokens must be a positive integer.')
    max_tokens = min(max_tokens, _output_ceiling(model))
    if body.get('stream') is not None and not isinstance(body['stream'], bool):
        _invalid('Claude stream must be a boolean.')
    system, messages = _messages(body)
    payload = {'model': model, 'messages': messages, 'max_tokens': max_tokens, 'stream': bool(body.get('stream', False))}
    if system:
        payload['system'] = system
    for key in ('temperature', 'top_p', 'top_k', 'service_tier'):
        if body.get(key) is not None:
            payload[key] = body[key]
    stop = body.get('stop', body.get('stop_sequences'))
    if stop is not None:
        stop = [stop] if isinstance(stop, str) else stop
        if not isinstance(stop, list) or any(not isinstance(value, str) for value in stop):
            _invalid('Claude stop sequences must be strings.')
        payload['stop_sequences'] = stop
    tools = body.get('tools', [])
    if tools is None:
        tools = []
    if not isinstance(tools, list):
        _invalid('Claude tools must be an array of function declarations.')
    if tools:
        payload['tools'] = []
        for tool in tools:
            if not isinstance(tool, dict) or tool.get('type', 'function') != 'function':
                _invalid('Claude supports WebUI function tools, not provider-executed tools or MCP servers.')
            function = tool.get('function') or {}
            if not isinstance(function, dict):
                _invalid('Claude tools require a function object.')
            schema = function.get('parameters', {'type': 'object', 'properties': {}})
            if not isinstance(schema, dict):
                _invalid('Claude tool parameters must be a JSON schema object.')
            converted = {
                'name': _required_string(function.get('name'), 'Claude tools require a function name.'),
                'input_schema': schema,
            }
            for key in ('description', 'strict'):
                if key in function:
                    converted[key] = function[key]
            payload['tools'].append(_cache(tool, converted))
    choice = body.get('tool_choice')
    if choice is not None:
        if isinstance(choice, str) and choice in ('auto', 'none', 'required'):
            converted_choice = {'type': 'any' if choice == 'required' else choice}
        elif isinstance(choice, dict) and choice.get('type') == 'function':
            function = choice.get('function') or {}
            if not isinstance(function, dict):
                _invalid('Claude forced tools require a function object.')
            converted_choice = {'type': 'tool', 'name': _required_string(function.get('name'), 'Claude forced tools require a name.')}
        else:
            _invalid('Claude tool_choice must be auto, none, required, or a named function.')
        if not tools and converted_choice['type'] not in ('none', 'auto'):
            _invalid('Claude tool_choice requires function declarations.')
        if tools or converted_choice['type'] == 'none':
            payload['tool_choice'] = converted_choice
    if tools and body.get('parallel_tool_calls') is not None:
        if not isinstance(body['parallel_tool_calls'], bool):
            _invalid('Claude parallel_tool_calls must be a boolean.')
        if payload.get('tool_choice', {}).get('type') != 'none':
            payload.setdefault('tool_choice', {'type': 'auto'})['disable_parallel_tool_use'] = not body['parallel_tool_calls']
    output_config = body.get('output_config')
    if output_config is not None:
        if not isinstance(output_config, dict) or set(output_config) - {'effort', 'format'}:
            _invalid('Claude output_config is invalid.')
        payload['output_config'] = dict(output_config)
    response_format = body.get('response_format')
    if response_format is not None:
        if not isinstance(response_format, dict):
            _invalid('Claude response_format is invalid.')
        kind = response_format.get('type')
        if kind == 'json_schema':
            schema = response_format.get('json_schema') or {}
            if not isinstance(schema, dict) or not isinstance(schema.get('schema'), dict):
                _invalid('Claude structured output requires a JSON schema.')
            payload.setdefault('output_config', {})['format'] = {'type': 'json_schema', 'schema': schema['schema']}
        elif kind != 'text':
            _invalid('Claude supports response_format text or json_schema, not unconstrained json_object mode.')
    _thinking(body, payload, 'max_tokens' in body or 'max_completion_tokens' in body)
    _replay_bound_history(payload)
    return payload


def _usage(raw):
    if not isinstance(raw, dict) or 'input_tokens' not in raw or 'output_tokens' not in raw:
        raise _protocol_error()
    def count(name):
        value = raw.get(name, 0)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise _protocol_error()
        return value
    input_tokens = count('input_tokens')
    output_tokens = count('output_tokens')
    cached = count('cache_read_input_tokens')
    created = count('cache_creation_input_tokens')
    prompt = input_tokens + cached + created
    usage = {
        'prompt_tokens': prompt, 'completion_tokens': output_tokens, 'total_tokens': prompt + output_tokens,
        'input_tokens': input_tokens, 'output_tokens': output_tokens,
    }
    for key in ('cache_read_input_tokens', 'cache_creation_input_tokens', 'cache_creation'):
        if key in raw:
            usage[key] = raw[key]
    if cached or created:
        usage['prompt_tokens_details'] = {'cached_tokens': cached, 'cache_creation_tokens': created}
    if 'output_tokens_details' in raw:
        usage['completion_tokens_details'] = raw['output_tokens_details']
    return usage


def _finish(reason):
    reasons = {
        'end_turn': 'stop', 'stop_sequence': 'stop', 'tool_use': 'tool_calls', 'max_tokens': 'length',
        'model_context_window_exceeded': 'length', 'refusal': 'content_filter', 'pause_turn': 'stop',
    }
    if not isinstance(reason, str) or reason not in reasons:
        raise _protocol_error()
    return reasons[reason]


def _detail(block, index):
    if block['type'] == 'redacted_thinking':
        return {'type': 'reasoning.encrypted', 'format': 'anthropic-claude-v1-redacted', 'index': index, 'data': block['data']}
    detail = {'type': 'reasoning.text', 'format': 'anthropic-claude-v1', 'index': index, 'text': block.get('thinking', '')}
    if block.get('signature'):
        detail['signature'] = block['signature']
        if not block.get('thinking'):
            # The existing stream consumer retains opaque data even with omitted text.
            detail['data'] = block['signature']
    return detail


def _completion(message, model, payload=None):
    if not isinstance(message.get('id'), str) or not isinstance(message.get('content'), list):
        raise _protocol_error()
    if len(message['content']) > MAX_CONTENT_BLOCKS:
        raise _protocol_error()
    result = {'role': 'assistant', 'content': ''}
    text, reasoning, details, calls = [], [], [], []
    for index, block in enumerate(message['content']):
        if not isinstance(block, dict):
            raise _protocol_error()
        kind = block.get('type')
        if kind == 'text' and isinstance(block.get('text'), str):
            text.append(block['text'])
        elif kind == 'thinking' and isinstance(block.get('thinking'), str):
            reasoning.append(block['thinking'])
            details.append(_detail(block, index))
        elif kind == 'redacted_thinking' and isinstance(block.get('data'), str):
            details.append(_detail(block, index))
        elif kind == 'tool_use' and isinstance(block.get('input'), dict) and isinstance(block.get('id'), str) and isinstance(block.get('name'), str):
            calls.append({'id': block['id'], 'type': 'function', 'function': {
                'name': block['name'], 'arguments': json.dumps(block['input'], separators=(',', ':'), ensure_ascii=False),
            }})
        else:
            raise _protocol_error()
    result['content'] = ''.join(text)
    if reasoning:
        result['reasoning_content'] = ''.join(reasoning)
    if details:
        if payload is not None:
            details[0].update(_reasoning_binding(payload, message['content']))
        result['reasoning_details'] = details
    if calls:
        result['tool_calls'] = calls
    if message.get('stop_reason') == 'refusal':
        stop_details = message.get('stop_details') or {}
        if not isinstance(stop_details, dict):
            raise _protocol_error()
        explanation = stop_details.get('explanation')
        if explanation:
            result['refusal'] = explanation
            if not isinstance(explanation, str):
                raise _protocol_error()
            if not result['content'].strip():
                result['content'] = explanation
    return {
        'id': message['id'], 'object': 'chat.completion', 'created': int(time.time()),
        'model': message.get('model') or model,
        'choices': [{'index': 0, 'message': result, 'finish_reason': _finish(message.get('stop_reason')),
                     'stop_reason': message.get('stop_reason'), 'stop_sequence': message.get('stop_sequence')}],
        'usage': _usage(message.get('usage') or {}),
    }


async def _events(stream):
    buffer = bytearray()
    data = []
    event_bytes = 0
    first_line = True
    async for chunk in stream:
        buffer.extend(chunk)
        while True:
            end = buffer.find(b'\n')
            if end < 0:
                if len(buffer) + event_bytes > MAX_EVENT_BYTES:
                    raise _protocol_error()
                break
            line = bytes(buffer[:end]).rstrip(b'\r')
            del buffer[:end + 1]
            if first_line:
                line = line.removeprefix(b'\xef\xbb\xbf')
                first_line = False
            event_bytes += len(line)
            if event_bytes > MAX_EVENT_BYTES:
                raise _protocol_error()
            if not line:
                if data:
                    yield _decode_json(b'\n'.join(data), MAX_EVENT_BYTES)
                data = []
                event_bytes = 0
            elif line.startswith(b'data:'):
                value = line[5:]
                data.append(value[1:] if value.startswith(b' ') else value)
    if buffer.strip() or data:
        raise _protocol_error()


async def _chat_chunks(events, model, payload=None):
    identity = None
    blocks = {}
    tool_index = 0
    raw_usage = {}
    stop_reason = None
    stop_sequence = None
    has_text = False
    created = int(time.time())
    native_blocks = {}
    native_bytes = 0

    def chunk(delta, finish=None):
        return {'id': identity['id'], 'object': 'chat.completion.chunk', 'created': created,
                'model': identity.get('model') or model,
                'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]}

    async for event in events:
        if payload is not None:
            native_bytes += len(json.dumps(event, ensure_ascii=False).encode())
            if native_bytes > MAX_JSON_BYTES:
                raise HTTPException(status_code=502, detail='Claude response exceeded the size limit.')
        kind = event.get('type')
        if kind == 'ping':
            continue
        if kind == 'error':
            error = event.get('error') or {}
            if not isinstance(error, dict):
                raise _protocol_error()
            status = {'overloaded_error': 503, 'rate_limit_error': 429, 'authentication_error': 401,
                      'permission_error': 403}.get(error.get('type'), 502)
            raise HTTPException(status_code=status, detail='Claude interrupted the response. Try again or reconnect if access expired.')
        if kind == 'message_start':
            message = event.get('message')
            if identity is not None or not isinstance(message, dict) or not isinstance(message.get('id'), str):
                raise _protocol_error()
            if not isinstance(message.get('usage', {}), dict):
                raise _protocol_error()
            identity = {'id': message['id'], 'model': message.get('model')}
            raw_usage.update(message.get('usage') or {})
            yield chunk({'role': 'assistant', 'content': ''})
            continue
        if identity is None:
            raise _protocol_error()
        if kind == 'content_block_start':
            index = event.get('index')
            block = event.get('content_block')
            if not isinstance(index, int) or index < 0 or index in blocks or len(blocks) >= MAX_CONTENT_BLOCKS or not isinstance(block, dict):
                raise _protocol_error()
            block_type = block.get('type')
            state = {'type': block_type, 'bytes': 0}
            blocks[index] = state
            if payload is not None:
                state['native'] = dict(block)
                state['parts'] = [block.get('text', block.get('thinking', ''))]
                native_blocks[index] = state['native']
            if block_type == 'tool_use' and isinstance(block.get('input'), dict) and isinstance(block.get('id'), str) and isinstance(block.get('name'), str):
                state['tool_index'] = tool_index
                state['initial_input'] = bool(block['input'])
                state['arguments_started'] = bool(block['input'])
                tool_index += 1
                arguments = json.dumps(block['input'], separators=(',', ':'), ensure_ascii=False) if block['input'] else ''
                yield chunk({'tool_calls': [{'index': state['tool_index'], 'id': block['id'], 'type': 'function',
                                             'function': {'name': block['name'], 'arguments': arguments}}]})
            elif block_type == 'text' and isinstance(block.get('text'), str):
                if block['text']:
                    has_text = has_text or bool(block['text'].strip())
                    yield chunk({'content': block['text']})
            elif block_type == 'thinking' and isinstance(block.get('thinking'), str):
                signature = block.get('signature') or ''
                if not isinstance(signature, str):
                    raise _protocol_error()
                state['signatures'] = [signature]
                state['has_text'] = bool(block['thinking'])
                delta = {'reasoning_details': [_detail(block, index)]}
                if block['thinking']:
                    delta['reasoning_content'] = block['thinking']
                yield chunk(delta)
            elif block_type == 'redacted_thinking' and isinstance(block.get('data'), str):
                yield chunk({'reasoning_details': [_detail(block, index)]})
            else:
                raise _protocol_error()
        elif kind == 'content_block_delta':
            block = blocks.get(event.get('index'))
            delta = event.get('delta')
            if not block or block.get('closed') or not isinstance(delta, dict):
                raise _protocol_error()
            delta_type = delta.get('type')
            if delta_type == 'text_delta' and block['type'] == 'text' and isinstance(delta.get('text'), str):
                if 'native' in block:
                    block['parts'].append(delta['text'])
                has_text = has_text or bool(delta['text'].strip())
                yield chunk({'content': delta['text']})
            elif delta_type == 'thinking_delta' and block['type'] == 'thinking' and isinstance(delta.get('thinking'), str):
                if 'native' in block:
                    block['parts'].append(delta['thinking'])
                block['has_text'] = block['has_text'] or bool(delta['thinking'])
                detail = _detail({'type': 'thinking', 'thinking': delta['thinking']}, event['index'])
                yield chunk({'reasoning_content': delta['thinking'], 'reasoning_details': [detail]})
            elif delta_type == 'signature_delta' and block['type'] == 'thinking' and isinstance(delta.get('signature'), str):
                block['bytes'] += len(delta['signature'].encode())
                if block['bytes'] > MAX_EVENT_BYTES:
                    raise _protocol_error()
                if delta['signature']:
                    block['signatures'].append(delta['signature'])
            elif delta_type == 'input_json_delta' and block['type'] == 'tool_use' and isinstance(delta.get('partial_json'), str):
                if 'native' in block:
                    block['parts'].append(delta['partial_json'])
                if block['initial_input']:
                    raise _protocol_error()
                block['bytes'] += len(delta['partial_json'].encode())
                if block['bytes'] > MAX_EVENT_BYTES:
                    raise _protocol_error()
                block['arguments_started'] = block['arguments_started'] or bool(delta['partial_json'])
                yield chunk({'tool_calls': [{'index': block['tool_index'], 'function': {'arguments': delta['partial_json']}}]})
            else:
                raise _protocol_error()
        elif kind == 'content_block_stop':
            block = blocks.get(event.get('index'))
            if not block or block.get('closed'):
                raise _protocol_error()
            block['closed'] = True
            if 'native' in block:
                native = block['native']
                text = ''.join(block.pop('parts'))
                if block['type'] == 'text':
                    native['text'] = text
                elif block['type'] == 'thinking':
                    native['thinking'] = text
                    native['signature'] = ''.join(block['signatures'])
                elif block['type'] == 'tool_use' and text:
                    native['input'] = _decode_json(text.encode(), MAX_EVENT_BYTES)
            if block['type'] == 'tool_use' and not block['arguments_started']:
                yield chunk({'tool_calls': [{'index': block['tool_index'], 'function': {'arguments': '{}'}}]})
            elif block['type'] == 'thinking':
                signature = ''.join(block.pop('signatures'))
                if signature:
                    detail = {'type': 'reasoning.text', 'format': 'anthropic-claude-v1',
                              'index': event['index'], 'signature': signature}
                    if not block['has_text']:
                        detail['data'] = signature
                    yield chunk({'reasoning_details': [detail]})
        elif kind == 'message_delta':
            delta = event.get('delta')
            if not isinstance(delta, dict) or not isinstance(event.get('usage', {}), dict):
                raise _protocol_error()
            raw_usage.update(event.get('usage') or {})
            if delta.get('stop_reason') is not None:
                stop_reason = delta['stop_reason']
                stop_sequence = delta.get('stop_sequence')
                if stop_reason == 'refusal' and isinstance(delta.get('stop_details'), dict):
                    explanation = delta['stop_details'].get('explanation')
                    if explanation:
                        if not isinstance(explanation, str):
                            raise _protocol_error()
                        refusal = {'refusal': explanation}
                        if not has_text:
                            refusal['content'] = explanation
                            has_text = True
                        yield chunk(refusal)
        elif kind == 'message_stop':
            if any(not block.get('closed') for block in blocks.values()):
                raise _protocol_error()
            first_thinking = next((index for index, block in native_blocks.items()
                                   if block['type'] in ('thinking', 'redacted_thinking')), None)
            if payload is not None and first_thinking is not None:
                yield chunk({'reasoning_details': [{
                    'index': first_thinking,
                    **_reasoning_binding(payload, list(native_blocks.values())),
                }]})
            final = chunk({}, _finish(stop_reason))
            final['choices'][0].update({'stop_reason': stop_reason, 'stop_sequence': stop_sequence})
            final['usage'] = _usage(raw_usage)
            yield final
            return
        else:
            raise _protocol_error()
    raise _protocol_error()


async def _collect(chunks):
    result = None
    message = {'role': 'assistant', 'content': ''}
    text, reasoning, details, calls = [], [], {}, {}
    size = 0
    async for chunk in chunks:
        size += len(json.dumps(chunk, ensure_ascii=False).encode())
        if size > MAX_JSON_BYTES:
            raise HTTPException(status_code=502, detail='Claude response exceeded the size limit.')
        if result is None:
            result = {key: chunk[key] for key in ('id', 'created', 'model')}
            result.update({'object': 'chat.completion', 'choices': [{'index': 0, 'message': message, 'finish_reason': None}]})
        choice = chunk['choices'][0]
        delta = choice['delta']
        text.append(delta.get('content', ''))
        reasoning.append(delta.get('reasoning_content', ''))
        if delta.get('refusal'):
            message['refusal'] = delta['refusal']
        for detail in delta.get('reasoning_details', []):
            target = details.setdefault(detail['index'], {})
            for key, value in detail.items():
                target[key] = target.get(key, '') + value if key == 'text' else value
        for call in delta.get('tool_calls', []):
            target = calls.setdefault(call['index'], {'id': call.get('id'), 'type': 'function', 'function': {'name': '', 'arguments': ''}})
            function = call.get('function') or {}
            target['function']['name'] += function.get('name', '')
            target['function']['arguments'] += function.get('arguments', '')
        if choice.get('finish_reason') is not None:
            result['choices'][0].update({key: value for key, value in choice.items() if key not in ('index', 'delta')})
        if 'usage' in chunk:
            result['usage'] = chunk['usage']
    if result is None or result['choices'][0]['finish_reason'] is None:
        raise _protocol_error()
    message['content'] = ''.join(text)
    if any(reasoning):
        message['reasoning_content'] = ''.join(reasoning)
    if details:
        message['reasoning_details'] = list(details.values())
    if calls:
        message['tool_calls'] = list(calls.values())
    return result


async def _json_chunks(completion):
    choice = completion['choices'][0]
    common = {key: completion[key] for key in ('id', 'created', 'model')}
    common['object'] = 'chat.completion.chunk'
    delta = dict(choice['message'])
    if 'tool_calls' in delta:
        delta['tool_calls'] = [{'index': index, **call} for index, call in enumerate(delta['tool_calls'])]
    yield {**common, 'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}
    yield {**common, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': choice['finish_reason'],
                                'stop_reason': choice['stop_reason'], 'stop_sequence': choice['stop_sequence']}],
           'usage': completion['usage']}


async def _sse(chunks, response, session):
    identity = None
    try:
        async for chunk in chunks:
            if identity is None:
                identity = {key: chunk[key] for key in ('id', 'created', 'model') if key in chunk}
            yield 'data: ' + json.dumps(chunk, separators=(',', ':'), ensure_ascii=False) + '\n\n'
    except asyncio.CancelledError:
        raise
    except Exception as error:
        detail = error.detail if isinstance(error, HTTPException) else 'Claude response stream was interrupted.'
        status = error.status_code if isinstance(error, HTTPException) else 502
        yield 'data: ' + json.dumps({'error': {'message': detail, 'type': 'upstream_error', 'code': status}}) + '\n\n'
        # A separate terminal survives consumers that continue after standalone errors.
        failed = {
            **(identity or {}), 'object': 'chat.completion.chunk',
            'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'failed'}],
        }
        yield 'data: ' + json.dumps(failed, separators=(',', ':'), ensure_ascii=False) + '\n\n'
    finally:
        try:
            await chunks.aclose()
        finally:
            await cleanup_response(response, session)
    yield 'data: [DONE]\n\n'


async def generate_chat_completion(form_data: dict, token: dict, session_id: str):
    if not isinstance(form_data, dict):
        _invalid('Claude requires a Chat Completions request object.')
    payload = _payload(form_data)
    streaming = payload['stream']
    response = None
    session = None
    handed_off = False
    try:
        session = await get_provider_session()
        response, upstream = await request_model(
            session, 'POST', MESSAGES_URL,
            retry=can_retry_model_request(form_data), stream=streaming, passthrough=True,
            json=payload, headers=_headers(token, streaming), ssl=True, allow_redirects=False,
            timeout=get_client_timeout(stream=streaming),
        )
        _check_status(response, 'inference')
        if 'text/event-stream' in response.headers.get('Content-Type', ''):
            chunks = _chat_chunks(_events(stream_wrapper(response, session=session, stream=upstream, passthrough=True)), payload['model'], payload)
            if not streaming:
                return await _collect(chunks)
        else:
            completion = _completion(await _read_json(response, stream=upstream), payload['model'], payload)
            if not streaming:
                return completion
            chunks = _json_chunks(completion)
        result = StreamingResponse(
            _sse(chunks, response, session), media_type='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
            background=BackgroundTask(cleanup_response, response, session),
        )
        handed_off = True
        return result
    except (aiohttp.ClientError, asyncio.TimeoutError):
        raise HTTPException(status_code=502, detail='Unable to reach Claude for inference.') from None
    except (KeyError, TypeError, ValueError, AttributeError, RecursionError):
        raise _protocol_error() from None
    finally:
        if not handed_off:
            await cleanup_response(response, session)
