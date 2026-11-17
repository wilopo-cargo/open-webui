"""Direct Codex OAuth and stateless Responses transport.

Protocol normalization adapted from 9router (https://github.com/decolua/9router).
MIT License — Copyright (c) 2024-2026 decolua and contributors

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
import binascii
import time
from copy import deepcopy
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import aiohttp
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from open_webui.utils.json_codec import JSONCodec
from open_webui.utils.misc import get_reasoning_details
from open_webui.utils.response import (
    chat_completion_chunk_template,
    convert_responses_stream_to_chat_completions,
)
from open_webui.utils.session_pool import (
    can_retry_model_request,
    cleanup_response,
    get_client_timeout,
    get_provider_session,
    request_model,
    stream_wrapper,
)

PROVIDER_ID = 'codex'
CLIENT_VERSION = '0.167.1'
RESPONSES_URL = 'https://chatgpt.com/backend-api/codex/responses'
MODELS_URL = f'https://chatgpt.com/backend-api/codex/models?client_version={CLIENT_VERSION}'
OAUTH = {
    'authorize_url': 'https://auth.openai.com/oauth/authorize',
    'token_url': 'https://auth.openai.com/oauth/token',
    'client_id': 'app_EMoamEEZ73f0CkXaXp7hrann',
    'redirect_uri': 'http://localhost:1455/auth/callback',
    'scope': 'openid profile email offline_access',
}

# These are wire-format distinctions, not a fallback model catalog. Only live models are exposed.
_LITE_MODELS = {'gpt-6.1-sol', 'gpt-6.1-terra', 'gpt-6-sol', 'gpt-6-luna'}
_ALLOWED_FIELDS = {
    'model', 'input', 'instructions', 'tools', 'tool_choice', 'stream', 'store',
    'reasoning', 'service_tier', 'include', 'prompt_cache_key', 'client_metadata',
    'text', 'parallel_tool_calls',
}
_REASONING_FORMAT = 'openai-responses-v1'
_IMAGE_TYPES = {'image/png', 'image/jpeg', 'image/webp', 'image/gif'}
_MAX_IMAGE_BYTES = 20 * 1024 * 1024
_OAUTH_TIMEOUT = aiohttp.ClientTimeout(total=30)


def authorization_url(state: str, challenge: str) -> str:
    return OAUTH['authorize_url'] + '?' + urlencode({
        'response_type': 'code',
        'client_id': OAUTH['client_id'],
        'redirect_uri': OAUTH['redirect_uri'],
        'scope': OAUTH['scope'],
        'code_challenge': challenge,
        'code_challenge_method': 'S256',
        'id_token_add_organizations': 'true',
        'codex_cli_simplified_flow': 'true',
        'originator': 'codex_cli_rs',
        'state': state,
    })


async def _read_json(response, operation: str) -> dict:
    if not 200 <= response.status < 300:
        status = response.status if 400 <= response.status < 600 else 502
        raise HTTPException(status, f'Codex {operation} failed (HTTP {response.status}).')
    try:
        result = await response.json(loads=JSONCodec.loads)
    except (ValueError, aiohttp.ContentTypeError):
        raise HTTPException(502, f'Codex {operation} returned invalid JSON.') from None
    if not isinstance(result, dict):
        raise HTTPException(502, f'Codex {operation} returned an invalid response.')
    return result


async def _oauth_request(payload: dict, *, form: bool) -> dict:
    response = None
    session = None
    try:
        session = await get_provider_session()
        response = await session.post(
            OAUTH['token_url'],
            **({'data': payload} if form else {'json': payload}),
            headers={'Accept': 'application/json'},
            timeout=_OAUTH_TIMEOUT,
            ssl=True,
            allow_redirects=False,
        )
        tokens = await _read_json(response, 'authorization')
        if not isinstance(tokens.get('access_token'), str) or not tokens['access_token']:
            raise HTTPException(502, 'Codex authorization returned no access token.')
        return tokens
    except (aiohttp.ClientError, asyncio.TimeoutError):
        raise HTTPException(502, 'Could not reach Codex authorization securely.') from None
    finally:
        await cleanup_response(response, session)


async def exchange_code(code: str, state: str, verifier: str) -> dict:
    # Core validates owner-bound, single-use state before reaching this exchange.
    return await _oauth_request({
        'grant_type': 'authorization_code',
        'client_id': OAUTH['client_id'],
        'code': code,
        'redirect_uri': OAUTH['redirect_uri'],
        'code_verifier': verifier,
    }, form=True)


async def refresh_token(refresh_token: str) -> dict:
    return await _oauth_request({
        'grant_type': 'refresh_token',
        'client_id': OAUTH['client_id'],
        'refresh_token': refresh_token,
    }, form=False)


def _account_id(token: dict) -> str | None:
    # JWT claims are routing metadata from the fixed OAuth endpoint, never authorization proof.
    for key in ('id_token', 'access_token'):
        value = token.get(key)
        if not isinstance(value, str) or value.count('.') != 2:
            continue
        try:
            encoded = value.split('.')[1]
            claims = JSONCodec.loads(base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)))
            auth = claims.get('https://api.openai.com/auth') or {}
            account = auth.get('chatgpt_account_id') or claims.get('account_id')
            if isinstance(account, str) and account and not any(ord(c) < 32 or ord(c) == 127 for c in account):
                return account
        except (ValueError, TypeError, AttributeError, binascii.Error):
            continue
    return None


def _headers(token: dict, session_id: str | None = None, *, lite: bool = False) -> dict:
    access_token = token.get('access_token')
    if not isinstance(access_token, str) or not access_token or any(c in access_token for c in '\r\n'):
        raise HTTPException(401, 'Reconnect this Codex subscription.')
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'text/event-stream' if session_id else 'application/json',
        'Content-Type': 'application/json',
        'originator': 'codex_cli_rs',
        'User-Agent': f'codex_cli_rs/{CLIENT_VERSION}',
        'version': CLIENT_VERSION,
    }
    account = _account_id(token)
    if account:
        headers['ChatGPT-Account-ID'] = account
    if session_id:
        headers['session_id'] = session_id
    if lite:
        headers['x-openai-internal-codex-responses-lite'] = 'true'
    return headers


async def list_models(token: dict) -> list[dict]:
    response = None
    session = None
    try:
        session = await get_provider_session()
        response = await session.get(
            MODELS_URL, headers=_headers(token), timeout=_OAUTH_TIMEOUT, ssl=True, allow_redirects=False,
        )
        data = await _read_json(response, 'model discovery')
        rows = data.get('models', data.get('data', data.get('results')))
        if not isinstance(rows, list):
            raise HTTPException(502, 'Codex model discovery returned no model list.')
        models = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            model_id = row.get('id') or row.get('slug') or row.get('model') or row.get('name')
            if not isinstance(model_id, str) or not model_id:
                continue
            if row.get('type') in ('image', 'stt', 'embedding') or row.get('kind') in ('image', 'stt', 'embedding'):
                continue
            modalities = row.get('input_modalities') or row.get('modalities') or []
            if isinstance(modalities, dict):
                modalities = modalities.get('input') or []
            name = row.get('display_name') or row.get('displayName') or row.get('name') or model_id
            models[model_id] = {
                'id': model_id,
                'name': name if isinstance(name, str) else model_id,
                'capabilities': {'vision': 'image' in modalities, 'builtin_tools': True},
                'responses_lite': bool(row.get('responses_lite') or row.get('responsesLite') or model_id in _LITE_MODELS),
            }
        if not models:
            raise HTTPException(502, 'Codex returned no supported chat models. Refresh models or reconnect.')
        return list(models.values())
    except (aiohttp.ClientError, asyncio.TimeoutError, TypeError, AttributeError, ValueError):
        raise HTTPException(502, 'Could not discover Codex models securely.') from None
    finally:
        await cleanup_response(response, session)


def _reasoning_details(item: dict, index: int = 0) -> list[dict]:
    encrypted = item.get('encrypted_content')
    if not encrypted:
        return []
    return [{
        'type': 'reasoning.encrypted', 'format': _REASONING_FORMAT,
        'id': item.get('id', ''), 'index': index, 'data': encrypted,
    }]


def _restore_reasoning(details) -> list[dict]:
    if not isinstance(details, list):
        details = [details] if details else []
    return [
        {'type': 'reasoning', 'summary': [], 'encrypted_content': detail['data']}
        for detail in details
        if isinstance(detail, dict) and detail.get('format') == _REASONING_FORMAT
        and detail.get('type') == 'reasoning.encrypted' and isinstance(detail.get('data'), str)
    ]


def _content_parts(content, *, tool: bool = False):
    if not isinstance(content, list):
        if content is not None and not isinstance(content, str):
            raise HTTPException(400, 'Codex message content must be text or text/image parts.')
        return content or ''
    parts = []
    for part in content:
        if not isinstance(part, dict):
            raise HTTPException(400, 'Codex message contains an invalid content part.')
        part = dict(part)
        kind = part.get('type')
        if kind in ('text', 'input_text', 'output_text'):
            if not isinstance(part.get('text'), str):
                raise HTTPException(400, 'Codex text parts require text.')
            part['type'] = 'input_text' if tool else 'text'
        elif kind in ('image_url', 'input_image'):
            part['type'] = 'input_image' if tool else 'image_url'
        else:
            raise HTTPException(400, 'Codex subscriptions support text and images, not audio or file input parts.')
        parts.append(part)
    return parts


def _prepare_payload(form_data: dict, token: dict, session_id: str) -> tuple[dict, bool]:
    # Import converters lazily: the normal chat router also imports the subscription dispatcher.
    from open_webui.routers.openai import convert_to_responses_payload

    payload = dict(form_data)
    messages = payload.pop('messages', None)
    if not isinstance(messages, list) or not messages:
        raise HTTPException(400, 'Codex requires chat messages.')
    for key in ('stop', 'audio', 'prediction', 'functions', 'function_call', 'thinking', 'mcp_servers', 'logit_bias', 'logprobs', 'top_logprobs'):
        if payload.get(key):
            raise HTTPException(400, f'Codex subscriptions do not support the {key} parameter.')
    if payload.get('modalities') not in (None, ['text']):
        raise HTTPException(400, 'Codex subscriptions return text and function calls only.')
    if payload.get('n') not in (None, 1):
        raise HTTPException(400, 'Codex subscriptions support one completion at a time.')
    if payload.get('input'):
        raise HTTPException(400, 'Use chat messages, not native Responses input, for Codex subscriptions.')
    tools = payload.get('tools') or []
    if not isinstance(tools, list) or any(
        not isinstance(tool, dict) or tool.get('type', 'function') != 'function'
        or not isinstance(tool.get('function', tool), dict)
        or not isinstance(tool.get('function', tool).get('name'), str)
        or not tool.get('function', tool)['name'] for tool in tools
    ):
        raise HTTPException(400, 'Codex subscriptions support WebUI function tools, not provider-hosted tools.')
    if 'tools' in payload:
        payload['tools'] = [{**tool, 'type': 'function'} for tool in tools]

    prepared = []
    system = []
    for original in deepcopy(messages):
        if not isinstance(original, dict) or original.get('role') not in ('system', 'developer', 'user', 'assistant', 'tool'):
            raise HTTPException(400, 'Codex message has an unsupported role.')
        message = original
        role = message['role']
        content = _content_parts(message.get('content'), tool=role == 'tool')
        if role in ('system', 'developer') and isinstance(content, list) and any(p['type'] != 'text' for p in content):
            raise HTTPException(400, 'Codex system and developer messages must contain text.')
        if role == 'assistant' and isinstance(content, list) and any(p['type'] != 'text' for p in content):
            raise HTTPException(400, 'Codex assistant history must contain text; images belong in user/tool messages.')
        message['content'] = content
        if role == 'tool' and (not isinstance(message.get('tool_call_id'), str) or not message['tool_call_id']):
            raise HTTPException(400, 'Codex tool results require their original tool_call_id.')
        for call in message.get('tool_calls') or []:
            if (not isinstance(call, dict) or call.get('type', 'function') != 'function'
                    or not isinstance(call.get('function'), dict)
                    or not isinstance(call.get('id'), str) or not call['id']
                    or not isinstance(call['function'].get('name'), str) or not call['function']['name']):
                raise HTTPException(400, 'Codex assistant tool calls require their original ID and function name.')
            arguments = call['function'].get('arguments', '{}')
            if isinstance(arguments, dict):
                arguments = JSONCodec.dumps(arguments)
            if not isinstance(arguments, str):
                raise HTTPException(400, 'Codex function arguments must be a JSON string or object.')
            call['function']['arguments'] = arguments
        if role == 'system':
            system.append(content if isinstance(content, str) else '\n'.join(p['text'] for p in content))
            continue
        if message.get('output'):
            if not isinstance(message['output'], list):
                raise HTTPException(400, 'Codex stored output must be a list.')
            replay = []
            for item in message['output']:
                if not isinstance(item, dict):
                    raise HTTPException(400, 'Codex stored output contains an invalid item.')
                restored = _restore_reasoning(item.get('reasoning_details')) if item.get('type') == 'reasoning' else []
                replay.extend(restored if restored else [item])
            message['output'] = replay
        else:
            restored = _restore_reasoning(get_reasoning_details(message))
            if restored:
                prepared.append({'role': 'assistant', 'output': restored})
        prepared.append(message)
    if system:
        prepared.insert(0, {'role': 'system', 'content': '\n\n'.join(system)})
    payload['messages'] = prepared
    if payload.get('text') is not None and not isinstance(payload['text'], dict):
        raise HTTPException(400, 'Codex text options must be an object.')
    response_format = payload.pop('response_format', None)
    if response_format:
        if not isinstance(response_format, dict) or response_format.get('type') not in ('text', 'json_object', 'json_schema'):
            raise HTTPException(400, 'Codex response_format must be text, json_object or json_schema.')
        fmt = {'type': response_format['type']}
        if fmt['type'] == 'json_schema':
            if not isinstance(response_format.get('json_schema'), dict):
                raise HTTPException(400, 'Codex json_schema response format requires a schema.')
            fmt.update(response_format['json_schema'])
        payload['text'] = {**(payload.get('text') or {}), 'format': fmt}
    payload = convert_to_responses_payload(payload)
    tool_choice = payload.get('tool_choice')
    if isinstance(tool_choice, dict):
        if tool_choice.get('type') != 'function' or tool_choice.get('name') not in {t.get('name') for t in payload.get('tools', [])}:
            raise HTTPException(400, 'Codex tool_choice must name an available function.')
    elif tool_choice not in (None, 'auto', 'none', 'required'):
        raise HTTPException(400, 'Codex tool_choice must be auto, none, required or a named function.')
    if tool_choice == 'required' and not tools:
        raise HTTPException(400, 'Codex tool_choice requires at least one function.')
    for item in payload['input']:
        if item.get('type') not in ('message', 'function_call', 'function_call_output', 'reasoning'):
            raise HTTPException(400, 'Codex cannot replay this stored output item without its full text/tool content.')
        # store=false cannot resolve server-generated IDs; call_id is the durable tool pairing key.
        item.pop('id', None)
        if item.get('role') == 'system':
            item['role'] = 'developer'
    if not payload['input']:
        raise HTTPException(400, 'Codex requires at least one non-system message.')
    model = payload.get('model')
    if not isinstance(model, str) or not model:
        raise HTTPException(400, 'Codex requires a model ID.')
    catalog = (token.get('connection') or {}).get('models') or []
    lite = model in _LITE_MODELS or any(m.get('id') == model and m.get('responses_lite') for m in catalog if isinstance(m, dict))
    reasoning = payload.get('reasoning')
    if reasoning is not None and not isinstance(reasoning, dict):
        raise HTTPException(400, 'Codex reasoning must be an object.')
    reasoning = dict(reasoning or {})
    effort = reasoning.get('effort') or payload.get('reasoning_effort') or ('medium' if lite else 'low')
    if effort not in ('none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'):
        raise HTTPException(400, 'Codex reasoning effort is not supported.')
    if lite and effort in ('none', 'minimal'):
        raise HTTPException(400, 'This Codex model requires reasoning effort low or higher.')
    reasoning['effort'] = effort
    if lite:
        if payload.get('parallel_tool_calls') is True:
            raise HTTPException(400, 'This Codex model does not support parallel tool calls.')
        prefix = [{'type': 'additional_tools', 'role': 'developer', 'tools': payload.get('tools') or []}]
        prefix.append({'type': 'message', 'role': 'developer', 'content': [
            {'type': 'input_text', 'text': payload.get('instructions') or 'You are a helpful assistant.'},
        ]})
        payload.update(input=[*prefix, *payload['input']], instructions='', tools=None, parallel_tool_calls=False)
        payload.setdefault('tool_choice', 'auto')
        reasoning['context'] = 'all_turns'
    else:
        payload.setdefault('instructions', 'You are a helpful assistant.')
        reasoning.setdefault('summary', 'auto')
    payload['reasoning'] = reasoning
    include = payload.get('include') or []
    if not isinstance(include, list) or any(not isinstance(value, str) for value in include):
        raise HTTPException(400, 'Codex include must be a list of strings.')
    if effort != 'none':
        payload['include'] = list(dict.fromkeys([*include, 'reasoning.encrypted_content']))
    if payload.get('service_tier') == 'fast':
        payload['service_tier'] = 'priority'
    elif payload.get('service_tier') not in (None, 'priority', 'auto', 'default'):
        raise HTTPException(400, 'Codex service_tier is not supported.')
    if payload.get('service_tier') in ('auto', 'default'):
        payload.pop('service_tier')
    # Codex rejects token caps and sampling fields, including WebUI's auxiliary-task caps.
    # They cannot be enforced on this subscription endpoint; never send them upstream.
    payload = {key: value for key, value in payload.items() if key in _ALLOWED_FIELDS}
    payload.update(stream=True, store=False, prompt_cache_key=session_id)
    return payload, bool(lite)


async def _inline_image(url: str) -> str:
    if not isinstance(url, str) or not url:
        raise HTTPException(400, 'Codex image input requires an image URL.')
    if url.startswith('data:'):
        prefix, separator, encoded = url.partition(',')
        if not separator or prefix.lower() not in {f'data:{mime};base64' for mime in _IMAGE_TYPES}:
            raise HTTPException(400, 'Codex images must be base64 PNG, JPEG, WebP or GIF.')
        if len(encoded) > ((_MAX_IMAGE_BYTES + 2) // 3) * 4:
            raise HTTPException(413, 'Codex image exceeds 20 MiB.')
        try:
            if len(base64.b64decode(encoded, validate=True)) > _MAX_IMAGE_BYTES:
                raise HTTPException(413, 'Codex image exceeds 20 MiB.')
        except (ValueError, binascii.Error):
            raise HTTPException(400, 'Codex image contains invalid base64.') from None
        return url
    try:
        target = urlsplit(url)
        if (target.scheme not in ('http', 'https') or not target.hostname or target.username or target.password
                or target.port not in (None, 80, 443) or any(ord(c) <= 32 or c == '\\' for c in url)):
            raise ValueError()
    except ValueError:
        raise HTTPException(400, 'Codex remote images require public HTTP(S) URLs on port 80 or 443 without credentials.') from None
    # The existing connector rejects non-public IPs at the actual DNS resolution/connect,
    # including IPv4-mapped IPv6. No proxies, redirects, cookies or subscription credentials.
    from open_webui.utils.apps import AppTCPConnector

    try:
        async with aiohttp.ClientSession(
            connector=AppTCPConnector(), timeout=aiohttp.ClientTimeout(total=15),
            trust_env=False, cookie_jar=aiohttp.DummyCookieJar(), auto_decompress=False,
        ) as session:
            async with session.get(url, ssl=True, allow_redirects=False, headers={'Accept-Encoding': 'identity'}) as response:
                if response.status != 200 or response.content_type not in _IMAGE_TYPES:
                    raise HTTPException(400, 'Codex could not fetch a supported public image. Inline the image instead.')
                if any(x.lower() not in ('', 'identity') for x in response.headers.getall('Content-Encoding', ())):
                    raise HTTPException(400, 'Codex remote images must not use compressed HTTP transfer content.')
                data = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    if len(data) + len(chunk) > _MAX_IMAGE_BYTES:
                        raise HTTPException(413, 'Codex image exceeds 20 MiB.')
                    data.extend(chunk)
                if not data:
                    raise HTTPException(400, 'Codex remote image is empty.')
                return f'data:{response.content_type};base64,{base64.b64encode(data).decode("ascii")}'
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        raise HTTPException(400, 'Codex could not securely fetch this public image. Inline the image instead.') from None


async def _inline_images(payload: dict):
    for item in payload['input']:
        for key in ('content', 'output'):
            if not isinstance(item.get(key), list):
                continue
            for part in item[key]:
                if not isinstance(part, dict):
                    raise HTTPException(400, 'Codex input contains an invalid content part.')
                kind = part.get('type')
                if kind in ('input_image', 'image_url'):
                    image = part.get('image_url')
                    detail = part.get('detail', 'auto')
                    if isinstance(image, dict):
                        detail = image.get('detail') or detail
                        image = image.get('url')
                    if detail not in ('auto', 'low', 'high', 'original'):
                        raise HTTPException(400, 'Codex image detail is not supported.')
                    part.update(type='input_image', image_url=await _inline_image(image), detail=detail)
                elif kind not in ('text', 'input_text', 'output_text', 'summary_text', 'reasoning_text', 'refusal'):
                    raise HTTPException(400, 'Codex subscriptions support only text and images in content/tool outputs.')
                elif key == 'output' and kind in ('text', 'output_text'):
                    part['type'] = 'input_text'


async def _events(response, stream, session=None):
    data_lines = []
    event_name = ''
    complete = False
    async for line in stream_wrapper(response, session, stream=stream):
        text = line.decode('utf-8') if isinstance(line, bytes) else line
        text = text.rstrip('\r\n')
        if text.startswith('event:'):
            event_name = text[6:].strip()
            continue
        if text.startswith('data:'):
            data_lines.append(text[5:].lstrip(' '))
            continue
        if text or not data_lines:
            continue
        encoded = '\n'.join(data_lines)
        data_lines = []
        if encoded == '[DONE]':
            continue
        try:
            event = JSONCodec.loads(encoded)
        except ValueError:
            raise HTTPException(502, 'Codex returned an invalid event stream.') from None
        if not isinstance(event, dict):
            raise HTTPException(502, 'Codex returned an invalid event stream.')
        kind = event.get('type') or event_name
        event_name = ''
        event['type'] = kind
        if kind in ('error', 'response.failed'):
            raise HTTPException(502, 'Codex could not complete the response. Try again or reconnect the subscription.')
        if kind in ('response.completed', 'response.incomplete'):
            complete = True
        yield event
    if data_lines or not complete:
        raise HTTPException(502, 'Codex disconnected before completing the response.')


async def _collect_response(events, model: str) -> dict:
    result = {'model': model}
    items = {}
    final_output = None
    async for event in events:
        kind = event['type']
        if kind in ('response.created', 'response.completed', 'response.incomplete'):
            value = event.get('response') or {}
            result.update({k: v for k, v in value.items() if k != 'output'})
            if kind != 'response.created':
                result['status'] = kind.removeprefix('response.')
                final_output = value.get('output') or None
        index = event.get('output_index', 0)
        if kind in ('response.output_item.added', 'response.output_item.done'):
            items[index] = event['item']
        elif kind == 'response.function_call_arguments.delta':
            item = items.setdefault(index, {'type': 'function_call', 'arguments': ''})
            item['arguments'] = item.get('arguments', '') + event['delta']
        elif kind in ('response.output_text.delta', 'response.refusal.delta', 'response.reasoning_text.delta', 'response.reasoning_summary_text.delta'):
            reasoning = kind.startswith('response.reasoning_')
            item = items.setdefault(index, {'type': 'reasoning' if reasoning else 'message', 'role': 'assistant'})
            summary = kind == 'response.reasoning_summary_text.delta'
            parts = item.setdefault('summary' if summary else 'content', [])
            part_index = event.get('summary_index' if summary else 'content_index', 0)
            while len(parts) <= part_index:
                parts.append({})
            part = parts[part_index]
            field = 'refusal' if kind == 'response.refusal.delta' else 'text'
            part['type'] = 'refusal' if field == 'refusal' else ('summary_text' if summary else 'output_text')
            part[field] = part.get(field, '') + event['delta']
    result['output'] = final_output if final_output is not None else [items[key] for key in sorted(items)]
    return result


def _completion(result: dict) -> dict:
    from open_webui.routers.openai import convert_responses_result

    response = convert_responses_result(result)
    reasoning_text = []
    details = []
    output = result.get('output') or []
    for index, item in enumerate(output):
        if item.get('type') != 'reasoning':
            continue
        reasoning_text.extend(part.get('text', '') for part in (item.get('summary') or item.get('content') or []))
        item_details = _reasoning_details(item, index)
        details.extend(item_details)
        if item_details:
            item['reasoning_details'] = item_details
    message = response['choices'][0]['message']
    if reasoning_text:
        message['reasoning_content'] = ''.join(reasoning_text)
    if details:
        message['reasoning_details'] = details
    # Preserve native items/annotations and encrypted reasoning for normal saved-chat replay.
    response['output'] = output
    return response


async def _chat_events(events, model: str):
    sent = set()
    completion_id = f'chatcmpl-{uuid4()}'
    created = int(time.time())
    async for event in events:
        if event['type'] == 'response.created':
            value = event.get('response') or {}
            completion_id = value.get('id') or completion_id
            created = value.get('created_at') or created
            model = value.get('model') or model
        candidates = []
        if event['type'] == 'response.output_item.done':
            candidates = [event.get('item') or {}]
        elif event['type'] in ('response.completed', 'response.incomplete'):
            candidates = (event.get('response') or {}).get('output') or []
        for index, item in enumerate(candidates):
            if item.get('type') != 'reasoning':
                continue
            details = _reasoning_details(item, event.get('output_index', index))
            if details and details[0]['data'] not in sent:
                sent.add(details[0]['data'])
                chunk = chat_completion_chunk_template(model, message_id=completion_id)
                chunk['created'] = created
                chunk['choices'][0].update(delta={'role': 'assistant', 'reasoning_details': details}, finish_reason=None)
                yield f'data: {JSONCodec.dumps(chunk)}\n\n'
        yield f'data: {JSONCodec.dumps(event)}\n\n'


async def _stream_response(response, stream, model: str, session):
    last_chunk = None
    try:
        converted = convert_responses_stream_to_chat_completions(_chat_events(_events(response, stream, session), model))
        async for chunk in converted:
            if chunk.startswith('data: ') and chunk.strip() != 'data: [DONE]':
                last_chunk = JSONCodec.loads(chunk[6:])
            yield chunk
    except (HTTPException, aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError, TypeError):
        # Never let already-parsed function arguments execute after an upstream failure.
        terminal = chat_completion_chunk_template(model)
        if isinstance(last_chunk, dict):
            terminal.update({key: last_chunk[key] for key in ('id', 'created', 'model') if key in last_chunk})
        terminal['choices'][0].update(delta={}, finish_reason='failed')
        yield 'data: ' + JSONCodec.dumps(terminal) + '\n\n'
        yield 'data: ' + JSONCodec.dumps({'error': {'message': 'Codex response interrupted before completion.', 'code': 'codex_stream_error'}}) + '\n\n'
        yield 'data: [DONE]\n\n'
    finally:
        await cleanup_response(response, session)


async def generate_chat_completion(form_data: dict, token: dict, session_id: str):
    session_id = session_id or str(uuid4())
    if not isinstance(session_id, str) or len(session_id) > 256 or any(ord(c) < 32 or ord(c) == 127 for c in session_id):
        raise HTTPException(400, 'Codex session ID is invalid.')
    payload, lite = _prepare_payload(form_data, token, session_id)
    await _inline_images(payload)
    response = None
    session = None
    streaming = False
    try:
        session = await get_provider_session()
        response, stream = await request_model(
            session, 'POST', RESPONSES_URL,
            data=JSONCodec.dumps(payload), headers=_headers(token, session_id, lite=lite),
            ssl=True, allow_redirects=False, timeout=get_client_timeout(stream=True),
            stream=True, retry=can_retry_model_request(payload),
        )
        if response.status >= 400:
            raise HTTPException(response.status, f'Codex request failed (HTTP {response.status}).')
        if response.status != 200 or 'text/event-stream' not in response.headers.get('Content-Type', ''):
            raise HTTPException(502, 'Codex did not return the required Responses event stream.')
        if form_data.get('stream'):
            streaming = True
            return StreamingResponse(
                _stream_response(response, stream, payload['model'], session), media_type='text/event-stream',
                headers={'Cache-Control': 'no-cache'}, background=BackgroundTask(cleanup_response, response, session),
            )
        result = await _collect_response(_events(response, stream, session), payload['model'])
        return _completion(result)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError, TypeError):
        raise HTTPException(502, 'Could not complete the Codex request securely.') from None
    finally:
        if not streaming:
            await cleanup_response(response, session)
