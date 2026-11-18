"""Real loopback model HTTP requests; no provider credentials, app startup, or forwarding mocks."""

import asyncio
import json
import math
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer


async def _check():
    from open_webui.utils.session_pool import (
        _model_retry_delay,
        can_retry_model_request,
        cleanup_response,
        request_model,
        stream_wrapper,
    )

    counts = Counter()
    received = defaultdict(list)
    late_disconnect = asyncio.Event()
    cancel_started = asyncio.Event()
    date_deadline = 0
    body = b'{"model":"test","messages":[{"role":"user","content":"hello"}]}'
    token = b'data: {"token":"one"}\n\n'

    async def provider(request):
        nonlocal date_deadline
        case = request.match_info['case']
        assert await request.read() == body
        assert request.headers['Authorization'] == 'Bearer test-only'
        counts[case] += 1
        received[case].append(time.time())
        attempt = counts[case]
        transport = request.transport
        assert transport is not None
        if case.startswith('status-'):
            status = int(case.split('-')[1])
            if attempt == 1 or (status < 500 and status != 429):
                return web.Response(status=status, headers={'Retry-After': '0'})
        if case in ('exhausted', 'unsafe', 'mixed', 'cancel', 'milliseconds', 'date'):
            if case in ('exhausted', 'unsafe') or attempt == 1:
                headers = {'Retry-After': '0'}
                if case == 'milliseconds':
                    headers = {'retry-after-ms': '30'}
                elif case == 'date':
                    date_deadline = math.ceil(time.time()) + 1
                    headers = {'Retry-After': format_datetime(datetime.fromtimestamp(date_deadline, timezone.utc), usegmt=True)}
                elif case == 'cancel':
                    headers = {'Retry-After': '60'}
                    cancel_started.set()
                return web.Response(status=503, headers=headers)
        if (case == 'disconnect' and attempt == 1) or (case == 'mixed' and attempt == 2):
            transport.close()
            return web.Response()
        if case == 'timeout' and attempt == 1:
            await asyncio.sleep(0.2)
        if case in ('truncated', 'early-stream', 'idle-stream', 'late-stream') and attempt == 1:
            content_type = 'application/json' if case == 'truncated' else 'text/event-stream'
            response = web.StreamResponse(headers={'Content-Type': content_type, 'Content-Length': '100'})
            await response.prepare(request)
            if case == 'truncated':
                await response.write(b'{')
            elif case == 'idle-stream':
                await asyncio.sleep(0.2)
            elif case == 'late-stream':
                await response.write(token)
                await late_disconnect.wait()
            transport.close()
            return response
        if case in ('early-stream', 'idle-stream', 'lines'):
            return web.Response(body=token, content_type='text/event-stream')
        if case == 'sse-error':
            return web.Response(body=b'event: error\ndata: {"error":"overloaded"}\n\n', content_type='text/event-stream')
        if case == 'invalid-json':
            return web.Response(body=b'not-json', content_type='application/json')
        return web.json_response({'ok': True})

    app = web.Application()
    app.router.add_post('/{case}', provider)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        async def send(case, *, retry=True, stream=False, passthrough=True):
            return await request_model(
                session,
                'POST',
                str(server.make_url('/' + case)),
                retry=retry,
                stream=stream,
                passthrough=passthrough,
                data=body,
                headers={'Authorization': 'Bearer test-only'},
                timeout=aiohttp.ClientTimeout(total=5, sock_read=0.05),
            )

        for status in (429, 500, 502, 503, 504, 599, 400, 401, 403, 404, 408, 422):
            case = f'status-{status}'
            response, _ = await send(case)
            transient = status == 429 or status >= 500
            assert counts[case] == (2 if transient else 1)
            assert response.status == (200 if transient else status)
            await cleanup_response(response)
        for case in ('disconnect', 'timeout', 'truncated', 'milliseconds', 'date'):
            response, _ = await send(case)
            assert await response.json() == {'ok': True}
            assert counts[case] == 2
            await cleanup_response(response)
        assert received['milliseconds'][1] - received['milliseconds'][0] >= 0.025
        assert received['date'][1] >= date_deadline - 0.01
        for case in ('early-stream', 'idle-stream', 'lines', 'sse-error'):
            response, chunks = await send(case, stream=True, passthrough=case != 'lines')
            output = b''.join([chunk async for chunk in stream_wrapper(response, stream=chunks)])
            assert output == (b'event: error\ndata: {"error":"overloaded"}\n\n' if case == 'sse-error' else token)
            assert counts[case] == (2 if case in ('early-stream', 'idle-stream') else 1)
            assert response.closed
        response, chunks = await send('late-stream', stream=True)
        stream = stream_wrapper(response, stream=chunks)
        assert await anext(stream) == token
        late_disconnect.set()
        try:
            async for _ in stream:
                pass
        except aiohttp.ClientPayloadError:
            pass
        else:
            raise AssertionError('A broken partial stream must fail, not restart')
        assert counts['late-stream'] == 1 and response.closed
        response, _ = await send('exhausted')
        assert response.status == 503 and counts['exhausted'] == 2
        await cleanup_response(response)
        try:
            await send('mixed')
        except aiohttp.ClientConnectionError:
            pass
        else:
            raise AssertionError('HTTP and transport retries must share two attempts')
        assert counts['mixed'] == 2
        response, _ = await send('invalid-json')
        try:
            await response.json()
        except json.JSONDecodeError:
            pass
        else:
            raise AssertionError('Invalid provider JSON must remain an application error')
        assert counts['invalid-json'] == 1
        await cleanup_response(response)
        assert can_retry_model_request({'tools': [{'type': 'function', 'function': {'name': 'write'}}]})
        assert can_retry_model_request({'tools': [{'name': 'anthropic-client-tool'}]})
        for payload in ({'tools': [{'type': tool}]} for tool in ('mcp', 'computer', 'code_interpreter', 'custom')):
            assert not can_retry_model_request(payload)
        assert not can_retry_model_request({'mcp_servers': [{'url': 'https://unused.invalid'}]})
        response, _ = await send('unsafe', retry=can_retry_model_request({'tools': [{'type': 'mcp'}]}))
        assert response.status == 503 and counts['unsafe'] == 1
        await cleanup_response(response)
        task = asyncio.create_task(send('cancel'))
        await cancel_started.wait()
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError('Cancellation must propagate immediately')
        assert counts['cancel'] == 1
        assert _model_retry_delay({'Retry-After': 'bad'}) == 1
        assert _model_retry_delay({'Retry-After': 'inf'}) == 1
        assert _model_retry_delay({'Retry-After': '0'}) == 0


def test_model_request_retry():
    # Keep import-time environment configuration out of other integration tests.
    subprocess.run([sys.executable, str(Path(__file__).resolve())], check=True)


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    with tempfile.TemporaryDirectory(prefix='open-webui-model-retry-') as directory:
        os.environ.update(
            PYTHON_DOTENV_DISABLED='1',
            FROM_INIT_PY='false',
            WEBUI_AUTH='true',
            WEBUI_SECRET_KEY=os.urandom(32).hex(),
            DATA_DIR=directory,
            STATIC_DIR=str(Path(directory) / 'static'),
            LICENSE_BLOB_PATH='',
            DATABASE_URL=f'sqlite:///{directory}/unused.db',
        )
        asyncio.run(_check())
