"""Shared aiohttp ClientSession pool.

Instead of creating a new ClientSession (and TCPConnector) per request,
callers acquire a long-lived session from this module.  The pool manages
a single TCPConnector with configurable limits, enabling TCP/SSL connection
reuse, shared DNS cache, and bounded concurrency.

All pool parameters are configurable via environment variables:
    - AIOHTTP_POOL_CONNECTIONS (default 100) — max total connections
    - AIOHTTP_POOL_CONNECTIONS_PER_HOST (default 30) — per-host limit
    - AIOHTTP_POOL_DNS_TTL (default 300) — DNS cache TTL in seconds

Usage:
    from open_webui.utils.session_pool import get_session, cleanup_response

    session = await get_session()
    r = await session.request(...)
    # When done with the *response* (not the session):
    await cleanup_response(r)

IMPORTANT: Callers must NOT close the shared session.  Only the response
needs cleanup.  The session is closed once during application shutdown
via ``close_session()``.
"""

import asyncio
import logging
import math
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import aiohttp
from open_webui.env import (
    AIOHTTP_CLIENT_STREAM_IDLE_TIMEOUT,
    AIOHTTP_CLIENT_TIMEOUT,
    AIOHTTP_POOL_CONNECTIONS,
    AIOHTTP_POOL_CONNECTIONS_PER_HOST,
    AIOHTTP_POOL_DNS_TTL,
)
from open_webui.utils.misc import stream_chunks_handler

log = logging.getLogger(__name__)

_session: Optional[aiohttp.ClientSession] = None

_CLIENT_TIMEOUT = aiohttp.ClientTimeout(total=AIOHTTP_CLIENT_TIMEOUT)
_CLIENT_STREAM_TIMEOUT = aiohttp.ClientTimeout(
    total=AIOHTTP_CLIENT_TIMEOUT,
    sock_read=AIOHTTP_CLIENT_STREAM_IDLE_TIMEOUT,
)


def get_client_timeout(stream: bool = False) -> aiohttp.ClientTimeout:
    return _CLIENT_STREAM_TIMEOUT if stream else _CLIENT_TIMEOUT


async def get_session() -> aiohttp.ClientSession:
    """Return the shared aiohttp ClientSession, creating it lazily."""
    global _session
    if _session is None or _session.closed:
        connector_kwargs = {
            'ttl_dns_cache': AIOHTTP_POOL_DNS_TTL,
            'enable_cleanup_closed': True,
        }
        if AIOHTTP_POOL_CONNECTIONS is not None:
            connector_kwargs['limit'] = AIOHTTP_POOL_CONNECTIONS
        else:
            connector_kwargs['limit'] = 0  # aiohttp: 0 = unlimited
        if AIOHTTP_POOL_CONNECTIONS_PER_HOST is not None:
            connector_kwargs['limit_per_host'] = AIOHTTP_POOL_CONNECTIONS_PER_HOST
        else:
            connector_kwargs['limit_per_host'] = 0  # aiohttp: 0 = unlimited
        connector = aiohttp.TCPConnector(**connector_kwargs)
        timeout = get_client_timeout()
        _session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            trust_env=True,
        )
        log.info(
            'Created shared aiohttp session pool (limit=%s, per_host=%s, dns_ttl=%d)',
            AIOHTTP_POOL_CONNECTIONS or 'unlimited',
            AIOHTTP_POOL_CONNECTIONS_PER_HOST or 'unlimited',
            AIOHTTP_POOL_DNS_TTL,
        )
    return _session


async def get_provider_session() -> aiohttp.ClientSession:
    """Isolate account cookies while reusing the shared connection pool.

    The caller owns this session and must close it after consuming its response.
    """
    shared = await get_session()
    return aiohttp.ClientSession(
        connector=shared.connector,
        connector_owner=False,
        cookie_jar=aiohttp.DummyCookieJar(),
        timeout=get_client_timeout(),
        trust_env=True,
    )


async def close_session():
    """Close the shared session.  Called during application shutdown."""
    global _session
    if _session and not _session.closed:
        await _session.close()
        log.info('Closed shared aiohttp session pool')
        _session = None


async def cleanup_response(
    response: Optional[aiohttp.ClientResponse],
    session: Optional[aiohttp.ClientSession] = None,
):
    """Release and close an aiohttp response, optionally closing the session.

    When using the shared pool, ``session`` should be ``None`` (the pool
    session is never closed per-request).  When a caller creates its own
    one-off session, pass it here to close it after the response.
    """
    if response:
        if not response.closed:
            # aiohttp 3.9+ made ClientResponse.close() synchronous (returns None).
            # Older versions returned a coroutine.  Handle both gracefully.
            result = response.close()
            if result is not None:
                await result
    if session:
        if not session.closed:
            result = session.close()
            if result is not None:
                await result


def can_retry_model_request(payload: dict) -> bool:
    """Function declarations are model inputs; provider-executed tools are not safe to replay."""
    tools = payload.get('tools') or []
    return not payload.get('mcp_servers') and isinstance(tools, list) and all(
        isinstance(tool, dict) and tool.get('type', 'function') == 'function' for tool in tools
    )


def _model_retry_delay(headers) -> float:
    for name, scale in (('Retry-After', 1), ('retry-after-ms', 0.001)):
        value = headers.get(name)
        if value is None:
            continue
        try:
            delay = float(value) * scale
        except ValueError:
            if name != 'Retry-After':
                continue
            try:
                date = parsedate_to_datetime(value)
                delay = max(0.0, date.replace(tzinfo=date.tzinfo or timezone.utc).timestamp() - time.time())
            except (ValueError, TypeError, OverflowError):
                continue
        if math.isfinite(delay) and delay >= 0:
            return delay
    return 1.0


async def _prefetched_stream(first, stream):
    if first is not None:
        yield first
    async for chunk in stream:
        yield chunk


async def request_model(session, method, url, *, retry: bool, stream=False, passthrough=False, **kwargs):
    """Retry a model HTTP request once, before returning any body bytes to its caller.

    Buffer non-streaming bodies and prime streams so read failures before output
    share the same two-attempt budget as connection and HTTP status failures.
    After return, the caller owns the response and no stream failure is retried.
    """
    if not retry:
        return await session.request(method, url, **kwargs), None

    for attempt in range(2):
        response = None
        try:
            response = await session.request(method, url, **kwargs)
            if attempt == 0 and (response.status == 429 or 500 <= response.status < 600):
                delay = _model_retry_delay(response.headers)
                await cleanup_response(response)
                await asyncio.sleep(delay)
                continue

            chunks = None
            if response.status < 400:
                if stream or 'text/event-stream' in response.headers.get('Content-Type', ''):
                    chunks = response.content.iter_any() if passthrough else stream_chunks_handler(response.content)
                    first = await anext(chunks, None)
                    chunks = _prefetched_stream(first, chunks)
                else:
                    await response.read()
            return response, chunks
        except (aiohttp.ClientSSLError, aiohttp.ServerFingerprintMismatch):
            await cleanup_response(response)
            raise
        except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError):
            await cleanup_response(response)
            if attempt:
                raise
            await asyncio.sleep(1)
        except BaseException:
            # Cancellation and validation/application errors must never become retries.
            await cleanup_response(response)
            raise


async def stream_wrapper(response, session=None, passthrough=False, stream=None):
    """Wrap a stream to ensure cleanup happens even if streaming is interrupted.

    This is more reliable than BackgroundTask which may not run if the client
    disconnects.  When using the shared pool, ``session`` should be ``None``.

    ``passthrough=True`` yields raw network chunks (iter_any) instead of
    lines: byte-identical output without a buffer scan, slice and copy per
    line. Only for streams no internal consumer parses line-by-line.
    ``stream`` may already contain a prefetched first chunk from request_model.
    """
    try:
        if stream is None:
            stream = response.content.iter_any() if passthrough else stream_chunks_handler(response.content)
        async for chunk in stream:
            yield chunk
    finally:
        await cleanup_response(response, session)
