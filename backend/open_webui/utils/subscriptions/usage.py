"""Read-only provider quota windows; never infer quota from local chat traffic.

Protocols: https://github.com/decolua/9router/tree/master/open-sse/services/usage
"""

import asyncio
import json
import math
from datetime import datetime

import aiohttp
from fastapi import HTTPException
from open_webui.utils.session_pool import cleanup_response, get_provider_session

MAX_USAGE_BYTES = 1024 * 1024
USAGE_TIMEOUT = aiohttp.ClientTimeout(total=20)


async def fetch_usage(url, headers):
    response = session = None
    try:
        session = await get_provider_session()
        response = await session.get(url, headers=headers, timeout=USAGE_TIMEOUT, ssl=True, allow_redirects=False)
        if response.status != 200:
            raise HTTPException(502, f'Provider usage is unavailable (HTTP {response.status}).')
        body = bytearray()
        async for chunk in response.content.iter_chunked(65536):
            if len(body) + len(chunk) > MAX_USAGE_BYTES:
                raise HTTPException(502, 'Provider usage response exceeded the size limit.')
            body.extend(chunk)
        data = json.loads(body)
        if not isinstance(data, dict):
            raise HTTPException(502, 'Provider returned invalid usage data.')
        return data
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError, RecursionError):
        raise HTTPException(502, 'Provider usage is temporarily unavailable.') from None
    finally:
        await cleanup_response(response, session)


def _finite_number(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _reset_at(value):
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            value = parsed.timestamp() if parsed.tzinfo is not None else None
        except (ValueError, OverflowError, OSError):
            return None
    elif _finite_number(value) and value >= 1_000_000_000_000:
        value /= 1000
    if not _finite_number(value) or not 0 < value <= 253402300799:
        return None
    return value


def _window(key, label, data, percent_key):
    if not isinstance(data, dict):
        return None
    used = data.get(percent_key)
    if not _finite_number(used) or used < 0:
        return None
    reset = next((data[name] for name in ('resets_at', 'reset_at', 'resetAt') if data.get(name) is not None), None)
    return {'id': key, 'label': label, 'used_percent': used, 'resets_at': _reset_at(reset)}


def codex_windows(data):
    by_id = data.get('rate_limits_by_limit_id')
    limits = data.get('rate_limit') or data.get('rate_limits') or (by_id.get('codex') if isinstance(by_id, dict) else None)
    if not isinstance(limits, dict):
        return []
    windows = []
    for key, alias, label in (('primary_window', 'primary', 'Session'), ('secondary_window', 'secondary', 'Weekly')):
        item = limits.get(key) or limits.get(alias)
        percent_key = 'used_percent' if isinstance(item, dict) and 'used_percent' in item else 'percent_used'
        window = _window(key, label, item, percent_key)
        if window is not None:
            windows.append(window)
    return windows


def claude_windows(data):
    windows = []
    for key, label in (
        ('five_hour', '5-hour'), ('seven_day', '7-day'),
        ('seven_day_sonnet', '7-day Sonnet'), ('seven_day_opus', '7-day Opus'),
        ('seven_day_oauth_apps', '7-day OAuth apps'),
    ):
        window = _window(key, label, data.get(key), 'utilization')
        if window is not None:
            windows.append(window)
    return windows
