import ipaddress
import logging
import re
import threading
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from urllib.parse import quote, urlsplit

import httpx
from open_webui.env import AIOHTTP_CLIENT_TIMEOUT_TOOL_SERVER
from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class ComposioToolkitPolicy(BaseModel):
    tools: list[str]
    auth_config_id: str | None = None

    model_config = ConfigDict(extra='forbid')

    @field_validator('tools', mode='before')
    @classmethod
    def normalize_tools(cls, value):
        if not isinstance(value, list):
            raise ValueError('Tools must be a list')

        tools = []
        for tool in value:
            if not isinstance(tool, str):
                raise ValueError('Tool slugs must be strings')
            tool = tool.strip()
            if not re.fullmatch(r'[A-Z0-9_]+', tool, flags=re.ASCII):
                raise ValueError('Tool slugs must contain only uppercase ASCII letters, digits, and underscores')
            tools.append(tool)

        if len(tools) != len(set(tools)):
            raise ValueError('Duplicate tool slugs are not allowed')
        return tools

    @field_validator('auth_config_id', mode='before')
    @classmethod
    def normalize_auth_config_id(cls, value):
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError('Auth config ID must be a string')
        value = value.strip()
        if not value:
            return None
        if not value.startswith('ac_'):
            raise ValueError('Auth config ID must start with ac_')
        return value


class ComposioPolicy(BaseModel):
    toolkits: dict[str, ComposioToolkitPolicy]

    model_config = ConfigDict(extra='forbid')

    @field_validator('toolkits', mode='before')
    @classmethod
    def normalize_toolkit_slugs(cls, value):
        if not isinstance(value, dict):
            raise ValueError('Toolkits must be an object')

        toolkits = {}
        for toolkit, policy in value.items():
            if not isinstance(toolkit, str):
                raise ValueError('Toolkit slugs must be strings')
            toolkit = toolkit.strip()
            if not re.fullmatch(r'[a-z0-9_-]+', toolkit, flags=re.ASCII):
                raise ValueError('Toolkit slugs must contain only lowercase ASCII letters, digits, underscores, and hyphens')
            if toolkit in toolkits:
                raise ValueError('Duplicate toolkit slugs are not allowed')
            toolkits[toolkit] = policy
        return toolkits

    @model_validator(mode='after')
    def validate_policy(self):
        if not self.toolkits:
            raise ValueError('At least one toolkit is required')
        if sum(len(policy.tools) for policy in self.toolkits.values()) > 1000:
            raise ValueError('A Composio policy may contain at most 1,000 tools')
        return self


_log_scope = ContextVar('composio_log_scope', default=False)
_log_filter_lock = threading.Lock()
_log_filter_installed = False


class _ComposioDependencyLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            _log_scope.get()
            and (
                any(record.name == name or record.name.startswith(f'{name}.') for name in ('httpx', 'httpcore', 'mcp'))
                or record.name == 'open_webui.utils.mcp.client'
            )
        )


def _install_composio_log_filter() -> None:
    global _log_filter_installed
    if _log_filter_installed:
        return
    with _log_filter_lock:
        if _log_filter_installed:
            return
        root = logging.getLogger()
        for handler in root.handlers:
            if not getattr(handler, '_composio_log_filter', False):
                handler.addFilter(_ComposioDependencyLogFilter())
                handler._composio_log_filter = True
        _log_filter_installed = True


@contextmanager
def composio_log_scope():
    _install_composio_log_filter()
    token = _log_scope.set(True)
    try:
        from opentelemetry.instrumentation.utils import suppress_instrumentation
    except ModuleNotFoundError as error:
        if error.name not in (
            'opentelemetry',
            'opentelemetry.instrumentation',
            'opentelemetry.instrumentation.utils',
        ):
            raise
        suppression_scope = nullcontext()
    else:
        suppression_scope = suppress_instrumentation()
    try:
        with suppression_scope:
            yield
    finally:
        _log_scope.reset(token)


def create_composio_httpx_client(headers=None, timeout=None, auth=None):
    kwargs = {'verify': True, 'follow_redirects': False}
    if timeout is not None:
        kwargs['timeout'] = timeout
    else:
        kwargs['timeout'] = (
            float(AIOHTTP_CLIENT_TIMEOUT_TOOL_SERVER) if AIOHTTP_CLIENT_TIMEOUT_TOOL_SERVER is not None else 30.0
        )
    if headers is not None:
        kwargs['headers'] = headers
    if auth is not None:
        kwargs['auth'] = auth
    return httpx.AsyncClient(**kwargs)


def _callback_url(webui_url: str | None) -> str:
    message = 'Configure the WebUI URL before using Composio'
    if not isinstance(webui_url, str) or not webui_url.strip():
        raise ValueError(message)
    url = webui_url.strip()
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        raise ValueError(message) from None
    if (
        parsed.scheme not in ('https', 'http')
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or '?' in url
        or '#' in url
        or any(character.isspace() for character in url)
    ):
        raise ValueError(message)
    if parsed.scheme == 'http':
        try:
            is_loopback = hostname.lower() == 'localhost' or ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            is_loopback = False
        if not is_loopback:
            raise ValueError(message)
    if port is not None and not 0 < port <= 65535:
        raise ValueError(message)
    return url.rstrip('/') + '/'


def _validate_session_url(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError('Composio is unavailable')
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError('Composio is unavailable') from None
    if (
        parsed.scheme != 'https'
        or parsed.hostname != 'backend.composio.dev'
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or '#' in value
        or any(character.isspace() for character in value)
        or port not in (None, 443)
    ):
        raise ValueError('Composio is unavailable')
    return value


def _check_composio_response(response: httpx.Response) -> None:
    if response.status_code in (401, 403):
        raise ValueError('Composio API rejected credentials') from None
    if response.status_code in (400, 422):
        raise ValueError('Composio tool policy was rejected') from None
    if response.status_code < 200 or response.status_code >= 300:
        raise ValueError('Composio is unavailable') from None


async def _resolve_composio_tools(session_id: str, api_key: str, open_toolkits: set[str]) -> set[str]:
    # Membership comes from the bound session catalog, never a tool-name prefix.
    encoded_session_id = quote(session_id, safe='').replace('.', '%2E')
    url = f'https://backend.composio.dev/api/v3.1/tool_router/session/{encoded_session_id}/tools'
    allowed_tools = set()
    seen_slugs = set()
    seen_cursors = set()
    params = {'limit': 500}
    try:
        with composio_log_scope():
            async with create_composio_httpx_client(
                headers={'x-api-key': api_key, 'accept': 'application/json'}
            ) as client:
                while True:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    result = response.json()
                    if not isinstance(result, dict) or not isinstance(result.get('items'), list):
                        raise ValueError
                    items = result['items']
                    if len(items) > 500 or len(seen_slugs) + len(items) > 1000:
                        raise ValueError
                    for item in items:
                        if not isinstance(item, dict):
                            raise ValueError
                        slug = item.get('slug')
                        toolkit = item.get('toolkit')
                        if (
                            not isinstance(slug, str)
                            or not re.fullmatch(r'[A-Z0-9_]+', slug, flags=re.ASCII)
                            or not isinstance(toolkit, dict)
                            or not isinstance(toolkit.get('slug'), str)
                            or not re.fullmatch(r'[a-z0-9_-]+', toolkit['slug'], flags=re.ASCII)
                            or slug in seen_slugs
                        ):
                            raise ValueError
                        seen_slugs.add(slug)
                        if toolkit['slug'] in open_toolkits and not slug.startswith('COMPOSIO_'):
                            allowed_tools.add(slug)
                    cursor = result.get('next_cursor')
                    if cursor is None:
                        return allowed_tools
                    if (
                        not isinstance(cursor, str)
                        or not cursor.strip()
                        or cursor in seen_cursors
                        or not items
                        or len(seen_slugs) >= 1000
                    ):
                        raise ValueError
                    seen_cursors.add(cursor)
                    params['cursor'] = cursor
    except httpx.HTTPStatusError as exc:
        _check_composio_response(exc.response)
        raise ValueError('Composio is unavailable') from None
    except Exception:
        raise ValueError('Composio is unavailable') from None


async def create_composio_session(connection: dict, user_id: str, webui_url: str) -> dict:
    _install_composio_log_filter()
    policy = ComposioPolicy.model_validate(connection.get('composio'))
    api_key = connection.get('key')
    if not isinstance(api_key, str) or not api_key.strip():
        raise ValueError('Composio API key cannot be blank')
    api_key = api_key.strip()
    if not isinstance(user_id, str) or not user_id:
        raise ValueError('Composio user identity is required')
    requested_user_id = f'openwebui:{user_id}'
    callback_url = _callback_url(webui_url)

    toolkits = list(policy.toolkits)
    payload = {
        'user_id': requested_user_id,
        'toolkits': {'enable': toolkits},
        'tools': {
            toolkit: {'enable': toolkit_policy.tools}
            for toolkit, toolkit_policy in policy.toolkits.items()
            if toolkit_policy.tools
        },
        'auth_configs': {
            toolkit: toolkit_policy.auth_config_id
            for toolkit, toolkit_policy in policy.toolkits.items()
            if toolkit_policy.auth_config_id is not None
        },
        'instant': False,
        'manage_connections': {
            'enable': True,
            'callback_url': callback_url,
            'enable_wait_for_connections': False,
            'enable_connection_removal': False,
        },
        'workbench': {'enable': False},
        'proxy_execute': {'enable': False},
        'multi_account': {'enable': False},
        'preload': {'tools': 'all'},
        'search': {'enable': False},
        'execute': {'enable_multi_execute': False},
    }
    timeout = float(AIOHTTP_CLIENT_TIMEOUT_TOOL_SERVER) if AIOHTTP_CLIENT_TIMEOUT_TOOL_SERVER is not None else 30.0

    try:
        with composio_log_scope():
            async with httpx.AsyncClient(
                verify=True,
                follow_redirects=False,
                timeout=timeout,
                headers={'x-api-key': api_key, 'accept': 'application/json', 'content-type': 'application/json'},
            ) as client:
                response = await client.post('https://backend.composio.dev/api/v3.1/tool_router/session', json=payload)
    except Exception:
        raise ValueError('Composio is unavailable') from None

    _check_composio_response(response)

    try:
        result = response.json()
        session_id = result.get('session_id')
        config = result.get('config')
        mcp = result.get('mcp')
        if (
            not isinstance(session_id, str)
            or not session_id.strip()
            or not isinstance(config, dict)
            or config.get('user_id') != requested_user_id
            or not isinstance(mcp, dict)
            or mcp.get('type') != 'http'
        ):
            raise ValueError
        session_url = _validate_session_url(mcp.get('url'))
    except Exception:
        raise ValueError('Composio is unavailable') from None

    allowed_tools = {tool for toolkit_policy in policy.toolkits.values() for tool in toolkit_policy.tools}
    open_toolkits = {toolkit for toolkit, toolkit_policy in policy.toolkits.items() if not toolkit_policy.tools}
    if open_toolkits:
        allowed_tools.update(await _resolve_composio_tools(session_id, api_key, open_toolkits))
        if len(allowed_tools - {'COMPOSIO_MANAGE_CONNECTIONS'}) > 1000:
            raise ValueError('Composio is unavailable')

    return {
        'session_id': session_id,
        'url': session_url,
        'headers': {'x-api-key': api_key},
        'allowed_toolkits': set(policy.toolkits),
        'allowed_tools': allowed_tools | {'COMPOSIO_MANAGE_CONNECTIONS'},
    }


def validate_composio_connection_arguments(arguments: dict, session_id: str, allowed_toolkits: set[str]) -> dict:
    if not isinstance(arguments, dict):
        raise ValueError('Invalid Composio connection arguments')
    if set(arguments) - {'toolkits', 'reinitiate_all', 'session_id'}:
        raise ValueError('Invalid Composio connection arguments')
    toolkits = arguments.get('toolkits')
    if not isinstance(toolkits, list) or not toolkits or any(
        not isinstance(toolkit, str) or toolkit not in allowed_toolkits for toolkit in toolkits
    ):
        raise ValueError('Composio connection request includes a forbidden toolkit')
    if 'reinitiate_all' in arguments and not isinstance(arguments['reinitiate_all'], bool):
        raise ValueError('Invalid Composio connection arguments')
    if 'session_id' in arguments and arguments['session_id'] != session_id:
        raise ValueError('Composio session ID does not match the current session')
    return {**arguments, 'session_id': session_id}
