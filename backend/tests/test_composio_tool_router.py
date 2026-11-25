"""Isolated security contracts for the native Composio tool-router adapter."""

import atexit
import asyncio
import copy
import json
import logging
import os
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

# Open WebUI reads these while importing models/config. Keep every import-time
# database effect out of the operator's real data directory and PostgreSQL.
_TEST_DATA_DIR = tempfile.TemporaryDirectory(prefix='open-webui-composio-tests-')
_TEST_DATA_PATH = Path(_TEST_DATA_DIR.name).resolve()
_TEST_ENV = {
    'DATA_DIR': str(_TEST_DATA_PATH),
    'STATIC_DIR': str(_TEST_DATA_PATH / 'static'),
    'DATABASE_URL': f'sqlite:///{_TEST_DATA_PATH / "test.db"}',
    # Empty overrides also prevent load_dotenv from restoring real credentials.
    'DATABASE_TYPE': '',
    'DATABASE_USER': '',
    'DATABASE_PASSWORD': '',
    'DATABASE_HOST': '',
    'DATABASE_PORT': '',
    'DATABASE_NAME': '',
    'DATABASE_SCHEMA': 'main',  # SQLite schema; an empty string breaks SQLAlchemy table introspection.
    'DATABASE_ENABLE_IAM_TOKEN_AUTH': 'false',
    'ENABLE_DB_MIGRATIONS': 'false',
    'FROM_INIT_PY': 'false',
    'ENV': 'test',
    'WEBUI_AUTH': 'true',
    'ENABLE_TOOL_SERVERS': 'true',
    'ENABLE_TOOLS': 'false',
    'ENABLE_FUNCTIONS': 'false',
    'ERP_MCP_SERVER_ID': 'wilopo-erp',
    'WEBUI_SECRET_KEY': 'test-only-composio-signing-key-never-use-in-production',
    'WEBUI_JWT_SECRET_KEY': '',
    'GLOBAL_LOG_LEVEL': '',
    'CUSTOM_NAME': '',
    'USE_SLIM_DOCKER': 'true',
    'VECTOR_DB': 'chroma',
    'STORAGE_PROVIDER': 'local',
    'CHROMA_HTTP_HOST': '',
    'WEBSOCKET_MANAGER': '',
    'REDIS_URL': '',
    'WEBSOCKET_REDIS_URL': '',
    'OFFLINE_MODE': 'true',
}
_PREVIOUS_ENV = {key: os.environ.get(key) for key in _TEST_ENV}
os.environ.update(_TEST_ENV)


def _restore_test_environment():
    for key, value in _PREVIOUS_ENV.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    _TEST_DATA_DIR.cleanup()


atexit.register(_restore_test_environment)

import httpx
import httpx2
import pytest
import pytest_asyncio
from httpx import AsyncClient as _HTTPX_ASYNC_CLIENT
from pydantic import ValidationError


@pytest.fixture(scope='session')
def app_modules():
    loaded_env = sys.modules.get('open_webui.env')
    if loaded_env is not None and loaded_env.DATABASE_URL != _TEST_ENV['DATABASE_URL']:
        pytest.fail('Run the Composio contract tests in a fresh process; Open WebUI was already configured')
    from open_webui import env
    from open_webui.internal import db
    from open_webui.routers import configs, tools
    from open_webui.utils import composio, composio_files, erp_mcp, middleware
    from open_webui.utils import tools as tool_utils
    from open_webui.models.config import Config
    from open_webui.models.models import Model
    from open_webui.models.oauth_sessions import OAuthSession

    assert env.DATA_DIR.resolve() == _TEST_DATA_PATH
    assert env.DATABASE_URL == _TEST_ENV['DATABASE_URL']
    assert env.WEBUI_AUTH is True
    assert env.WEBUI_SECRET_KEY == _TEST_ENV['WEBUI_SECRET_KEY']
    assert db.engine.url.get_backend_name() == 'sqlite'
    assert db.async_engine.url.get_backend_name() == 'sqlite'
    assert Path(db.engine.url.database).resolve() == _TEST_DATA_PATH / 'test.db'
    assert Path(db.async_engine.url.database).resolve() == _TEST_DATA_PATH / 'test.db'
    try:
        # Migrations are disabled: the normal chat overlay reads these tables,
        # but this Composio fixture intentionally has no subscription accounts or presets.
        Config.__table__.create(db.engine, checkfirst=True)
        OAuthSession.__table__.create(db.engine, checkfirst=True)
        Model.__table__.create(db.engine, checkfirst=True)
        yield SimpleNamespace(
            configs=configs,
            composio=composio,
            composio_files=composio_files,
            erp_mcp=erp_mcp,
            middleware=middleware,
            tools=tools,
            tool_utils=tool_utils,
        )
    finally:
        db.engine.dispose()
        _restore_test_environment()


def _connection(toolkits=None, *, server_id='composio', enabled=True, grants=None):
    return {
        'type': 'composio',
        'url': 'https://backend.composio.dev/api/v3.1',
        'path': 'tool_router/session',
        'auth_type': 'none',
        'key': 'test-project-key',
        'composio': {
            'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE']}} if toolkits is None else toolkits
        },
        'config': {'enable': enabled, 'access_grants': grants or []},
        'info': {
            'id': server_id,
            'name': 'Composio',
            'description': 'Connect a work account.',
        },
    }


def _user(user_id, role='user'):
    from open_webui.models.users import UserModel

    return UserModel(
        id=user_id,
        email=f'{user_id}@example.test',
        name=user_id,
        role=role,
        last_active_at=0,
        updated_at=0,
        created_at=0,
    )


def _install_composio_transport(monkeypatch, composio, handler):
    def async_client(*args, **kwargs):
        kwargs['transport'] = httpx.MockTransport(handler)
        return _HTTPX_ASYNC_CLIENT(*args, **kwargs)

    monkeypatch.setattr(composio.httpx, 'AsyncClient', async_client)


def _session_response(user_id, *, url='https://backend.composio.dev/mcp/session'):
    return {
        'session_id': f'session-{user_id}',
        'config': {'user_id': f'openwebui:{user_id}'},
        'mcp': {'type': 'http', 'url': url},
    }


@pytest.mark.asyncio
async def test_composio_dependency_logs_are_scoped_and_native_logs_survive(app_modules, caplog):
    caplog.set_level(logging.DEBUG)
    composio = app_modules.composio
    private_url = 'https://backend.composio.dev/mcp/session/private-user-session'
    project_key = 'private-project-key-for-log-test'
    composio_started = asyncio.Event()
    native_finished = asyncio.Event()

    async def composio_transport_logs():
        with composio.composio_log_scope():
            logging.getLogger('httpx').info('HTTP request to %s with %s', private_url, project_key)
            logging.getLogger('httpcore.connection').debug('transport %s', private_url)
            logging.getLogger('httpx2').info('MCP HTTP request to %s with %s', private_url, project_key)
            logging.getLogger('httpcore2.connection').debug('MCP transport %s', private_url)
            try:
                raise RuntimeError(f'{private_url} {project_key}')
            except RuntimeError:
                logging.getLogger('mcp.client.streamable_http').exception('MCP transport failed')
                logging.getLogger('httpx').debug('request failed', exc_info=True)
            logging.getLogger('open_webui.utils.composio').warning('safe Composio diagnostic')
            composio_started.set()
            await native_finished.wait()
        logging.getLogger('httpcore').debug('native logging after Composio scope reset')

    async def native_transport_logs():
        await composio_started.wait()
        logging.getLogger('httpx').info('native HTTP request to https://native.example/mcp')
        logging.getLogger('httpx2').info('native MCP HTTP request to https://native.example/mcp')
        logging.getLogger('httpcore2.connection').debug('native MCP transport logging')
        logging.getLogger('mcp.client.streamable_http').info('native MCP transport connected')
        native_finished.set()

    await asyncio.gather(composio_transport_logs(), native_transport_logs())
    assert 'native.example' in caplog.text
    assert 'native MCP HTTP request' in caplog.text
    assert 'native MCP transport logging' in caplog.text
    assert 'native MCP transport connected' in caplog.text
    assert 'native logging after Composio scope reset' in caplog.text
    assert 'safe Composio diagnostic' in caplog.text
    # Formatted text includes exception tracebacks, unlike getMessage().
    assert private_url not in caplog.text
    assert project_key not in caplog.text
    assert not any(record.exc_info for record in caplog.records)


@pytest.mark.parametrize(
    'policy',
    [
        {},
        {'toolkits': {'googledrive': {}}},
        {'toolkits': {'googledrive': {'tools': None}}},
        {'toolkits': {'googledrive': {'tools': {}}}},
        {'toolkits': {'googledrive': {'tools': [None]}}},
        {'toolkits': {'googledrive': {'tools': 'GOOGLEDRIVE_FIND_FILE'}}},
        {'toolkits': {'googledrive': {'tools': ['']}}},
        {'toolkits': {'googledrive': {'tools': ['googledrive_find_file']}}},
        {'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_FÍND_FILE']}}},
        {'toolkits': {'googlédrive': {'tools': ['GOOGLEDRIVE_FIND_FILE']}}},
        {'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE'], 'auth_config_id': 'shared-auth'}}},
        {'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE'], 'extra': True}}},
        {'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE']}}, 'extra': True},
        {
            'toolkits': {
                'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE']},
                ' googledrive ': {'tools': ['GOOGLEDRIVE_GET_ABOUT']},
            }
        },
        {'toolkits': {'googledrive': {'tools': [f'GOOGLEDRIVE_TOOL_{index}' for index in range(1001)]}}},
        {'toolkits': {}},
        {'toolkits': {'googledrive': {'tools': ['*']}}},
        {'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_*']}}},
        {'toolkits': {'GoogleDrive': {'tools': ['GOOGLEDRIVE_FIND_FILE']}}},
        {'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE', ' GOOGLEDRIVE_FIND_FILE ']}}},
    ],
)
def test_policy_rejects_missing_malformed_wildcard_and_duplicate_allowlists(app_modules, policy):
    with pytest.raises(ValidationError):
        app_modules.composio.ComposioPolicy.model_validate(policy)


@pytest.mark.parametrize(
    'auth_fields', [{}, {'auth_config_id': None}, {'auth_config_id': '  '}, {'auth_config_id': ' ac_drive_read '}]
)
def test_empty_tools_allow_open_toolkit_without_changing_optional_auth_config(app_modules, auth_fields):
    policy = app_modules.composio.ComposioPolicy.model_validate(
        {
            'toolkits': {
                'googledrive': {'tools': [], **auth_fields},
                'notion': {'tools': [' NOTION_SEARCH ']},
            },
        }
    )
    assert policy.toolkits['googledrive'].tools == []
    assert policy.toolkits['googledrive'].auth_config_id == ((auth_fields.get('auth_config_id') or '').strip() or None)
    assert policy.toolkits['notion'].tools == ['NOTION_SEARCH']


def test_composio_connection_validation_is_type_specific(app_modules):
    valid = _connection()
    parsed = app_modules.configs.ToolServerConnection.model_validate(valid)
    assert parsed.type == 'composio'
    assert parsed.composio.toolkits['googledrive'].tools == ['GOOGLEDRIVE_FIND_FILE']

    for field, value in (
        ('url', 'https://attacker.example/collect'),
        ('path', 'custom/path'),
        ('auth_type', 'bearer'),
        ('forward_cookies', True),
        ('headers', {'x-forwarded': 'value'}),
        ('key', '   '),
        ('composio', {'toolkits': {}}),
        ('composio', None),
    ):
        invalid = {**valid, field: value}
        with pytest.raises(ValidationError):
            app_modules.configs.ToolServerConnection.model_validate(invalid)

    for server_id in ('', 'comp:one', 'comp|one'):
        invalid = _connection(server_id=server_id)
        with pytest.raises(ValidationError):
            app_modules.configs.ToolServerConnection.model_validate(invalid)

    invalid_disabled = _connection(toolkits={}, enabled=False)
    with pytest.raises(ValidationError):
        app_modules.configs.ToolServerConnection.model_validate(invalid_disabled)

    native = app_modules.configs.ToolServerConnection.model_validate(
        {
            'type': 'mcp',
            'url': 'https://native.example/mcp',
            'path': '',
            'auth_type': 'none',
            'key': None,
            'config': {},
            'info': {'id': 'native'},
        }
    )
    assert native.type == 'mcp'
    assert native.composio is None


@pytest_asyncio.fixture
async def config_import_client(app_modules, monkeypatch):
    from fastapi import FastAPI
    from open_webui import events
    from open_webui.internal import db

    Config = app_modules.configs.Config
    Config.__table__.create(db.engine, checkfirst=True)
    monkeypatch.setattr(Config, 'PERSISTENT_ENABLED', True)
    monkeypatch.setattr(Config, 'OAUTH_PERSISTENT_ENABLED', False)
    monkeypatch.setattr(Config, 'DEFAULTS', copy.deepcopy(Config.DEFAULTS))
    saved = await Config.get_all()
    # Import notifications/plugins are outside this persistence regression.
    monkeypatch.setattr(events, 'EVENT_SINKS', [])
    app = FastAPI()
    app.include_router(app_modules.configs.router, prefix='/configs')
    app.dependency_overrides[app_modules.configs.get_admin_user] = lambda: _user('admin', role='admin')
    try:
        async with _HTTPX_ASYNC_CLIENT(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            yield client
    finally:
        await Config.clear()
        await Config.upsert(saved)
        await db.async_engine.dispose()


@pytest.mark.parametrize(
    'invalid_fields',
    [
        pytest.param({}, id='missing-policy'),
        {'composio': None},
        {'composio': {'toolkits': {}}},
        {'composio': {'toolkits': {'googledrive': {}}}},
        {'composio': {'toolkits': {'googledrive': {'tools': None}}}},
        {'composio': {'toolkits': {'googledrive': {'tools': 'GOOGLEDRIVE_FIND_FILE'}}}},
        {'composio': {'toolkits': {'googledrive': {'tools': ['']}}}},
        {'composio': {'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_*']}}}},
        {'composio': {'toolkits': {'private-provider-value': {'tools': ['private-tool-value']}}}},
        {'composio': {'toolkits': {}}, 'config': {'enable': False}},
        {'url': 'https://private-provider.example/secret-path'},
        {'key': '   '},
    ],
)
@pytest.mark.asyncio
async def test_full_config_import_rejects_invalid_composio_without_changing_prior_state(
    app_modules, config_import_client, invalid_fields
):
    Config = app_modules.configs.Config
    previous_connection = _connection(server_id='previous')
    await Config.upsert(
        {
            'tool_server.connections': [previous_connection],
            'ui.default_models': 'previous-model',
            'import_test.unrelated': {'keep': ['previous-value']},
            'oauth.import_test_guard': 'previous-default',
        }
    )
    before = await Config.get_all()
    before_defaults = copy.deepcopy(Config.DEFAULTS)
    invalid = {**_connection(), **invalid_fields, 'key': invalid_fields.get('key', 'private-import-api-key')}
    if not invalid_fields:
        invalid.pop('composio')
    response = await config_import_client.post(
        '/configs/import',
        json={
            'config': {
                'ui.default_models': 'replacement-model',
                'auth.mfa.enable': True,
                'oauth.import_test_guard': 'replacement-default',
                'import_test.unrelated': {'replace': True},
                'import_test.new': 'must-not-be-persisted',
                'tool_server.connections': [_connection(server_id='valid-first'), invalid],
            }
        },
    )
    assert response.status_code == 400
    for private_value in (
        'private-import-api-key',
        'private-provider-value',
        'private-tool-value',
        'private-provider.example',
        'secret-path',
    ):
        assert private_value not in response.text
    assert await Config.get_all() == before
    assert Config.DEFAULTS == before_defaults
    readable = await config_import_client.get('/configs/tool_servers')
    assert readable.status_code == 200
    assert readable.json()['TOOL_SERVER_CONNECTIONS'][0]['info']['id'] == 'previous'


@pytest.mark.parametrize('connection_kind', ['composio', 'composio-open', 'native', 'empty', 'omitted'])
@pytest.mark.asyncio
async def test_full_config_import_accepts_valid_connections_and_keeps_them_readable(
    app_modules, config_import_client, connection_kind
):
    Config = app_modules.configs.Config
    await Config.upsert(
        {
            'tool_server.connections': [_connection(server_id='previous')],
            'import_test.retained': {'keep': True},
        }
    )
    native = {**_native_connection(), 'legacy_extension': {'preserve': True}}
    imported = {'ui.default_models': 'imported-model'}
    if connection_kind in ('composio', 'composio-open'):
        connection = _connection(
            {
                ' googledrive ': {
                    'tools': [] if connection_kind == 'composio-open' else [' GOOGLEDRIVE_FIND_FILE '],
                    'auth_config_id': ' ac_approved_read_only ',
                },
                'github': {'tools': ['GITHUB_GET_THE_AUTHENTICATED_USER']},
            },
            server_id=' imported-composio ',
            grants=_user_read_grant('alice'),
        )
        connection['key'] = ' imported-project-key '
        imported['tool_server.connections'] = [native, connection]
    elif connection_kind == 'native':
        imported['tool_server.connections'] = [native]
    elif connection_kind == 'empty':
        imported['tool_server.connections'] = []

    response = await config_import_client.post('/configs/import', json={'config': imported})
    assert response.status_code == 200
    assert response.json()['ui.default_models'] == 'imported-model'
    assert await Config.get('import_test.retained') == {'keep': True}
    readable = await config_import_client.get('/configs/tool_servers')
    assert readable.status_code == 200
    stored = await Config.get('tool_server.connections')
    assert response.json()['tool_server.connections'] == stored
    if connection_kind in ('composio', 'composio-open', 'native'):
        assert stored[0] == native
        assert readable.json()['TOOL_SERVER_CONNECTIONS'][0]['legacy_extension'] == {'preserve': True}
    if connection_kind in ('composio', 'composio-open'):
        assert stored[1]['info']['id'] == 'imported-composio'
        assert stored[1]['key'] == 'imported-project-key'
        assert stored[1]['composio']['toolkits']['googledrive'] == {
            'tools': [] if connection_kind == 'composio-open' else ['GOOGLEDRIVE_FIND_FILE'],
            'auth_config_id': 'ac_approved_read_only',
        }
        assert readable.json()['TOOL_SERVER_CONNECTIONS'][1] == stored[1]
    elif connection_kind == 'empty':
        assert readable.json()['TOOL_SERVER_CONNECTIONS'] == []
    elif connection_kind == 'omitted':
        assert readable.json()['TOOL_SERVER_CONNECTIONS'][0]['info']['id'] == 'previous'


@pytest.mark.asyncio
async def test_session_uses_exact_per_user_tool_router_contract(app_modules, monkeypatch):
    composio = app_modules.composio
    connection = _connection(
        {
            'googledrive': {
                'tools': [' GOOGLEDRIVE_FIND_FILE ', 'GOOGLEDRIVE_GET_FILE_METADATA'],
                'auth_config_id': ' ac_drive_read ',
            },
            'notion': {'tools': ['NOTION_SEARCH']},
        }
    )
    # These must never be forwarded, even if hostile extra fields reach the adapter.
    connection['headers'] = {'Authorization': 'bad-header', 'x-other': 'bad-header'}
    connection['cookies'] = {'session': 'bad-cookie'}
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=_session_response('alice'), request=request)

    _install_composio_transport(monkeypatch, composio, handler)
    session = await composio.create_composio_session(connection, 'alice', 'https://webui.example.test/admin')

    assert len(seen) == 1
    request = seen[0]
    assert request.method == 'POST'
    assert str(request.url) == 'https://backend.composio.dev/api/v3.1/tool_router/session'
    assert request.headers['x-api-key'] == 'test-project-key'
    assert request.headers['content-type'].startswith('application/json')
    assert 'authorization' not in request.headers and 'cookie' not in request.headers
    assert 'x-other' not in request.headers
    body = json.loads(request.content)
    assert body == {
        'user_id': 'openwebui:alice',
        'toolkits': {'enable': ['googledrive', 'notion']},
        'tools': {
            'googledrive': {'enable': ['GOOGLEDRIVE_FIND_FILE', 'GOOGLEDRIVE_GET_FILE_METADATA']},
            'notion': {'enable': ['NOTION_SEARCH']},
        },
        'auth_configs': {'googledrive': 'ac_drive_read'},
        'instant': False,
        'manage_connections': {
            'enable': True,
            'callback_url': 'https://webui.example.test/admin/',
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
    assert session == {
        'session_id': 'session-alice',
        'url': 'https://backend.composio.dev/mcp/session',
        'headers': {'x-api-key': 'test-project-key'},
        'allowed_toolkits': {'googledrive', 'notion'},
        'allowed_tools': {
            'GOOGLEDRIVE_FIND_FILE',
            'GOOGLEDRIVE_GET_FILE_METADATA',
            'NOTION_SEARCH',
            'COMPOSIO_MANAGE_CONNECTIONS',
        },
    }


@pytest.mark.parametrize(
    ('status', 'message'),
    [
        (401, 'Composio API rejected credentials'),
        (403, 'Composio API rejected credentials'),
        (400, 'Composio tool policy was rejected'),
        (422, 'Composio tool policy was rejected'),
        (500, 'Composio is unavailable'),
        (302, 'Composio is unavailable'),
        (429, 'Composio is unavailable'),
    ],
)
@pytest.mark.asyncio
async def test_provider_errors_are_safe_and_never_retry(app_modules, monkeypatch, status, message):
    composio = app_modules.composio
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={'message': 'raw provider detail and secret'}, request=request)

    _install_composio_transport(monkeypatch, composio, handler)
    with pytest.raises(ValueError, match=message) as error:
        await composio.create_composio_session(_connection(), 'alice', 'https://webui.example.test')
    assert len(calls) == 1
    assert 'raw provider detail' not in str(error.value)
    assert 'test-project-key' not in str(error.value)


@pytest.mark.parametrize(
    'response',
    [
        {**_session_response('bob')},
        None,
        [],
        {},
        {**_session_response('alice'), 'session_id': 123},
        {**_session_response('alice'), 'mcp': {'type': 'sse', 'url': 'https://backend.composio.dev/mcp'}},
        {**_session_response('alice', url='http://backend.composio.dev/mcp')},
        {**_session_response('alice', url='https://evil.example/mcp')},
        {**_session_response('alice', url='https://user@backend.composio.dev/mcp')},
        {**_session_response('alice', url='https://backend.composio.dev:444/mcp')},
        {**_session_response('alice', url='https://backend.composio.dev/mcp#fragment')},
        {
            'session_id': ' ',
            'config': {'user_id': 'openwebui:alice'},
            'mcp': {'type': 'http', 'url': 'https://backend.composio.dev/mcp'},
        },
    ],
)
@pytest.mark.asyncio
async def test_mismatched_or_unsafe_session_responses_fail_before_mcp_connection(app_modules, monkeypatch, response):
    client_class = _in_process_mcp_client_class()
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=response, request=request)

    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    connection = _connection(server_id='shared', grants=_user_read_grant('alice'))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('alice'), ['server:composio:shared'], client_class=client_class
    )
    with pytest.raises(ValueError, match='^Composio is unavailable$'):
        await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    assert len(seen) == 1
    assert client_class.instances == []


@pytest.mark.asyncio
async def test_callback_url_is_required_and_restricted_to_secure_or_loopback_origins(app_modules, monkeypatch):
    composio = app_modules.composio
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=_session_response('alice'), request=request)

    _install_composio_transport(monkeypatch, composio, handler)
    for url in (
        None,
        '',
        '/relative',
        'http://webui.example.test',
        'https://user@webui.example.test',
        'https://webui.example.test/?q=1',
        'https://webui.example.test/#anchor',
        'https://webui.example.test/?',
    ):
        with pytest.raises(ValueError, match='Configure the WebUI URL before using Composio'):
            await composio.create_composio_session(_connection(), 'alice', url)
    assert calls == []

    for url in (
        'http://127.0.0.1:3000',
        'http://localhost:3000/',
        'http://[::1]:3000',
        'https://webui.example.test/path///',
    ):
        await composio.create_composio_session(_connection(), 'alice', url)
        assert json.loads(calls[-1].content)['manage_connections']['callback_url'] == url.rstrip('/') + '/'
    assert len(calls) == 4


@pytest.mark.parametrize('factory_name', ['create_composio_httpx_client', 'create_composio_mcp_httpx_client'])
@pytest.mark.asyncio
async def test_composio_http_factory_does_not_follow_real_local_redirect(app_modules, factory_name):
    class RedirectHandler(BaseHTTPRequestHandler):
        destination = ''
        destination_hits = 0
        received_key = None

        def do_GET(self):
            if self.path == '/redirect':
                type(self).received_key = self.headers.get('x-api-key')
                self.send_response(302)
                self.send_header('Location', self.destination)
                self.end_headers()
            elif self.path == '/destination':
                type(self).destination_hits += 1
                self.send_response(200)
                self.end_headers()

        def log_message(self, _format, *_args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), RedirectHandler)
    RedirectHandler.destination = f'http://127.0.0.1:{server.server_port}/destination'
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        async with getattr(app_modules.composio, factory_name)(
            headers={'x-api-key': 'test-project-key'}
        ) as client:
            response = await client.get(f'http://127.0.0.1:{server.server_port}/redirect')
        assert response.status_code == 302
        assert RedirectHandler.destination_hits == 0
        assert RedirectHandler.received_key == 'test-project-key'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize(
    'arguments',
    [
        {},
        {'toolkits': []},
        {'toolkits': 'googledrive'},
        {'toolkits': ['googledrive', 'notion']},
        {'toolkits': [None]},
        {'toolkits': ['notion']},
        {'toolkits': ['googledrive'], 'unexpected': 'field'},
        {'toolkits': ['googledrive'], 'reinitiate_all': 'yes'},
        {'toolkits': ['googledrive'], 'session_id': 'foreign-session'},
    ],
)
def test_connection_manager_arguments_are_allowlisted_and_session_bound(app_modules, arguments):
    with pytest.raises(ValueError):
        app_modules.composio.validate_composio_connection_arguments(
            arguments,
            'current-session',
            {'googledrive'},
        )


def test_connection_manager_arguments_bind_only_current_session(app_modules):
    assert app_modules.composio.validate_composio_connection_arguments(
        {'toolkits': ['googledrive'], 'reinitiate_all': False},
        'current-session',
        {'googledrive'},
    ) == {
        'toolkits': ['googledrive'],
        'reinitiate_all': False,
        'session_id': 'current-session',
    }


def test_policy_normalizes_inputs_and_accepts_exact_maximum(app_modules):
    policy = app_modules.composio.ComposioPolicy.model_validate(
        {'toolkits': {' googledrive ': {'tools': [' GOOGLEDRIVE_FIND_FILE '], 'auth_config_id': '  '}}}
    )
    assert policy.model_dump() == {
        'toolkits': {'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE'], 'auth_config_id': None}}
    }
    maximum = app_modules.composio.ComposioPolicy.model_validate(
        {'toolkits': {'googledrive': {'tools': [f'GOOGLEDRIVE_TOOL_{index}' for index in range(1000)]}}}
    )
    assert len(maximum.toolkits['googledrive'].tools) == 1000


@pytest.mark.asyncio
async def test_native_mcp_verification_still_lists_and_disconnects(app_modules, monkeypatch):
    class NativeMCPClient:
        instances = []

        def __init__(self):
            self.connected = None
            self.closed = False
            self.instances.append(self)

        async def connect(self, url, headers=None):
            self.connected = (url, headers)

        async def list_tool_specs(self):
            return [{'name': 'native_search', 'description': 'Native tool', 'parameters': {'type': 'object'}}]

        async def disconnect(self):
            self.closed = True

    monkeypatch.setattr(app_modules.configs, 'MCPClient', NativeMCPClient)
    connection = app_modules.configs.ToolServerConnection.model_validate(
        {
            'type': 'mcp',
            'url': 'https://native.example/mcp',
            'path': '',
            'auth_type': 'none',
            'key': None,
            'headers': None,
            'config': {},
            'info': {'id': 'native'},
        }
    )
    result = await app_modules.configs.verify_tool_servers_config(
        SimpleNamespace(state=SimpleNamespace()),
        connection,
        SimpleNamespace(id='admin', role='admin'),
    )
    assert result == {
        'status': True,
        'specs': [{'name': 'native_search', 'description': 'Native tool', 'parameters': {'type': 'object'}}],
    }
    assert NativeMCPClient.instances[0].connected == ('https://native.example/mcp', None)
    assert NativeMCPClient.instances[0].closed is True


_APP_TOOL_SPECS = [
    {
        'name': 'GOOGLEDRIVE_FIND_FILE',
        'description': 'Find Drive files.',
        'parameters': {'type': 'object', 'properties': {'query': {'type': 'string'}}},
    },
    {
        'name': 'GOOGLEDRIVE_GET_FILE_METADATA',
        'description': 'Read Drive metadata.',
        'parameters': {'type': 'object', 'properties': {'file_id': {'type': 'string'}}},
    },
    {
        'name': 'GOOGLEDRIVE_WRITE_FILE',
        'description': 'Write a Drive file.',
        'parameters': {'type': 'object', 'properties': {'file_id': {'type': 'string'}}},
    },
    {
        'name': 'NOTION_SEARCH',
        'description': 'Search Notion.',
        'parameters': {'type': 'object', 'properties': {'query': {'type': 'string'}}},
    },
    {
        'name': 'COMPOSIO_SEARCH_TOOLS',
        'description': 'Provider helper that is never allowlisted by default.',
        'parameters': {'type': 'object', 'properties': {}},
    },
    {
        'name': 'COMPOSIO_MANAGE_CONNECTIONS',
        'description': 'Connect or inspect a work account.',
        'parameters': {
            'type': 'object',
            'properties': {
                'toolkits': {'type': 'array', 'items': {'type': 'string'}},
                'reinitiate_all': {'type': 'boolean'},
                'session_id': {'type': 'string'},
            },
            'required': ['toolkits'],
        },
    },
]


# Deliberately independent of policy/MCP names: only provider toolkit metadata
# establishes membership, and a toolkit's slug need not prefix its app tools.
_OPEN_TOOL_CATALOG = [
    {'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': {'slug': 'googledrive'}},
    {'slug': 'GOOGLEDRIVE_GET_FILE_METADATA', 'toolkit': {'slug': 'googledrive'}},
    {'slug': 'GOOGLEDRIVE_WRITE_FILE', 'toolkit': {'slug': 'googledrive'}},
    {'slug': 'WORKSPACE_FIND_FILE', 'toolkit': {'slug': 'googledrive'}},
    {'slug': 'NOTION_SEARCH', 'toolkit': {'slug': 'notion'}},
    {'slug': 'NOTION_DELETE_PAGE', 'toolkit': {'slug': 'notion'}},
    {'slug': 'GOOGLEDRIVE_FOREIGN', 'toolkit': {'slug': 'github'}},
    {'slug': 'GITHUB_PRIVATE_TOOL', 'toolkit': {'slug': 'github'}},
    {'slug': 'COMPOSIO_SEARCH_TOOLS', 'toolkit': {'slug': 'googledrive'}},
    {'slug': 'COMPOSIO_PROXY_EXECUTE', 'toolkit': {'slug': 'googledrive'}},
    {'slug': 'COMPOSIO_MANAGE_CONNECTIONS', 'toolkit': {'slug': 'googledrive'}},
]
_OPEN_TOOL_SPECS = _APP_TOOL_SPECS + [
    {'name': slug, 'description': slug, 'parameters': {'type': 'object', 'properties': {}}}
    for slug in (
        'WORKSPACE_FIND_FILE',
        'NOTION_DELETE_PAGE',
        'GOOGLEDRIVE_FOREIGN',
        'GITHUB_PRIVATE_TOOL',
        'GOOGLEDRIVE_UNKNOWN_MCP_ONLY',
        'COMPOSIO_PROXY_EXECUTE',
    )
]


_PRIVATE_DOCUMENTS = {
    'alice': [
        {'id': 'alice-board-budget', 'title': 'shared document', 'owner': 'alice', 'content': 'ALICE-PRIVATE-BUDGET'}
    ],
    'bob': [{'id': 'bob-work-goals', 'title': 'shared document', 'owner': 'bob', 'content': 'BOB-PRIVATE-GOALS'}],
}


def _in_process_mcp_client_class(
    *, app_specs=None, native_specs=None, fail_connect=False, fail_listing=False, consent_status='active'
):
    """Contract provider: opaque issued sessions enforce access to stored documents."""

    class InProcessMCPClient:
        instances = []
        sessions = {}
        disconnect_order = []

        def __init__(self, event_caller=None, server_name=''):
            self.url = None
            self.headers = None
            self.account_id = None
            self.session_id = None
            self.calls = []
            self.closed = False
            self.instructions = None
            self.event_caller = event_caller
            self.server_name = server_name
            self.instances.append(self)

        async def connect(self, url, headers=None, auth=None, *, httpx_client_factory=None):
            self.url = url
            self.headers = headers or {}
            self.auth = auth
            if url == 'https://native.example/mcp':
                self.account_id = 'native'
            else:
                # Routing depends on the server's issued session, never tool kwargs.
                bound = self.sessions[url]
                assert self.headers == {'x-api-key': 'test-project-key'}
                self.account_id = bound['account_id']
                self.session_id = bound['session_id']
            if fail_connect:
                raise RuntimeError(f'private connect failure at {url} with {self.headers}')

        async def list_tool_specs(self):
            if fail_listing:
                raise RuntimeError(f'private listing failure at {self.url} with {self.headers}')
            specs = (
                native_specs
                if self.account_id == 'native' and native_specs is not None
                else (app_specs if app_specs is not None else _APP_TOOL_SPECS)
            )
            self.tool_definitions = {spec['name']: copy.deepcopy(spec) for spec in specs}
            return copy.deepcopy(specs)

        async def call_tool(self, function_name, function_args):
            self.calls.append((function_name, copy.deepcopy(function_args)))
            if self.account_id == 'native':
                return {'provider': 'native', 'tool': function_name}
            bound = self.sessions[self.url]
            if function_name == 'COMPOSIO_MANAGE_CONNECTIONS':
                assert function_args['session_id'] == self.session_id
                return {
                    'status': consent_status,
                    'connect_link': bound['connect_link'],
                }
            if consent_status != 'active':
                raise RuntimeError(f'External account consent is not complete at {self.url}')
            documents = _PRIVATE_DOCUMENTS.get(self.account_id, [])
            if function_name in ('GOOGLEDRIVE_FIND_FILE', 'WORKSPACE_FIND_FILE'):
                query = function_args.get('query', '').lower()
                return {
                    'files': copy.deepcopy(
                        [
                            document
                            for document in documents
                            if query in document['title'].lower() or query in document['content'].lower()
                        ]
                    )
                }
            if function_name == 'GOOGLEDRIVE_GET_FILE_METADATA':
                document = next(
                    (document for document in documents if document['id'] == function_args.get('file_id')), None
                )
                return copy.deepcopy(document) if document else {'error': 'File not found'}
            if function_name == 'GOOGLEDRIVE_WRITE_FILE':
                return {'written_by': self.account_id, 'file_id': function_args.get('file_id')}
            if function_name == 'NOTION_SEARCH':
                return {'pages': [{'id': 'work-plan', 'title': 'plan'}]}
            raise AssertionError(f'Unexpected provider tool: {function_name}')

        async def disconnect(self):
            self.disconnect_order.append(self)
            self.closed = True

    return InProcessMCPClient


def _install_user_bound_sessions(monkeypatch, app_modules, client_class, *, catalog_pages=None, catalog_requests=None):
    requests = []
    catalog_requests = [] if catalog_requests is None else catalog_requests
    catalog_pages = (
        {None: {'items': _OPEN_TOOL_CATALOG, 'next_cursor': None}} if catalog_pages is None else catalog_pages
    )

    def handler(request):
        assert request.url.host == 'backend.composio.dev'
        assert request.headers['x-api-key'] == 'test-project-key'
        assert 'authorization' not in request.headers and 'cookie' not in request.headers
        if request.method == 'GET':
            catalog_requests.append(request)
            assert request.url.params['limit'] == '500'
            session_ids = {session['session_id'] for session in client_class.sessions.values()}
            assert request.url.path in {
                f'/api/v3.1/tool_router/session/{session_id}/tools' for session_id in session_ids
            }
            cursor = request.url.params.get('cursor')
            assert set(request.url.params) == ({'limit', 'cursor'} if cursor is not None else {'limit'})
            assert cursor in catalog_pages
            return httpx.Response(200, json=catalog_pages[cursor], request=request)
        assert request.method == 'POST'
        assert request.url.path == '/api/v3.1/tool_router/session'
        requests.append(request)
        payload = json.loads(request.content)
        user_id = payload['user_id'].removeprefix('openwebui:')
        opaque_id = f'issued-{len(requests)}'
        url = f'https://backend.composio.dev/mcp/{opaque_id}'
        session_id = f'session-{opaque_id}'
        client_class.sessions[url] = {
            'account_id': user_id,
            'session_id': session_id,
            'connect_link': f'https://connect.example.test/{opaque_id}',
        }
        return httpx.Response(
            200,
            json={
                'session_id': session_id,
                'config': {'user_id': payload['user_id']},
                'mcp': {'type': 'http', 'url': url},
            },
            request=request,
        )

    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    return requests


def _prepare_chat_payload(
    monkeypatch, app_modules, connections, user, tool_ids, *, hostile_metadata=None, client_class
):
    from open_webui.config import DEFAULT_USER_PERMISSIONS

    middleware = app_modules.middleware

    async def config_get(key, default=None):
        values = {
            'tool_server.connections': connections,
            'webui.url': 'https://webui.example.test',
            'terminal_server.connections': [],
            'user.permissions': copy.deepcopy(DEFAULT_USER_PERMISSIONS),
        }
        return values.get(key, default)

    async def return_form(_request, form_data, *_args, **_kwargs):
        return form_data

    async def no_result(*_args, **_kwargs):
        return None

    async def no_groups(*_args, **_kwargs):
        return []

    from open_webui.models.groups import Groups

    monkeypatch.setattr(middleware.Config, 'get', staticmethod(config_get))
    monkeypatch.setattr(middleware, 'ENABLE_FUNCTIONS', False)
    monkeypatch.setattr(middleware, 'MCPClient', client_class)
    monkeypatch.setattr(app_modules.tool_utils, 'MCPClient', client_class)
    monkeypatch.setattr(middleware, 'apply_params_to_form_data', lambda form, _model: form)
    monkeypatch.setattr(middleware, 'process_pipeline_inlet_filter', return_form)
    monkeypatch.setattr(middleware, 'get_event_emitter', no_result)
    monkeypatch.setattr(middleware, 'get_event_call', no_result)
    monkeypatch.setattr(middleware, 'get_system_oauth_token', no_result)
    monkeypatch.setattr(middleware, 'get_task_model_id', lambda model_id, *_args: model_id)
    monkeypatch.setattr(Groups, 'get_groups_by_member_id', staticmethod(no_groups))

    model = {
        'id': 'test-model',
        'info': {'meta': {'capabilities': {'file_context': False, 'builtin_tools': False}}},
        'owned_by': 'openai',
    }
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(MODELS={'test-model': model})),
        state=SimpleNamespace(direct=False),
        cookies={'operator_cookie': 'not-for-composio'},
        headers={'host': 'attacker.example', 'authorization': 'Bearer browser-token'},
    )
    form_data = {
        'model': 'test-model',
        'messages': [{'role': 'user', 'content': 'Find a work document.'}],
        'tool_ids': tool_ids,
        'params': {'function_calling': 'native'},
    }
    metadata = {'chat_id': '', 'params': {'function_calling': 'native'}, **(hostile_metadata or {})}
    return request, form_data, metadata, model


async def _process_chat_with_servers(
    monkeypatch, app_modules, connections, user, tool_ids, *, hostile_metadata=None, client_class=None
):
    client_class = client_class or _in_process_mcp_client_class()
    request, form_data, metadata, model = _prepare_chat_payload(
        monkeypatch,
        app_modules,
        connections,
        user,
        tool_ids,
        hostile_metadata=hostile_metadata,
        client_class=client_class,
    )
    form_data, metadata, _events, _pending = await app_modules.middleware.process_chat_payload(
        request,
        form_data,
        user,
        metadata,
        model,
    )
    return request, form_data, metadata, client_class


def _composio_function_name(server_id, tool_slug):
    import hashlib

    digest = hashlib.sha256(f'{server_id}:{tool_slug}'.encode()).hexdigest()[:24]
    return f'comp_{digest}'


def _user_read_grant(user_id):
    return [{'principal_type': 'user', 'principal_id': user_id, 'permission': 'read'}]


def _tool_server_enabled(connection):
    return {**connection, 'config': {**connection.get('config', {}), 'enable': True}}


@pytest.mark.parametrize('open_toolkit', [False, True])
@pytest.mark.asyncio
async def test_users_get_distinct_bound_callables_not_metadata_selected_accounts(
    app_modules, monkeypatch, open_toolkit
):
    grants = _user_read_grant('alice') + _user_read_grant('bob')
    connection = _connection(
        {'googledrive': {'tools': [] if open_toolkit else ['GOOGLEDRIVE_FIND_FILE', 'GOOGLEDRIVE_GET_FILE_METADATA']}},
        server_id='shared',
        grants=grants,
    )
    connection['user_id'] = 'alice'
    connection['info']['user_id'] = 'alice'
    connection['config']['composio_user_id'] = 'alice'
    connection['connected_account_ids'] = ['alice-external-account']
    client_class = _in_process_mcp_client_class()
    requests = _install_user_bound_sessions(monkeypatch, app_modules, client_class)

    _req_a, _form_a, metadata_a, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        ['server:composio:shared'],
        client_class=client_class,
    )
    hostile = {
        'user_id': 'alice',
        '__user__': {'id': 'alice'},
        'composio_user_id': 'alice',
        'connection_metadata': {'user_id': 'alice'},
    }
    _req_b, _form_b, metadata_b, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [connection],
        _user('bob'),
        ['server:composio:shared'],
        hostile_metadata=hostile,
        client_class=client_class,
    )

    tool_name = _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE')
    result_a = await metadata_a['tools'][tool_name]['callable'](query='shared document')
    result_b = await metadata_b['tools'][tool_name]['callable'](query='shared document')
    assert result_a == {'files': _PRIVATE_DOCUMENTS['alice']}
    assert result_b == {'files': _PRIVATE_DOCUMENTS['bob']}
    alice_file = result_a['files'][0]
    metadata_tool = metadata_b['tools'][_composio_function_name('shared', 'GOOGLEDRIVE_GET_FILE_METADATA')]['callable']
    foreign_file = await metadata_tool(file_id=alice_file['id'], user_id='alice', owner='alice')
    assert foreign_file == {'error': 'File not found'}
    foreign_search = await metadata_b['tools'][tool_name]['callable'](
        query=alice_file['content'], session_id=client_class.instances[0].session_id, user_id='alice'
    )
    assert foreign_search == {'files': []}
    assert alice_file['content'] not in json.dumps([result_b, foreign_file, foreign_search])
    assert [json.loads(request.content)['user_id'] for request in requests] == [
        'openwebui:alice',
        'openwebui:bob',
    ]
    assert [client.account_id for client in client_class.instances] == ['alice', 'bob']
    assert client_class.instances[0].session_id != client_class.instances[1].session_id
    assert client_class.instances[0].url != client_class.instances[1].url
    manager_name = _composio_function_name('shared', 'COMPOSIO_MANAGE_CONNECTIONS')
    link_a = await metadata_a['tools'][manager_name]['callable'](toolkits=['googledrive'])
    link_b = await metadata_b['tools'][manager_name]['callable'](toolkits=['googledrive'])
    assert link_a['connect_link'] != link_b['connect_link']
    assert link_b['connect_link'] == client_class.sessions[client_class.instances[1].url]['connect_link']


@pytest.mark.parametrize('open_toolkit', [False, True])
@pytest.mark.asyncio
async def test_disabled_denied_and_forged_composio_ids_make_no_provider_request(app_modules, monkeypatch, open_toolkit):
    client_class = _in_process_mcp_client_class()
    catalog_requests = []
    requests = _install_user_bound_sessions(monkeypatch, app_modules, client_class, catalog_requests=catalog_requests)
    alice = _user('alice')
    bob = _user('bob')

    cases = [
        (
            _connection(
                server_id='shared',
                enabled=False,
                grants=[{'principal_type': 'user', 'principal_id': '*', 'permission': 'read'}],
            ),
            'shared',
            alice,
        ),
        (_connection(server_id='shared', grants=_user_read_grant('alice')), 'shared', bob),
        (_connection(server_id='shared', grants=_user_read_grant('alice')), 'alice', bob),
        (_connection(server_id='shared', grants=[]), 'shared', alice),
    ]
    for connection, server_id, user in cases:
        if open_toolkit:
            connection['composio']['toolkits']['googledrive']['tools'] = []
        _request, _form, _metadata, _model = _prepare_chat_payload(
            monkeypatch,
            app_modules,
            [connection],
            user,
            [f'server:composio:{server_id}'],
            client_class=client_class,
        )
        result = await app_modules.middleware.connect_composio_server(_request, server_id, user)
        assert result is None
        _form, chat_metadata, _events, _pending = await app_modules.middleware.process_chat_payload(
            _request, _form, user, _metadata, _model
        )
        assert not chat_metadata.get('mcp_clients')
        assert not chat_metadata.get('tools')

    assert requests == []
    assert catalog_requests == []
    assert client_class.instances == []


@pytest.mark.asyncio
async def test_chat_callable_exposes_only_policy_tools_and_allows_explicit_write_and_other_toolkit(
    app_modules, monkeypatch
):
    client_class = _in_process_mcp_client_class()
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    user = _user('alice')

    read_only = _tool_server_enabled(
        _connection(
            {
                'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE']},
            },
            server_id='shared',
            grants=_user_read_grant('alice'),
        )
    )
    read_only['composio']['toolkits']['googledrive']['tools'].append('COMPOSIO_SEARCH_TOOLS')
    read_only['config']['function_name_filter_list'] = 'GOOGLEDRIVE_FIND_FILE,COMPOSIO_SEARCH_TOOLS'
    _request, _form, metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [read_only],
        user,
        ['server:composio:shared'],
        client_class=client_class,
    )
    names = set(metadata['tools'])
    find_name = _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE')
    manager_name = _composio_function_name('shared', 'COMPOSIO_MANAGE_CONNECTIONS')
    assert names == {find_name, manager_name}
    assert _composio_function_name('shared', 'GOOGLEDRIVE_WRITE_FILE') not in names
    assert _composio_function_name('shared', 'COMPOSIO_SEARCH_TOOLS') not in names
    find_spec = metadata['tools'][find_name]['spec']
    assert find_spec['description'].startswith('GOOGLEDRIVE_FIND_FILE')

    write_policy = _tool_server_enabled(
        _connection(
            {
                'googledrive': {
                    'tools': ['GOOGLEDRIVE_FIND_FILE', 'GOOGLEDRIVE_WRITE_FILE'],
                },
            },
            server_id='shared',
            grants=_user_read_grant('alice'),
        )
    )
    _request, _form, write_metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [write_policy],
        user,
        ['server:composio:shared'],
        client_class=client_class,
    )
    write_name = _composio_function_name('shared', 'GOOGLEDRIVE_WRITE_FILE')
    write_result = await write_metadata['tools'][write_name]['callable'](file_id='doc-1')
    assert write_result == {'written_by': 'alice', 'file_id': 'doc-1'}

    notion_policy = _tool_server_enabled(
        _connection(
            {'notion': {'tools': ['NOTION_SEARCH']}},
            server_id='shared',
            grants=_user_read_grant('alice'),
        )
    )
    _request, _form, notion_metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [notion_policy],
        user,
        ['server:composio:shared'],
        client_class=client_class,
    )
    notion_name = _composio_function_name('shared', 'NOTION_SEARCH')
    assert notion_name in notion_metadata['tools']
    assert _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE') not in notion_metadata['tools']
    notion_result = await notion_metadata['tools'][notion_name]['callable'](query='plan')
    assert notion_result == {'pages': [{'id': 'work-plan', 'title': 'plan'}]}


@pytest.mark.asyncio
async def test_manager_tool_schema_and_callable_are_policy_and_session_scoped(app_modules, monkeypatch):
    client_class = _in_process_mcp_client_class(consent_status='pending')
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _tool_server_enabled(
        _connection(
            {
                'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE']},
            },
            server_id='shared',
            grants=_user_read_grant('alice'),
        )
    )
    _request, form_data, metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        ['server:composio:shared'],
        client_class=client_class,
    )

    manager_name = _composio_function_name('shared', 'COMPOSIO_MANAGE_CONNECTIONS')
    manager = metadata['tools'][manager_name]
    properties = manager['spec']['parameters']['properties']
    assert 'session_id' not in properties
    assert properties['toolkits']['items']['enum'] == ['googledrive']
    assert 'session_id' not in manager['spec']['parameters'].get('required', [])
    assert manager['spec']['parameters']['additionalProperties'] is False
    assert 'session_id' in _APP_TOOL_SPECS[-1]['parameters']['properties']
    system_context = '\n'.join(
        message.get('content', '')
        for message in form_data['messages']
        if message.get('role') == 'system' and isinstance(message.get('content'), str)
    )
    assert manager_name in system_context
    assert 'test-project-key' not in json.dumps(form_data, default=str)
    assert client_class.instances[0].url not in system_context

    provider = client_class.instances[0]
    for arguments in (
        {'toolkits': ['notion']},
        {'toolkits': ['googledrive'], 'session_id': 'foreign-session'},
        {'toolkits': ['googledrive'], 'unknown': 'value'},
        {'toolkits': ['googledrive'], 'reinitiate_all': 'true'},
    ):
        calls_before = len(provider.calls)
        with pytest.raises(ValueError):
            await manager['callable'](**arguments)
        assert len(provider.calls) == calls_before

    result = await manager['callable'](toolkits=['googledrive'])
    assert result == {
        'status': 'pending',
        'connect_link': client_class.sessions[provider.url]['connect_link'],
    }
    provider_call = provider.calls[-1]
    assert provider_call[0] == 'COMPOSIO_MANAGE_CONNECTIONS'
    assert provider_call[1] == {'toolkits': ['googledrive'], 'session_id': provider.session_id}


@pytest.mark.parametrize('consent_status', ['pending', 'inactive', 'failed'])
@pytest.mark.parametrize('tool_slug', ['GOOGLEDRIVE_FIND_FILE', 'GOOGLEDRIVE_WRITE_FILE'])
@pytest.mark.asyncio
async def test_incomplete_consent_is_not_misreported_as_authorized_or_retried(
    app_modules, monkeypatch, consent_status, tool_slug
):
    client_class = _in_process_mcp_client_class(consent_status=consent_status)
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _tool_server_enabled(
        _connection(
            {'googledrive': {'tools': [tool_slug]}},
            server_id='shared',
            grants=_user_read_grant('alice'),
        )
    )
    _request, _form, metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        ['server:composio:shared'],
        client_class=client_class,
    )
    manager = metadata['tools'][_composio_function_name('shared', 'COMPOSIO_MANAGE_CONNECTIONS')]['callable']
    app_tool = metadata['tools'][_composio_function_name('shared', tool_slug)]['callable']
    pending = await manager(toolkits=['googledrive'])
    assert pending['status'] == consent_status
    assert 'connect_link' in pending

    provider = client_class.instances[0]
    arguments = {'query': 'private report'} if tool_slug == 'GOOGLEDRIVE_FIND_FILE' else {'file_id': 'private report'}
    with pytest.raises(ValueError, match='^Composio is unavailable$'):
        await app_tool(**arguments)
    assert [name for name, _args in provider.calls] == [
        'COMPOSIO_MANAGE_CONNECTIONS',
        tool_slug,
    ]


@pytest.mark.parametrize('failure', ['connect', 'listing'])
@pytest.mark.asyncio
async def test_mcp_failures_close_client_and_sanitize_private_diagnostics(app_modules, monkeypatch, caplog, failure):
    client_class = _in_process_mcp_client_class(fail_connect=failure == 'connect', fail_listing=failure == 'listing')
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _tool_server_enabled(_connection(server_id='shared', grants=_user_read_grant('alice')))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        ['server:composio:shared'],
        client_class=client_class,
    )
    caplog.set_level(logging.DEBUG)
    with pytest.raises(ValueError, match='^Composio is unavailable$') as error:
        await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    private_url = client_class.instances[0].url
    assert private_url not in str(error.value)
    assert 'test-project-key' not in str(error.value)

    assert len(client_class.instances) == 1
    assert client_class.instances[0].closed is True
    assert private_url not in caplog.text
    assert 'test-project-key' not in caplog.text


@pytest.mark.asyncio
async def test_missing_connection_manager_fails_closed_and_disconnects(app_modules, monkeypatch):
    catalog = [_APP_TOOL_SPECS[0]]
    client_class = _in_process_mcp_client_class(app_specs=catalog)
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _tool_server_enabled(_connection(server_id='shared', grants=_user_read_grant('alice')))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        ['server:composio:shared'],
        client_class=client_class,
    )
    with pytest.raises(ValueError, match='^Composio is unavailable$'):
        await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    assert client_class.instances[0].closed is True


@pytest.mark.asyncio
async def test_request_cleanup_closes_composio_client_and_clears_full_tool_id_map(app_modules, monkeypatch):
    client_class = _in_process_mcp_client_class()
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _tool_server_enabled(_connection(server_id='shared', grants=_user_read_grant('alice')))
    _request, _form, metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        ['server:composio:shared'],
        client_class=client_class,
    )
    client_map = metadata['mcp_clients']
    assert list(client_map) == ['server:composio:shared']
    await app_modules.middleware.disconnect_mcp_clients(client_map)
    assert client_map == {}
    assert client_class.instances[0].closed is True


@pytest.mark.asyncio
async def test_native_and_composio_with_same_id_and_overlapping_slug_keep_callables_separate(app_modules, monkeypatch):
    native_connection = {
        'type': 'mcp',
        'url': 'https://native.example/mcp',
        'path': '',
        'auth_type': 'none',
        'key': None,
        'headers': None,
        'config': {'enable': True, 'access_grants': _user_read_grant('alice')},
        'info': {'id': 'shared', 'name': 'Native'},
    }
    composio_connection = _tool_server_enabled(
        _connection(
            {'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE']}},
            server_id='shared',
            grants=_user_read_grant('alice'),
        )
    )
    client_class = _in_process_mcp_client_class(
        native_specs=[
            {
                'name': 'GOOGLEDRIVE_FIND_FILE',
                'description': 'Native provider copy.',
                'parameters': {'type': 'object', 'properties': {'query': {'type': 'string'}}},
            }
        ]
    )
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    _request, _form, metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [native_connection, composio_connection],
        _user('alice'),
        ['server:mcp:shared', 'server:composio:shared'],
        client_class=client_class,
    )

    native_name = 'shared_GOOGLEDRIVE_FIND_FILE'
    composio_name = _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE')
    assert native_name in metadata['tools']
    assert composio_name in metadata['tools']
    assert native_name != composio_name
    assert set(metadata['mcp_clients']) == {'shared', 'server:composio:shared'}
    native_result = await metadata['tools'][native_name]['callable'](query='document')
    composio_result = await metadata['tools'][composio_name]['callable'](query='shared document')
    assert native_result == {'provider': 'native', 'tool': 'GOOGLEDRIVE_FIND_FILE'}
    assert composio_result == {'files': _PRIVATE_DOCUMENTS['alice']}
    assert [client.account_id for client in client_class.instances] == ['native', 'alice']
    await app_modules.middleware.disconnect_mcp_clients(metadata['mcp_clients'])
    assert metadata['mcp_clients'] == {}
    assert all(client.closed for client in client_class.instances)
    assert client_class.disconnect_order == list(reversed(client_class.instances))


@pytest.mark.asyncio
async def test_remaining_function_name_collision_fails_and_closes_all_clients(app_modules, monkeypatch):
    import hashlib

    server_id = 'comp'
    slug = 'GOOGLEDRIVE_FIND_FILE'
    collision_suffix = hashlib.sha256(f'{server_id}:{slug}'.encode()).hexdigest()[:24]
    native_connection = {
        'type': 'mcp',
        'url': 'https://native.example/mcp',
        'path': '',
        'auth_type': 'none',
        'key': None,
        'headers': None,
        'config': {'enable': True, 'access_grants': _user_read_grant('alice')},
        'info': {'id': server_id},
    }
    composio_connection = _tool_server_enabled(
        _connection(
            {'googledrive': {'tools': [slug]}},
            server_id=server_id,
            grants=_user_read_grant('alice'),
        )
    )
    client_class = _in_process_mcp_client_class(
        native_specs=[
            {
                'name': collision_suffix,
                'description': 'Chosen to collide with the hashed Composio name.',
                'parameters': {'type': 'object', 'properties': {}},
            }
        ]
    )
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    try:
        await _process_chat_with_servers(
            monkeypatch,
            app_modules,
            [native_connection, composio_connection],
            _user('alice'),
            ['server:mcp:comp', 'server:composio:comp'],
            client_class=client_class,
        )
    except Exception as error:
        assert 'Duplicate MCP tool function name' in str(error)
    else:
        raise AssertionError('A remaining native/Composio function-name collision must fail closed')
    assert len(client_class.instances) == 2
    assert all(client.closed for client in client_class.instances)


def _native_connection(server_id='shared'):
    return {
        'type': 'mcp',
        'url': 'https://native.example/mcp',
        'path': '',
        'auth_type': 'none',
        'key': None,
        'headers': None,
        'config': {'enable': True, 'access_grants': _user_read_grant('alice')},
        'info': {'id': server_id, 'name': 'Native'},
    }


async def _execute_tool_output(app_modules, request, form_data, metadata, user, name, arguments):
    tool_call = {
        'call_id': 'test-call',
        'type': 'function_call',
        'name': name,
        'arguments': json.dumps(arguments),
    }
    _params, result, tool, tool_type, direct = await app_modules.middleware.execute_tool_call(
        form_data, metadata, None, tool_call
    )
    if tool:
        result, _files, _embeds = await app_modules.middleware.process_tool_result(
            request, name, result, tool_type, direct, metadata, user
        )
    return {'content': app_modules.middleware.tool_result_content(result)}


@pytest.mark.parametrize('policy', [None, {}, {'toolkits': {}}, {'toolkits': {'googledrive': {'tools': ['*']}}}])
@pytest.mark.asyncio
async def test_saved_invalid_policy_cannot_make_a_session_request(app_modules, monkeypatch, policy):
    seen = []

    def handler(request):
        seen.append(request)
        raise AssertionError('Invalid policy must fail before contacting Composio')

    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    connection = _connection()
    connection['composio'] = policy
    with pytest.raises(ValidationError):
        await app_modules.composio.create_composio_session(connection, 'alice', 'https://webui.example.test')
    assert seen == []


@pytest.mark.asyncio
async def test_session_redirect_never_forwards_project_key_or_connects_mcp(app_modules, monkeypatch):
    seen = []
    client_class = _in_process_mcp_client_class()

    def handler(request):
        seen.append(request)
        if request.url.host == 'backend.composio.dev':
            return httpx.Response(307, headers={'location': 'https://attacker.example/collect'}, request=request)
        raise AssertionError('Redirect destination must not be contacted')

    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    connection = _connection(server_id='shared', grants=_user_read_grant('alice'))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('alice'), ['server:composio:shared'], client_class=client_class
    )
    with pytest.raises(ValueError, match='^Composio is unavailable$'):
        await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    assert len(seen) == 1
    assert seen[0].url.host == 'backend.composio.dev'
    assert seen[0].headers['x-api-key'] == 'test-project-key'
    assert client_class.instances == []


@pytest.mark.parametrize('failure', ['transport', 'timeout', 'invalid-json'])
@pytest.mark.asyncio
async def test_session_transport_and_json_errors_are_safe_without_retry(app_modules, monkeypatch, caplog, failure):
    seen = []
    secret = 'private-provider-error-and-key'
    caplog.set_level(logging.DEBUG)

    def handler(request):
        seen.append(request)
        if failure == 'invalid-json':
            return httpx.Response(200, content=secret, request=request)
        error_class = httpx.ConnectError if failure == 'transport' else httpx.ReadTimeout
        try:
            raise error_class(secret, request=request)
        except httpx.HTTPError:
            logging.getLogger('httpcore.connection').exception('failed private request')
            raise

    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    with pytest.raises(ValueError, match='^Composio is unavailable$'):
        await app_modules.composio.create_composio_session(_connection(), 'alice', 'https://webui.example.test')
    assert len(seen) == 1
    assert secret not in caplog.text
    assert 'test-project-key' not in caplog.text


@pytest.mark.asyncio
async def test_sessions_are_fresh_per_request_and_multiple_connections_bind_their_own_client(app_modules, monkeypatch):
    client_class = _in_process_mcp_client_class()
    requests = _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connections = [
        _connection(server_id='drive', grants=_user_read_grant('alice')),
        _connection({'notion': {'tools': ['NOTION_SEARCH']}}, server_id='notion', grants=_user_read_grant('alice')),
    ]
    saved = copy.deepcopy(connections)
    _request, form_data, first, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        connections,
        _user('alice'),
        ['server:composio:drive', 'server:composio:notion'],
        client_class=client_class,
    )
    _request, _form, second, _ = await _process_chat_with_servers(
        monkeypatch, app_modules, connections, _user('alice'), ['server:composio:drive'], client_class=client_class
    )
    drive_name = _composio_function_name('drive', 'GOOGLEDRIVE_FIND_FILE')
    notion_name = _composio_function_name('notion', 'NOTION_SEARCH')
    assert first['tools'][drive_name]['callable'] is not second['tools'][drive_name]['callable']
    assert await first['tools'][drive_name]['callable'](query='shared document') == {
        'files': _PRIVATE_DOCUMENTS['alice']
    }
    assert await second['tools'][drive_name]['callable'](query='shared document') == {
        'files': _PRIVATE_DOCUMENTS['alice']
    }
    assert await first['tools'][notion_name]['callable'](query='plan') == {
        'pages': [{'id': 'work-plan', 'title': 'plan'}]
    }
    assert [client.calls[0][0] for client in client_class.instances] == [
        'GOOGLEDRIVE_FIND_FILE',
        'NOTION_SEARCH',
        'GOOGLEDRIVE_FIND_FILE',
    ]
    assert len({client.session_id for client in client_class.instances}) == 3
    assert [json.loads(request.content)['user_id'] for request in requests] == ['openwebui:alice'] * 3
    assert set(first['mcp_clients']) == {'server:composio:drive', 'server:composio:notion'}
    assert connections == saved
    model_context = json.dumps({'tools': form_data['tools'], 'messages': form_data['messages']})
    for client in client_class.instances:
        assert client.url not in model_context and client.session_id not in model_context
    assert 'test-project-key' not in model_context
    await app_modules.middleware.disconnect_mcp_clients(first['mcp_clients'])
    await app_modules.middleware.disconnect_mcp_clients(second['mcp_clients'])
    assert all(client.closed for client in client_class.instances)


@pytest.mark.asyncio
async def test_catalog_is_access_filtered_without_sessions_oauth_or_private_fields(app_modules, monkeypatch):
    from open_webui import config
    from open_webui.models.groups import Groups

    connections = [
        _connection(server_id='shared', grants=_user_read_grant('alice')),
        _connection(server_id='private'),
        _connection(server_id='disabled', enabled=False, grants=_user_read_grant('alice')),
        _connection(server_id='bob-only', grants=_user_read_grant('bob')),
        _native_connection(),
    ]
    seen = []

    async def config_get(key, default=None):
        return connections if key == 'tool_server.connections' else default

    async def no_servers(*_args, **_kwargs):
        return []

    async def forbidden_oauth(*_args, **_kwargs):
        raise AssertionError('Composio catalog must not enter native OAuth or create provider sessions')

    def handler(request):
        seen.append(request)
        raise AssertionError('Listing integrations must not contact Composio')

    monkeypatch.setattr(app_modules.tools.Config, 'get', staticmethod(config_get))
    monkeypatch.setattr(app_modules.tools, 'ENABLE_TOOLS', False)
    monkeypatch.setattr(app_modules.tools, 'BYPASS_ADMIN_ACCESS_CONTROL', False)
    monkeypatch.setattr(config, 'BYPASS_ADMIN_ACCESS_CONTROL', False)
    monkeypatch.setattr(Groups, 'get_groups_by_member_id', staticmethod(no_servers))
    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                redis=None,
                TOOL_SERVERS=[],
                oauth_client_manager=SimpleNamespace(get_oauth_token=forbidden_oauth),
            )
        )
    )
    alice = await app_modules.tools.get_tools(request, user=_user('alice'), db=None)
    assert {tool.id for tool in alice} == {'server:composio:shared', 'server:mcp:shared'}
    bob = await app_modules.tools.get_tools(request, user=_user('bob'), db=None)
    assert {tool.id for tool in bob} == {'server:composio:bob-only'}
    admin = await app_modules.tools.get_tools(request, user=_user('admin', role='admin'), db=None)
    assert {tool.id for tool in admin} == {'server:composio:private'}
    catalog = [tool.model_dump() for tool in alice + bob + admin]
    serialized = json.dumps(catalog)
    assert 'test-project-key' not in serialized
    assert 'backend.composio.dev' not in serialized
    assert 'auth_config_id' not in serialized
    for tool in catalog:
        assert 'authenticated' not in tool
        assert 'composio' not in tool and 'headers' not in tool and 'key' not in tool
    assert seen == []
    assert request.app.state.TOOL_SERVERS == []


@pytest.mark.parametrize('enabled', [True, False])
@pytest.mark.parametrize('open_toolkit', [False, True])
@pytest.mark.asyncio
async def test_admin_verification_returns_only_safe_summary_without_external_consent(
    app_modules, monkeypatch, enabled, open_toolkit
):
    client_class = _in_process_mcp_client_class(app_specs=_OPEN_TOOL_SPECS, consent_status='pending')
    catalog_requests = []
    requests = _install_user_bound_sessions(monkeypatch, app_modules, client_class, catalog_requests=catalog_requests)
    toolkits = {'googledrive': {'tools': []}, 'notion': {'tools': ['NOTION_SEARCH']}} if open_toolkit else None
    connection = _connection(toolkits, server_id='shared', enabled=enabled)
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('admin', role='admin'), [], client_class=client_class
    )
    monkeypatch.setattr(app_modules.configs, 'MCPClient', client_class)
    result = await app_modules.configs.verify_tool_servers_config(
        request, app_modules.configs.ToolServerConnection.model_validate(connection), _user('admin', role='admin')
    )
    assert result == {'status': True, 'composio': True, 'tool_count': 6 if open_toolkit else 2}
    assert len(requests) == 1
    assert len(catalog_requests) == (1 if open_toolkit else 0)
    assert json.loads(requests[0].content)['user_id'] == 'openwebui:admin'
    assert len(client_class.instances) == 1
    client = client_class.instances[0]
    assert client.closed
    assert client.calls == []
    assert 'test-project-key' not in json.dumps(result) and client.url not in json.dumps(result)


@pytest.mark.parametrize('failure', ['invalid-callback', 'foreign-user', 'listing', 'missing-manager'])
@pytest.mark.parametrize('open_toolkit', [False, True])
@pytest.mark.asyncio
async def test_admin_verification_fails_closed_and_cleans_up(app_modules, monkeypatch, caplog, failure, open_toolkit):
    from fastapi import HTTPException

    client_class = _in_process_mcp_client_class(
        fail_listing=failure == 'listing', app_specs=[_APP_TOOL_SPECS[0]] if failure == 'missing-manager' else None
    )
    catalog_requests = []
    requests = _install_user_bound_sessions(monkeypatch, app_modules, client_class, catalog_requests=catalog_requests)
    connection = _connection({'googledrive': {'tools': []}} if open_toolkit else None)
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('admin', role='admin'), [], client_class=client_class
    )
    monkeypatch.setattr(app_modules.configs, 'MCPClient', client_class)
    expected = 'Composio is unavailable'
    if failure == 'invalid-callback':
        original_get = app_modules.configs.Config.get

        async def config_get(key, default=None):
            return 'http://attacker.example' if key == 'webui.url' else await original_get(key, default)

        monkeypatch.setattr(app_modules.configs.Config, 'get', staticmethod(config_get))
        expected = 'Configure the WebUI URL before using Composio'
    elif failure == 'foreign-user':

        def handler(provider_request):
            requests.append(provider_request)
            return httpx.Response(200, json=_session_response('alice'), request=provider_request)

        _install_composio_transport(monkeypatch, app_modules.composio, handler)
    caplog.set_level(logging.DEBUG)
    with pytest.raises(HTTPException) as error:
        await app_modules.configs.verify_tool_servers_config(
            request, app_modules.configs.ToolServerConnection.model_validate(connection), _user('admin', role='admin')
        )
    assert error.value.status_code == 400
    assert error.value.detail == expected
    assert len(requests) == (0 if failure == 'invalid-callback' else 1)
    assert len(catalog_requests) == (1 if open_toolkit and failure in {'listing', 'missing-manager'} else 0)
    assert len(client_class.instances) == (1 if failure in {'listing', 'missing-manager'} else 0)
    assert all(client.closed and not client.calls for client in client_class.instances)
    assert 'test-project-key' not in caplog.text
    for client in client_class.instances:
        assert client.url not in caplog.text and client.url not in str(error.value.detail)


@pytest.mark.asyncio
async def test_invalid_composio_record_is_revalidated_before_oauth_or_config_mutations(app_modules, monkeypatch):
    effects = []

    async def config_get(*_args, **_kwargs):
        effects.append('read-existing-config')
        return []

    async def upsert(*_args, **_kwargs):
        effects.append('upsert')

    monkeypatch.setattr(app_modules.configs.Config, 'get', staticmethod(config_get))
    monkeypatch.setattr(app_modules.configs.Config, 'upsert', staticmethod(upsert))
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                oauth_client_manager=SimpleNamespace(
                    remove_client=lambda *_args: effects.append('remove-oauth'),
                    add_client=lambda *_args: effects.append('add-oauth'),
                )
            )
        )
    )
    valid = app_modules.configs.ToolServerConnection.model_validate(_connection())
    invalid = valid.model_copy(deep=True)
    invalid.composio = None  # Simulate a malformed saved/imported policy past form construction.
    form = app_modules.configs.ToolServersConfigForm.model_construct(TOOL_SERVER_CONNECTIONS=[valid, invalid])
    with pytest.raises(ValidationError):
        await app_modules.configs.set_tool_servers_config(request, form, _user('admin', role='admin'))
    assert effects == []


@pytest.mark.parametrize('consent_status', ['active', 'pending'])
@pytest.mark.asyncio
async def test_actual_tool_execution_rejects_hostile_manager_kwargs_and_normalizes_mcp_results(
    app_modules, monkeypatch, consent_status
):
    base_client = _in_process_mcp_client_class(consent_status=consent_status, native_specs=[_APP_TOOL_SPECS[0]])

    class ContentMCPClient(base_client):
        async def call_tool(self, function_name, function_args):
            result = await super().call_tool(function_name, function_args)
            return [{'type': 'text', 'text': json.dumps(result)}]

    client_class = ContentMCPClient
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _connection(server_id='shared', grants=_user_read_grant('alice'))
    user = _user('alice')
    request, form_data, metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [_native_connection(), connection],
        user,
        ['server:mcp:shared', 'server:composio:shared'],
        client_class=client_class,
    )
    manager_name = _composio_function_name('shared', 'COMPOSIO_MANAGE_CONNECTIONS')
    provider = client_class.instances[1]
    for arguments in (
        {'toolkits': ['notion']},
        {'toolkits': ['googledrive'], 'session_id': 'foreign-session'},
        {'toolkits': ['googledrive'], 'unknown': True},
        {'toolkits': ['googledrive'], 'reinitiate_all': 'not-a-boolean'},
    ):
        result = await _execute_tool_output(app_modules, request, form_data, metadata, user, manager_name, arguments)
        assert json.loads(result['content'])['error']
        assert provider.calls == []
    result = await _execute_tool_output(
        app_modules, request, form_data, metadata, user, manager_name, {'toolkits': ['googledrive']}
    )
    assert json.loads(result['content']) == {
        'status': consent_status,
        'connect_link': client_class.sessions[provider.url]['connect_link'],
    }
    assert provider.calls == [
        ('COMPOSIO_MANAGE_CONNECTIONS', {'toolkits': ['googledrive'], 'session_id': provider.session_id})
    ]
    app_name = _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE')
    app_result = await _execute_tool_output(
        app_modules, request, form_data, metadata, user, app_name, {'query': 'shared document'}
    )
    assert json.loads(app_result['content']) == (
        {'files': _PRIVATE_DOCUMENTS['alice']} if consent_status == 'active' else {'error': 'Composio is unavailable'}
    )
    assert len(provider.calls) == 2
    for slug in ('GOOGLEDRIVE_WRITE_FILE', 'COMPOSIO_SEARCH_TOOLS'):
        not_allowed = await _execute_tool_output(
            app_modules, request, form_data, metadata, user, _composio_function_name('shared', slug), {}
        )
        assert 'not found' in not_allowed['content']
    assert len(provider.calls) == 2
    native_result = await _execute_tool_output(
        app_modules,
        request,
        form_data,
        metadata,
        user,
        'shared_GOOGLEDRIVE_FIND_FILE',
        {'query': 'document', 'unknown': True},
    )
    assert json.loads(native_result['content']) == {'provider': 'native', 'tool': 'GOOGLEDRIVE_FIND_FILE'}
    assert client_class.instances[0].calls == [('GOOGLEDRIVE_FIND_FILE', {'query': 'document', 'unknown': True})]
    await app_modules.middleware.disconnect_mcp_clients(metadata['mcp_clients'])


@pytest.mark.asyncio
async def test_native_mcp_listing_failure_still_propagates_and_closes_client(app_modules, monkeypatch):
    client_class = _in_process_mcp_client_class(fail_listing=True)
    request, _form, metadata, _model = _prepare_chat_payload(
        monkeypatch,
        app_modules,
        [_native_connection()],
        _user('alice'),
        ['server:mcp:shared'],
        client_class=client_class,
    )
    with pytest.raises(RuntimeError, match='private listing failure'):
        await app_modules.tool_utils.connect_mcp_server(request, 'shared', _user('alice'), {'__metadata__': metadata})
    assert len(client_class.instances) == 1
    assert client_class.instances[0].closed
    assert client_class.sessions == {}


@pytest.mark.parametrize('server_type', ['mcp', 'composio'])
@pytest.mark.asyncio
async def test_duplicate_specs_within_one_mcp_catalog_fail_without_overwriting(app_modules, monkeypatch, server_type):
    duplicated = [copy.deepcopy(_APP_TOOL_SPECS[0]), copy.deepcopy(_APP_TOOL_SPECS[0])]
    client_class = _in_process_mcp_client_class(app_specs=duplicated + [_APP_TOOL_SPECS[-1]], native_specs=duplicated)
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = (
        _native_connection()
        if server_type == 'mcp'
        else _connection(server_id='shared', grants=_user_read_grant('alice'))
    )
    with pytest.raises(ValueError, match='^Duplicate MCP tool function name$'):
        await _process_chat_with_servers(
            monkeypatch,
            app_modules,
            [connection],
            _user('alice'),
            [f'server:{server_type}:shared'],
            client_class=client_class,
        )
    assert len(client_class.instances) == 1
    assert client_class.instances[0].closed
    assert client_class.instances[0].calls == []


@pytest.mark.parametrize('source', ['plugin', 'aliased-plugin', 'terminal', 'direct', 'inlet', 'builtin'])
@pytest.mark.asyncio
async def test_mcp_name_collisions_at_other_tool_merge_boundaries_fail_and_close_clients(
    app_modules, monkeypatch, source
):
    middleware = app_modules.middleware
    client_class = _in_process_mcp_client_class()
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _connection(server_id='shared', grants=_user_read_grant('alice'))
    user = _user('alice')
    tool_ids = ['server:composio:shared', *(['fixture-plugin'] if source in {'plugin', 'aliased-plugin'} else [])]
    request, form_data, metadata, model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], user, tool_ids, client_class=client_class
    )
    name = _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE')
    spec = {'name': name, 'description': 'Conflicting tool.', 'parameters': {'type': 'object', 'properties': {}}}

    async def conflicting_tool(*_args, **_kwargs):
        raise AssertionError('A conflicting callable must never be invoked')

    callable_key = 'different-callable-key' if source == 'aliased-plugin' else name
    collision = {callable_key: {'spec': spec, 'callable': conflicting_tool, 'type': 'local', 'direct': False}}

    async def collision_tools(*_args, **_kwargs):
        return collision

    async def no_result(*_args, **_kwargs):
        return None

    if source in {'plugin', 'aliased-plugin'}:

        async def no_filters(*_args, **_kwargs):
            return []

        async def filter_form(*_args, **kwargs):
            return kwargs['form_data'], {}

        monkeypatch.setattr(middleware, 'ENABLE_FUNCTIONS', True)
        monkeypatch.setattr(middleware, 'get_filter_context', lambda _request: None)
        monkeypatch.setattr(middleware, 'get_filter_functions', no_filters)
        monkeypatch.setattr(middleware, 'process_filter_functions', filter_form)
        monkeypatch.setattr(middleware, 'get_tools', collision_tools)
    elif source == 'terminal':
        from open_webui.utils import terminals

        original_get = middleware.Config.get

        async def config_get(key, default=None):
            return (
                [{'id': 'fixture-terminal'}]
                if key == 'terminal_server.connections'
                else await original_get(key, default)
            )

        form_data['terminal_id'] = 'fixture-terminal'
        monkeypatch.setattr(middleware.Config, 'get', staticmethod(config_get))
        monkeypatch.setattr(middleware, 'get_terminal_tools', collision_tools)
        monkeypatch.setattr(terminals, 'get_terminal_agents_md', no_result)
    elif source == 'direct':
        metadata['tool_servers'] = [{'url': 'https://direct.example/tool', 'specs': [spec]}]
    elif source == 'inlet':

        async def inlet(_request, body, *_args, **_kwargs):
            body['tools'] = [{'type': 'function', 'function': spec}]
            return body

        monkeypatch.setattr(middleware, 'process_pipeline_inlet_filter', inlet)
    else:
        from open_webui.models.skills import Skills

        async def no_skills(*_args, **_kwargs):
            return []

        async def file_context(messages, *_args, **_kwargs):
            return messages

        metadata['session_id'] = 'fixture-browser'
        model['info']['meta']['capabilities']['builtin_tools'] = True
        monkeypatch.setattr(Skills, 'get_skills', staticmethod(no_skills))
        monkeypatch.setattr(middleware, 'get_builtin_tools', collision_tools)
        monkeypatch.setattr(middleware, 'add_file_context', file_context)
    with pytest.raises(ValueError, match='^Duplicate MCP tool function name$'):
        await middleware.process_chat_payload(request, form_data, user, metadata, model)
    assert len(client_class.instances) == 1
    assert client_class.instances[0].closed
    assert client_class.instances[0].calls == []
    assert not metadata.get('mcp_clients')


@pytest.mark.asyncio
async def test_real_composio_lifecycle_suppresses_private_dependency_logs_but_not_native(
    app_modules, monkeypatch, caplog
):
    base_client = _in_process_mcp_client_class(native_specs=[_APP_TOOL_SPECS[0]])

    class LoggingMCPClient(base_client):
        def _emit(self, phase):
            self.phases.append(phase)
            key = self.headers.get('x-api-key', 'native-no-project-key')
            logging.getLogger('httpx2').info('%s request to %s with %s', phase, self.url, key)
            logging.getLogger('httpcore2.connection').debug('%s transport at %s', phase, self.url)
            try:
                raise RuntimeError(f'{phase} exception at {self.url} with {key}')
            except RuntimeError:
                logging.getLogger('mcp.client.streamable_http').debug('transport traceback', exc_info=True)
                logging.getLogger('open_webui.utils.mcp.client').debug('client traceback', exc_info=True)

        async def connect(self, url, headers=None, auth=None, *, httpx_client_factory=None):
            await super().connect(url, headers, auth=auth, httpx_client_factory=httpx_client_factory)
            self.phases = []
            self._emit('connect')
            await asyncio.sleep(0)

        async def list_tool_specs(self):
            self._emit('list')
            return await super().list_tool_specs()

        async def call_tool(self, function_name, function_args):
            self._emit('call')
            await asyncio.sleep(0)
            return await super().call_tool(function_name, function_args)

        async def disconnect(self):
            self._emit('disconnect')
            await super().disconnect()

    client_class = LoggingMCPClient
    requests = _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    caplog.set_level(logging.DEBUG)
    _request, _form, metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [_native_connection(), _connection(server_id='shared', grants=_user_read_grant('alice'))],
        _user('alice'),
        ['server:mcp:shared', 'server:composio:shared'],
        client_class=client_class,
    )
    native_name = 'shared_GOOGLEDRIVE_FIND_FILE'
    composio_name = _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE')
    await asyncio.gather(
        metadata['tools'][native_name]['callable'](query='shared document'),
        metadata['tools'][composio_name]['callable'](query='shared document'),
    )
    await app_modules.middleware.disconnect_mcp_clients(metadata['mcp_clients'])
    assert len(requests) == 1
    for client in client_class.instances:
        assert client.phases == ['connect', 'list', 'call', 'disconnect']
    private_client = client_class.instances[1]
    assert private_client.url not in caplog.text
    assert 'test-project-key' not in caplog.text
    assert 'https://native.example/mcp' in caplog.text
    assert 'native-no-project-key' in caplog.text
    assert any(record.name.startswith('mcp.') and record.exc_info for record in caplog.records)


@pytest.mark.asyncio
async def test_saving_composio_policy_preserves_native_settings_without_oauth_or_shared_sessions(
    app_modules, monkeypatch
):
    store = {'tool_server.connections': []}
    published = []
    seen = []

    async def config_get(key, default=None):
        return store.get(key, default)

    async def config_upsert(values):
        store.update(copy.deepcopy(values))

    async def publish(*_args, **kwargs):
        published.append(kwargs['data'])

    def forbidden_oauth(*_args, **_kwargs):
        raise AssertionError('Composio must not create or remove native OAuth registrations')

    def handler(request):
        seen.append(request)
        raise AssertionError('Saving an integration must not establish a Composio session')

    monkeypatch.setattr(app_modules.configs.Config, 'get', staticmethod(config_get))
    monkeypatch.setattr(app_modules.configs.Config, 'upsert', staticmethod(config_upsert))
    monkeypatch.setattr(app_modules.configs, 'publish_event', publish)
    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                redis=None,
                TOOL_SERVERS=[],
                oauth_client_manager=SimpleNamespace(remove_client=forbidden_oauth, add_client=forbidden_oauth),
            )
        )
    )
    composio = _connection(
        {'googledrive': {'tools': ['GOOGLEDRIVE_FIND_FILE'], 'auth_config_id': 'ac_approved_read_only'}},
        grants=_user_read_grant('alice'),
    )
    native = _native_connection()
    form = app_modules.configs.ToolServersConfigForm.model_validate({'TOOL_SERVER_CONNECTIONS': [native, composio]})
    result = await app_modules.configs.set_tool_servers_config(request, form, _user('admin', role='admin'))
    stored = store['tool_server.connections']
    assert result['TOOL_SERVER_CONNECTIONS'] == stored
    assert stored[0]['type'] == 'mcp' and stored[0]['url'] == native['url']
    assert stored[0]['config'] == native['config']
    assert stored[1]['composio'] == composio['composio']
    assert stored[1]['config'] == composio['config']
    assert request.app.state.TOOL_SERVERS == []
    assert seen == []
    assert published == [{'count': 2, 'types': ['mcp', 'composio']}]


@pytest.mark.asyncio
async def test_native_mcp_oauth_discovery_verification_remains_separate(app_modules, monkeypatch):
    metadata = {
        'issuer': 'https://native.example',
        'authorization_endpoint': 'https://native.example/authorize',
        'token_endpoint': 'https://native.example/token',
        'response_types_supported': ['code'],
        'code_challenge_methods_supported': ['S256'],
    }
    requested = []

    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def json(self):
            return metadata

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def get(self, url, **_kwargs):
            requested.append(url)
            return Response()

    async def discovery_urls(_url):
        return ['https://native.example/.well-known/oauth-authorization-server']

    def forbidden_mcp():
        raise AssertionError('OAuth discovery must not establish an MCP or Composio session')

    monkeypatch.setattr(app_modules.configs, 'get_discovery_urls', discovery_urls)
    monkeypatch.setattr(app_modules.configs.aiohttp, 'ClientSession', lambda **_kwargs: Session())
    monkeypatch.setattr(app_modules.configs, 'MCPClient', forbidden_mcp)
    connection = _native_connection()
    connection['auth_type'] = 'oauth_2.1'
    result = await app_modules.configs.verify_tool_servers_config(
        SimpleNamespace(state=SimpleNamespace()),
        app_modules.configs.ToolServerConnection.model_validate(connection),
        _user('admin', role='admin'),
    )
    assert result['status'] is True
    assert 'oauth_server_metadata' in result
    assert result['oauth_server_metadata']['response_types_supported'] == ['code']
    assert result['oauth_server_metadata']['code_challenge_methods_supported'] == ['S256']
    assert 'specs' not in result and 'composio' not in result
    assert requested == ['https://native.example/.well-known/oauth-authorization-server']


@pytest.mark.asyncio
async def test_bound_callable_captures_policy_and_rejects_unlisted_or_reserved_tools(app_modules, monkeypatch):
    client_class = _in_process_mcp_client_class()
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _connection(server_id='shared', grants=_user_read_grant('alice'))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('alice'), ['server:composio:shared'], client_class=client_class
    )
    client, _specs, session = await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    manager = app_modules.middleware._make_mcp_tool_function(client, 'COMPOSIO_MANAGE_CONNECTIONS', session)
    unlisted_write = app_modules.middleware._make_mcp_tool_function(client, 'GOOGLEDRIVE_WRITE_FILE', session)
    session['allowed_toolkits'].add('notion')
    session['allowed_tools'].add('GOOGLEDRIVE_WRITE_FILE')
    session['session_id'] = 'foreign-session'
    with pytest.raises(ValueError):
        await manager(toolkits=['notion'])
    with pytest.raises(ValueError, match='^Composio tool policy was rejected$'):
        await unlisted_write(file_id='alice-board-budget')
    session['allowed_tools'].add('COMPOSIO_SEARCH_TOOLS')
    reserved_helper = app_modules.middleware._make_mcp_tool_function(client, 'COMPOSIO_SEARCH_TOOLS', session)
    with pytest.raises(ValueError, match='^Composio tool policy was rejected$'):
        await reserved_helper()
    assert client.calls == []
    result = await manager(toolkits=['googledrive'])
    assert result['connect_link'] == client_class.sessions[client.url]['connect_link']
    assert client.calls[-1][1]['session_id'] == client.session_id
    await app_modules.middleware.disconnect_mcp_clients({'server:composio:shared': client})


@pytest.mark.parametrize('private_status', [200, 503])
@pytest.mark.asyncio
async def test_composio_scope_suppresses_real_httpx_telemetry_without_suppressing_native(
    app_modules, monkeypatch, private_status
):
    # Use the deployed instrumentor, a real HTTP transport and a local-only exporter.
    # MockTransport is not patched by global HTTPX instrumentation.
    monkeypatch.setenv('NO_PROXY', '*')
    monkeypatch.setenv('OTEL_PYTHON_HTTPX_EXCLUDED_URLS', '')
    instrumentation = pytest.importorskip(
        'opentelemetry.instrumentation.httpx',
        reason='Optional runtime tracing dependency; exercise with --with opentelemetry-instrumentation-httpx==0.63b1',
        exc_type=ModuleNotFoundError,
    )
    HTTPXClientInstrumentor = instrumentation.HTTPXClientInstrumentor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    private_token = 'private-composio-session-token'
    project_key = 'test-only-composio-project-key-for-tracing'
    hook_urls = []

    async def request_hook(span, request):
        # Match Open WebUI's URL-bearing httpx_async_request_hook behavior.
        url = str(request.url)
        hook_urls.append(url)
        span.update_name(f'{request.method.decode()} {url}')
        span.set_attribute('http.url', url)

    class Handler(BaseHTTPRequestHandler):
        received = []

        def do_GET(self):
            type(self).received.append((self.path, self.headers.get('x-api-key')))
            body = b'{"status":true}'
            self.send_response(private_status if private_token in self.path else 200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *_args):
            pass

    instrumentor = HTTPXClientInstrumentor()
    assert not instrumentor.is_instrumented_by_opentelemetry
    exporter = InMemorySpanExporter()
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f'http://127.0.0.1:{server.server_port}'
    private_url = f'{base_url}/mcp/{private_token}'
    native_url = f'{base_url}/native-mcp'
    after_scope_url = f'{base_url}/native-after-scope'
    try:
        instrumentor.instrument(tracer_provider=provider, async_request_hook=request_hook)
        assert instrumentor.is_instrumented_by_opentelemetry
        # Positive control: this exact factory/URL would leak through tracing without the scope.
        async with app_modules.composio.create_composio_httpx_client(
            headers={'x-api-key': project_key}, timeout=httpx.Timeout(5.0)
        ) as client:
            response = await client.get(private_url)
            assert response.status_code == private_status
            assert any(private_url in span.to_json() for span in exporter.get_finished_spans())
            exporter.clear()
            hook_urls.clear()
            Handler.received.clear()
            scope_started = asyncio.Event()
            native_finished = asyncio.Event()

            async def scoped_request():
                with app_modules.composio.composio_log_scope():
                    with app_modules.composio.composio_log_scope():
                        scope_started.set()
                        # MCP transport workers inherit the caller's telemetry context.
                        result = await asyncio.create_task(client.get(private_url))
                        assert result.status_code == private_status
                        if private_status == 503:
                            with pytest.raises(httpx.HTTPStatusError):
                                result.raise_for_status()
                    await native_finished.wait()
                # The same task/client is instrumented again after token restoration.
                client.headers.pop('x-api-key')
                result = await client.get(after_scope_url)
                assert result.status_code == 200

            async def native_request():
                await scope_started.wait()
                try:
                    async with _HTTPX_ASYNC_CLIENT(trust_env=False, timeout=5.0) as native_client:
                        result = await native_client.get(native_url)
                        assert result.status_code == 200
                finally:
                    native_finished.set()

            await asyncio.gather(scoped_request(), native_request())
        spans = exporter.get_finished_spans()
        serialized = '\n'.join(span.to_json() for span in spans)
        assert len(spans) == 2
        assert native_url in serialized
        assert after_scope_url in serialized
        assert private_url not in serialized
        assert private_token not in serialized
        assert project_key not in serialized
        assert set(hook_urls) == {native_url, after_scope_url}
        assert Handler.received.count((f'/mcp/{private_token}', project_key)) == 1
        assert len(Handler.received) == 3
    finally:
        instrumentor.uninstrument()
        provider.shutdown()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


# This provider fixture proves local policy, not live auth-config/scope enforcement.
@pytest.mark.parametrize('auth_config_id', [None, 'ac_provider_fixture'])
@pytest.mark.asyncio
async def test_open_toolkit_catalog_membership_controls_actual_chat_callables(app_modules, monkeypatch, auth_config_id):
    client_class = _in_process_mcp_client_class(app_specs=_OPEN_TOOL_SPECS)
    catalog_requests = []
    requests = _install_user_bound_sessions(monkeypatch, app_modules, client_class, catalog_requests=catalog_requests)
    drive = {'tools': []}
    if auth_config_id is not None:
        drive['auth_config_id'] = auth_config_id
    connection = _connection(
        {'googledrive': drive, 'notion': {'tools': ['NOTION_SEARCH']}},
        server_id='shared',
        grants=_user_read_grant('alice'),
    )
    _request, _form, metadata, _ = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        ['server:composio:shared'],
        client_class=client_class,
    )
    body = json.loads(requests[0].content)
    assert body == {
        'user_id': 'openwebui:alice',
        'toolkits': {'enable': ['googledrive', 'notion']},
        'tools': {'notion': {'enable': ['NOTION_SEARCH']}},
        'auth_configs': {} if auth_config_id is None else {'googledrive': auth_config_id},
        'instant': False,
        'manage_connections': {
            'enable': True,
            'callback_url': 'https://webui.example.test/',
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
    assert len(requests) == len(catalog_requests) == 1
    allowed = {
        'GOOGLEDRIVE_FIND_FILE',
        'GOOGLEDRIVE_GET_FILE_METADATA',
        'GOOGLEDRIVE_WRITE_FILE',
        'WORKSPACE_FIND_FILE',
        'NOTION_SEARCH',
        'COMPOSIO_MANAGE_CONNECTIONS',
    }
    assert set(metadata['tools']) == {_composio_function_name('shared', slug) for slug in allowed}
    # This name has no Drive prefix; the provider membership grants it access.
    lookup = metadata['tools'][_composio_function_name('shared', 'WORKSPACE_FIND_FILE')]['callable']
    assert await lookup(query='shared document') == {'files': _PRIVATE_DOCUMENTS['alice']}
    read = metadata['tools'][_composio_function_name('shared', 'GOOGLEDRIVE_GET_FILE_METADATA')]['callable']
    assert await read(file_id='alice-board-budget') == _PRIVATE_DOCUMENTS['alice'][0]
    write = metadata['tools'][_composio_function_name('shared', 'GOOGLEDRIVE_WRITE_FILE')]['callable']
    assert await write(file_id='alice-board-budget') == {'written_by': 'alice', 'file_id': 'alice-board-budget'}
    notion = metadata['tools'][_composio_function_name('shared', 'NOTION_SEARCH')]['callable']
    assert await notion(query='plan') == {'pages': [{'id': 'work-plan', 'title': 'plan'}]}
    # Foreign tools, a misleading Drive prefix, unknown MCP-only tools, and all
    # helpers except manager are excluded even when MCP advertises their schemas.
    assert {call[0] for call in client_class.instances[0].calls} <= allowed
    await app_modules.middleware.disconnect_mcp_clients(metadata['mcp_clients'])
    assert client_class.instances[0].closed


@pytest.mark.parametrize('surface', ['chat', 'admin-verify'])
@pytest.mark.asyncio
async def test_provider_uppercase_catalog_toolkits_match_lowercase_open_policy(
    app_modules, monkeypatch, caplog, surface
):
    # Replay public metadata from the deployed provider's 200 catalog response.
    # Its first item is a COMPOSIO helper, followed by uppercase Drive metadata.
    catalog = {
        'items': [
            {'slug': 'COMPOSIO_MANAGE_CONNECTIONS', 'toolkit': {'slug': 'COMPOSIO', 'name': 'composio'}},
            {'slug': 'GOOGLEDRIVE_ADD_PARENT', 'toolkit': {'slug': 'GOOGLEDRIVE', 'name': 'googledrive'}},
            {'slug': 'GOOGLEDRIVE_ADD_PROPERTY', 'toolkit': {'slug': 'GOOGLEDRIVE', 'name': 'googledrive'}},
            # Exercise reads and foreign/helper exclusions after provider parsing.
            {'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': {'slug': 'GoogleDrive', 'name': 'googledrive'}},
            {'slug': 'GOOGLEDRIVE_FOREIGN', 'toolkit': {'slug': 'GitHub', 'name': 'googledrive'}},
            {'slug': 'COMPOSIO_SEARCH_TOOLS', 'toolkit': {'slug': 'GOOGLEDRIVE', 'name': 'googledrive'}},
        ],
        'next_cursor': None,
        'total_pages': 1,
        'current_page': 1,
        'total_items': 6,
    }
    allowed = {
        'COMPOSIO_MANAGE_CONNECTIONS',
        'GOOGLEDRIVE_ADD_PARENT',
        'GOOGLEDRIVE_ADD_PROPERTY',
        'GOOGLEDRIVE_FIND_FILE',
    }
    app_specs = [
        {'name': item['slug'], 'description': item['slug'], 'parameters': {'type': 'object', 'properties': {}}}
        for item in catalog['items']
    ]
    client_class = _in_process_mcp_client_class(app_specs=app_specs)
    catalog_requests = []
    requests = _install_user_bound_sessions(
        monkeypatch,
        app_modules,
        client_class,
        catalog_pages={None: catalog},
        catalog_requests=catalog_requests,
    )
    user = _user('admin', role='admin') if surface == 'admin-verify' else _user('alice')
    connection = _connection({'googledrive': {'tools': []}}, server_id='shared', grants=_user_read_grant(user.id))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], user, [], client_class=client_class
    )
    caplog.set_level(logging.DEBUG)
    if surface == 'admin-verify':
        monkeypatch.setattr(app_modules.configs, 'MCPClient', client_class)
        result = await app_modules.configs.verify_tool_servers_config(
            request, app_modules.configs.ToolServerConnection.model_validate(connection), user
        )
        assert result == {'status': True, 'composio': True, 'tool_count': len(allowed)}
    else:
        client, specs, session = await app_modules.middleware.connect_composio_server(request, 'shared', user)
        assert session['allowed_toolkits'] == {'googledrive'}
        assert session['allowed_tools'] == allowed
        assert {spec['name'] for spec in specs} == allowed
        read = app_modules.middleware._make_mcp_tool_function(client, 'GOOGLEDRIVE_FIND_FILE', session)
        assert await read(query='shared document') == {'files': _PRIVATE_DOCUMENTS['alice']}
        for slug in ('GOOGLEDRIVE_FOREIGN', 'COMPOSIO_SEARCH_TOOLS'):
            forbidden = app_modules.middleware._make_mcp_tool_function(client, slug, session)
            with pytest.raises(ValueError, match='^Composio tool policy was rejected$'):
                await forbidden()
        await app_modules.middleware.disconnect_mcp_clients({'server:composio:shared': client})
    assert len(requests) == len(catalog_requests) == 1
    assert all(client.closed for client in client_class.instances)
    assert client_class.instances[0].calls == (
        [] if surface == 'admin-verify' else [('GOOGLEDRIVE_FIND_FILE', {'query': 'shared document'})]
    )
    assert 'test-project-key' not in caplog.text
    assert str(catalog_requests[0].url) not in caplog.text


@pytest.mark.asyncio
async def test_open_toolkit_catalog_paginates_to_preload_ceiling_and_calls_late_page_tool(app_modules, monkeypatch):
    first = [{'slug': f'DRIVE_APP_{index}', 'toolkit': {'slug': 'googledrive'}} for index in range(500)]
    second = [{'slug': f'DRIVE_APP_{index}', 'toolkit': {'slug': 'googledrive'}} for index in range(500, 999)] + [
        {'slug': 'WORKSPACE_FIND_FILE', 'toolkit': {'slug': 'googledrive'}}
    ]
    cursor = 'opaque+/=&cursor'
    catalog_requests = []
    client_class = _in_process_mcp_client_class(app_specs=_OPEN_TOOL_SPECS)
    requests = _install_user_bound_sessions(
        monkeypatch,
        app_modules,
        client_class,
        catalog_requests=catalog_requests,
        catalog_pages={
            None: {'items': first, 'next_cursor': cursor},
            cursor: {'items': second},  # Missing next_cursor is a terminal page.
        },
    )
    connection = _connection({'googledrive': {'tools': []}}, server_id='shared', grants=_user_read_grant('alice'))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('alice'), [], client_class=client_class
    )
    client, specs, session = await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    assert json.loads(requests[0].content)['tools'] == {}
    assert len(requests) == 1 and len(catalog_requests) == 2
    assert [request.url.params.get('cursor') for request in catalog_requests] == [None, cursor]
    assert len(session['allowed_tools'] - {'COMPOSIO_MANAGE_CONNECTIONS'}) == 1000
    assert {spec['name'] for spec in specs} == {'WORKSPACE_FIND_FILE', 'COMPOSIO_MANAGE_CONNECTIONS'}
    late_tool = app_modules.middleware._make_mcp_tool_function(client, 'WORKSPACE_FIND_FILE', session)
    assert await late_tool(query='shared document') == {'files': _PRIVATE_DOCUMENTS['alice']}
    await app_modules.middleware.disconnect_mcp_clients({'server:composio:shared': client})
    assert client.closed


@pytest.mark.parametrize(
    'catalog',
    [
        None,
        [],
        {},
        {'items': None},
        {'items': {}},
        {'items': [None]},
        {'items': [{'slug': 'GOOGLEDRIVE_FIND_FILE'}]},
        {'items': [{'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': None}]},
        {'items': [{'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': 'googledrive'}]},
        {'items': [{'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': {}}]},
        {'items': [{'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': {'slug': None}}]},
        {'items': [{'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': {'slug': 'GOOGLEDRI\u212aE'}}]},
        {'items': [{'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': {'slug': ' googledrive '}}]},
        {'items': [{'slug': 'GOOGLEDRIVE_FIND_FILE', 'toolkit': {'slug': '*'}}]},
        {'items': [{'slug': None, 'toolkit': {'slug': 'googledrive'}}]},
        {'items': [{'slug': 'googledrive_find_file', 'toolkit': {'slug': 'googledrive'}}]},
        {'items': [{'slug': 'GOOGLEDRIVE_*', 'toolkit': {'slug': 'googledrive'}}]},
        {'items': [{'slug': ' GOOGLEDRIVE_FIND_FILE ', 'toolkit': {'slug': 'googledrive'}}]},
        {'items': [_OPEN_TOOL_CATALOG[0]], 'next_cursor': 1},
        {'items': [_OPEN_TOOL_CATALOG[0]], 'next_cursor': ''},
        {'items': [_OPEN_TOOL_CATALOG[0]], 'next_cursor': '  '},
        {'items': [], 'next_cursor': 'must-not-be-followed'},
        {'items': [_OPEN_TOOL_CATALOG[0], _OPEN_TOOL_CATALOG[0]]},
    ],
)
@pytest.mark.parametrize('surface', ['chat', 'admin-verify'])
@pytest.mark.asyncio
async def test_malformed_open_catalog_fails_safely_before_mcp_on_chat_and_verify(
    app_modules, monkeypatch, caplog, catalog, surface
):
    from fastapi import HTTPException

    client_class = _in_process_mcp_client_class(app_specs=_OPEN_TOOL_SPECS)
    catalog_requests = []
    requests = _install_user_bound_sessions(
        monkeypatch,
        app_modules,
        client_class,
        catalog_pages={None: catalog},
        catalog_requests=catalog_requests,
    )
    user = _user('admin', role='admin') if surface == 'admin-verify' else _user('alice')
    connection = _connection({'googledrive': {'tools': []}}, server_id='shared', grants=_user_read_grant(user.id))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], user, [], client_class=client_class
    )
    caplog.set_level(logging.DEBUG)
    if surface == 'admin-verify':
        monkeypatch.setattr(app_modules.configs, 'MCPClient', client_class)
        with pytest.raises(HTTPException) as error:
            await app_modules.configs.verify_tool_servers_config(
                request, app_modules.configs.ToolServerConnection.model_validate(connection), user
            )
        assert error.value.status_code == 400
        assert error.value.detail == 'Composio is unavailable'
    else:
        with pytest.raises(ValueError, match='^Composio is unavailable$'):
            await app_modules.middleware.connect_composio_server(request, 'shared', user)
    assert len(requests) == len(catalog_requests) == 1
    assert client_class.instances == []
    assert 'test-project-key' not in caplog.text
    assert str(catalog_requests[0].url) not in caplog.text
    assert all(session['session_id'] not in caplog.text for session in client_class.sessions.values())


@pytest.mark.parametrize(
    ('failure', 'message'),
    [
        (401, 'Composio API rejected credentials'),
        (403, 'Composio API rejected credentials'),
        (400, 'Composio tool policy was rejected'),
        (422, 'Composio tool policy was rejected'),
        (503, 'Composio is unavailable'),
        ('redirect', 'Composio is unavailable'),
        ('transport', 'Composio is unavailable'),
        ('timeout', 'Composio is unavailable'),
        ('invalid-json', 'Composio is unavailable'),
    ],
)
@pytest.mark.asyncio
async def test_open_catalog_errors_never_retry_broaden_or_expose_private_data(
    app_modules, monkeypatch, caplog, failure, message
):
    seen = []
    client_class = _in_process_mcp_client_class()
    private = 'private-catalog-session?secret=test-project-key'

    def handler(provider_request):
        seen.append(provider_request)
        if provider_request.method == 'POST':
            response = _session_response('alice')
            response['session_id'] = private
            return httpx.Response(200, json=response, request=provider_request)
        assert provider_request.method == 'GET' and provider_request.url.host == 'backend.composio.dev'
        assert provider_request.url.raw_path.split(b'?')[0] == (
            b'/api/v3.1/tool_router/session/private-catalog-session%3Fsecret%3Dtest-project-key/tools'
        )
        assert provider_request.headers['x-api-key'] == 'test-project-key'
        if failure == 'redirect':
            return httpx.Response(
                307, headers={'location': 'https://attacker.example/collect'}, request=provider_request
            )
        if failure == 'transport':
            raise httpx.ConnectError(f'{private} at {provider_request.url}', request=provider_request)
        if failure == 'timeout':
            raise httpx.ReadTimeout(f'{private} at {provider_request.url}', request=provider_request)
        if failure == 'invalid-json':
            return httpx.Response(
                200, content=f'private malformed response {private}'.encode(), request=provider_request
            )
        return httpx.Response(failure, json={'private': private}, request=provider_request)

    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    connection = _connection(
        {'googledrive': {'tools': []}, 'notion': {'tools': ['NOTION_SEARCH']}},
        server_id='shared',
        grants=_user_read_grant('alice'),
    )
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('alice'), [], client_class=client_class
    )
    caplog.set_level(logging.DEBUG)
    with pytest.raises(ValueError, match=f'^{message}$') as error:
        await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    assert [request.method for request in seen] == ['POST', 'GET']
    assert client_class.instances == []
    assert private not in str(error.value) and private not in caplog.text
    assert 'test-project-key' not in caplog.text
    assert all(str(request.url) not in caplog.text for request in seen)


@pytest.mark.parametrize(
    'failure',
    ['oversize-page', 'oversize-total', 'ceiling-cursor', 'repeated-cursor', 'duplicate-slug', 'union-ceiling'],
)
@pytest.mark.asyncio
async def test_open_catalog_pagination_is_bounded_and_fail_closed(app_modules, monkeypatch, failure):
    first = [{'slug': f'DRIVE_APP_{index}', 'toolkit': {'slug': 'googledrive'}} for index in range(500)]
    second = [{'slug': f'DRIVE_APP_{index}', 'toolkit': {'slug': 'googledrive'}} for index in range(500, 1000)]
    pages = {None: {'items': first, 'next_cursor': 'page-two'}, 'page-two': {'items': second}}
    expected_requests = 2
    if failure == 'oversize-page':
        pages = {None: {'items': first + [second[0]]}}
        expected_requests = 1
    elif failure == 'oversize-total':
        pages['page-two'] = {'items': second[:499], 'next_cursor': 'page-three'}
        pages['page-three'] = {
            'items': [
                second[-1],
                {'slug': 'TOOL_1001', 'toolkit': {'slug': 'googledrive'}},
            ]
        }
        expected_requests = 3
    elif failure == 'ceiling-cursor':
        pages['page-two']['next_cursor'] = 'page-three'
    elif failure == 'repeated-cursor':
        pages = {
            None: {'items': [first[0]], 'next_cursor': 'page-two'},
            'page-two': {'items': [first[1]], 'next_cursor': 'page-two'},
        }
    elif failure == 'duplicate-slug':
        pages = {
            None: {'items': [first[0]], 'next_cursor': 'page-two'},
            'page-two': {'items': [{'slug': first[0]['slug'], 'toolkit': {'slug': 'github'}}]},
        }
    toolkits = {'googledrive': {'tools': []}}
    if failure == 'union-ceiling':
        toolkits['notion'] = {'tools': ['NOTION_SEARCH']}
    client_class = _in_process_mcp_client_class()
    catalog_requests = []
    requests = _install_user_bound_sessions(
        monkeypatch, app_modules, client_class, catalog_pages=pages, catalog_requests=catalog_requests
    )
    connection = _connection(toolkits, server_id='shared', grants=_user_read_grant('alice'))
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('alice'), [], client_class=client_class
    )
    with pytest.raises(ValueError, match='^Composio is unavailable$'):
        await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    assert len(requests) == 1 and len(catalog_requests) == expected_requests
    assert client_class.instances == []


@pytest.mark.asyncio
async def test_resolved_open_callable_snapshot_cannot_be_broadened_or_rebound(app_modules, monkeypatch):
    catalog = copy.deepcopy(_OPEN_TOOL_CATALOG)
    client_class = _in_process_mcp_client_class(app_specs=_OPEN_TOOL_SPECS)
    _install_user_bound_sessions(monkeypatch, app_modules, client_class, catalog_pages={None: {'items': catalog}})
    connection = _connection({'googledrive': {'tools': []}}, server_id='shared', grants=_user_read_grant('alice'))
    connection['config']['function_name_filter_list'] = 'WORKSPACE_FIND_FILE'
    request, _form, _metadata, _model = _prepare_chat_payload(
        monkeypatch, app_modules, [connection], _user('alice'), [], client_class=client_class
    )
    client, specs, session = await app_modules.middleware.connect_composio_server(request, 'shared', _user('alice'))
    assert {spec['name'] for spec in specs} == {'WORKSPACE_FIND_FILE', 'COMPOSIO_MANAGE_CONNECTIONS'}
    make_callable = app_modules.middleware._make_mcp_tool_function
    lookup = make_callable(client, 'WORKSPACE_FIND_FILE', session)
    manager = make_callable(client, 'COMPOSIO_MANAGE_CONNECTIONS', session)
    rejected = {
        slug: make_callable(client, slug, session)
        for slug in ('NOTION_SEARCH', 'GOOGLEDRIVE_FOREIGN', 'GOOGLEDRIVE_UNKNOWN_MCP_ONLY', 'COMPOSIO_SEARCH_TOOLS')
    }
    session['allowed_tools'].clear()
    session['allowed_tools'].update(rejected)
    session['allowed_toolkits'].add('notion')
    session['session_id'] = 'foreign-session'
    connection['composio']['toolkits']['notion'] = {'tools': []}
    catalog.append({'slug': 'GOOGLEDRIVE_UNKNOWN_MCP_ONLY', 'toolkit': {'slug': 'googledrive'}})
    for callable_ in rejected.values():
        with pytest.raises(ValueError, match='^Composio tool policy was rejected$'):
            await callable_()
    # Reserved helpers remain uncallable even if a mutable snapshot gains one.
    reserved = make_callable(client, 'COMPOSIO_SEARCH_TOOLS', session)
    with pytest.raises(ValueError, match='^Composio tool policy was rejected$'):
        await reserved()
    with pytest.raises(ValueError):
        await manager(toolkits=['notion'])
    with pytest.raises(ValueError):
        await manager(toolkits=['googledrive'], session_id='foreign-session')
    assert client.calls == []
    assert await lookup(query='shared document') == {'files': _PRIVATE_DOCUMENTS['alice']}
    result = await manager(toolkits=['googledrive'])
    assert result['connect_link'] == client_class.sessions[client.url]['connect_link']
    assert client.calls[-1] == (
        'COMPOSIO_MANAGE_CONNECTIONS',
        {
            'toolkits': ['googledrive'],
            'session_id': client.session_id,
        },
    )
    await app_modules.middleware.disconnect_mcp_clients({'server:composio:shared': client})
    assert client.closed


@pytest.mark.parametrize('deferred', [False, True])
@pytest.mark.asyncio
async def test_erp_mcp_statement_reconcile_relays_owned_current_turn_bytes(app_modules, monkeypatch, deferred):
    import hashlib
    from email.parser import BytesParser
    from email.policy import default
    from open_webui.utils.session_pool import close_session
    from open_webui.utils import tool_search

    if deferred:

        async def search_config():
            return {'defer_threshold': 0, 'always_loaded': [], 'defer_builtin_tools': False}

        monkeypatch.setattr(tool_search, 'get_tool_search_config', search_config)

    statement = b'No. Rekening,=,1234567890\nNama,=,Fixture\nMata Uang,=,IDR\n'
    statement += b'Tanggal,Keterangan,Cabang,Jumlah,,Saldo\n09/10/2026,fixture,000,100000.00,CR,100000.00\n'
    statement += b'Saldo Awal,=,0.00\nKredit,=,100000.00\nDebet,=,0.00\nSaldo Akhir,=,100000.00\n'
    remote_id = '11111111-1111-4111-8111-111111111111'
    received = []

    def read_body(handler):
        if handler.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            chunks = []
            while True:
                size = int(handler.rfile.readline().split(b';', 1)[0], 16)
                if size == 0:
                    handler.rfile.readline()
                    break
                chunks.append(handler.rfile.read(size))
                handler.rfile.read(2)
            return b''.join(chunks)
        return handler.rfile.read(int(handler.headers.get('Content-Length', '0')))

    class Receiver(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_POST(self):
            assert self.path == '/uploads/statements'
            body = read_body(self)
            message = BytesParser(policy=default).parsebytes(
                b'MIME-Version: 1.0\r\n' + f'Content-Type: {self.headers["Content-Type"]}\r\n\r\n'.encode() + body
            )
            part = next(part for part in message.walk() if part.get_content_disposition() == 'form-data')
            received.append(
                {
                    'authorization': self.headers.get('Authorization'),
                    'filename': part.get_filename(),
                    'bytes': part.get_payload(decode=True),
                }
            )
            payload = json.dumps(
                {
                    'file_id': remote_id,
                    'sha256': hashlib.sha256(statement).hexdigest(),
                    'expires_at': '2099-01-01T00:00:00Z',
                }
            ).encode()
            self.send_response(201)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        server_id = 'wilopo-erp'
        file_id = 'web-file-1'
        upload_path = Path(app_modules.erp_mcp.UPLOAD_DIR) / 'fixture-bank.csv'
        upload_path.parent.mkdir(parents=True, exist_ok=True)
        upload_path.write_bytes(statement)

        class FileStore:
            async def get_file_by_id_and_user_id(self, requested_id, user_id):
                if requested_id != file_id or user_id != 'alice':
                    return None
                return SimpleNamespace(
                    id=file_id,
                    filename='fixture-bank.csv',
                    path=str(upload_path),
                    meta={'size': len(statement)},
                )

        class StorageStore:
            @staticmethod
            def get_file(path):
                return path

        monkeypatch.setattr(app_modules.erp_mcp, 'Files', FileStore())
        monkeypatch.setattr(app_modules.erp_mcp, 'Storage', StorageStore())

        class OAuthClientManager:
            async def get_oauth_token(self, user_id, client_id):
                assert user_id == 'alice'
                assert client_id == f'mcp:{server_id}'
                return {'access_token': 'erp-user-token'}

        statement_spec = {
            'name': 'statement_reconcile',
            'description': 'Reconcile one statement.',
            'parameters': {
                'type': 'object',
                'properties': {
                    'file_id': {'type': 'string'},
                    'target_account_id': {'type': 'string'},
                },
                'required': ['file_id', 'target_account_id'],
            },
        }
        base_client = _in_process_mcp_client_class(native_specs=[statement_spec])

        class ERPClient(base_client):
            async def connect(self, url, headers=None, auth=None, *, httpx_client_factory=None):
                self.url = url
                self.headers = headers or {}
                self.account_id = 'native'

        connection = {
            'type': 'mcp',
            'url': f'http://127.0.0.1:{server.server_port}/mcp',
            'path': '',
            'auth_type': 'oauth_2.1_static',
            'key': None,
            'headers': {'X-ERP-Context': 'employee'},
            'config': {'enable': True, 'access_grants': _user_read_grant('alice')},
            'info': {'id': server_id, 'name': 'ERP'},
        }
        user = _user('alice')
        request, form_data, metadata, model = _prepare_chat_payload(
            monkeypatch,
            app_modules,
            [connection],
            user,
            [f'server:mcp:{server_id}'],
            hostile_metadata={'user_message': {'files': [{'type': 'file', 'id': file_id}]}},
            client_class=ERPClient,
        )
        request.app.state.oauth_client_manager = OAuthClientManager()
        await app_modules.middleware.process_chat_payload(request, form_data, user, metadata, model)

        tool = metadata['tools'][f'{server_id}_statement_reconcile']
        assert file_id in tool['spec']['description']
        assert 'fixture-bank.csv' in tool['spec']['description']

        async def invoke_reconcile():
            arguments = {'file_id': file_id, 'target_account_id': 'account-1'}
            if not deferred:
                return await tool['callable'](**arguments)
            assert tool['spec']['name'] in metadata['deferred_tools']
            call = {
                'function': {
                    'name': 'call_tool',
                    'arguments': json.dumps({'tool_name': tool['spec']['name'], 'params': json.dumps(arguments)}),
                }
            }
            assert app_modules.middleware.resolve_tool_call(call, metadata['tools']) is None
            _params, result, *_ = await app_modules.middleware.execute_tool_call(
                form_data, metadata, None, call['function']
            )
            return result

        result = await invoke_reconcile()
        assert result == {'provider': 'native', 'tool': 'statement_reconcile'}
        second_result = await invoke_reconcile()
        assert second_result == {'provider': 'native', 'tool': 'statement_reconcile'}
        assert received == [
            {
                'authorization': 'Bearer erp-user-token',
                'filename': 'fixture-bank.csv',
                'bytes': statement,
            }
        ]
        assert hashlib.sha256(received[0]['bytes']).hexdigest() == hashlib.sha256(statement).hexdigest()
        assert ERPClient.instances[0].calls == [
            ('statement_reconcile', {'file_id': remote_id, 'target_account_id': 'account-1'}),
            ('statement_reconcile', {'file_id': remote_id, 'target_account_id': 'account-1'}),
        ]
    finally:
        await close_session()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize(
    'case', ['foreign', 'shared', 'stale', 'temporary', 'non_csv', 'filesystem', 'oversize', 'actual_oversize']
)
@pytest.mark.asyncio
async def test_erp_mcp_statement_reconcile_rejects_untrusted_attachment_references(app_modules, monkeypatch, case):
    server_id = 'wilopo-erp'
    requested_id = f'{case}-file'
    current_id = 'current-file'
    upload_dir = Path(app_modules.erp_mcp.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)
    valid_path = upload_dir / f'{case}-fixture.csv'
    valid_path.write_bytes(b'fixture,csv\n')
    rows = {
        current_id: SimpleNamespace(
            id=current_id,
            filename='current.csv',
            path=str(valid_path),
            meta={'size': valid_path.stat().st_size},
        ),
        requested_id: SimpleNamespace(
            id=requested_id,
            filename='requested.csv',
            path=str(valid_path),
            meta={'size': valid_path.stat().st_size},
        ),
    }
    if case == 'non_csv':
        rows[current_id].filename = 'statement.txt'
    elif case == 'filesystem':
        rows[current_id].path = str(Path(app_modules.erp_mcp.__file__).resolve())
    elif case == 'oversize':
        rows[current_id].meta = {'size': 10 * 1024 * 1024 + 1}
    elif case == 'actual_oversize':
        valid_path.write_bytes(b'x' * (10 * 1024 * 1024 + 1))
        rows[current_id].meta = {'size': valid_path.stat().st_size}

    class FileStore:
        async def get_file_by_id_and_user_id(self, file_id, user_id):
            if user_id != 'alice' or case in {'foreign', 'shared'}:
                return None
            return rows.get(file_id)

    class StorageStore:
        @staticmethod
        def get_file(path):
            return path

    monkeypatch.setattr(app_modules.erp_mcp, 'Files', FileStore())
    monkeypatch.setattr(app_modules.erp_mcp, 'Storage', StorageStore())

    async def forbidden_session():
        raise AssertionError('untrusted attachment must not reach the upload session')

    monkeypatch.setattr(app_modules.erp_mcp, 'get_session', forbidden_session)

    class OAuthClientManager:
        async def get_oauth_token(self, _user_id, _client_id):
            return {'access_token': 'erp-user-token'}

    statement_spec = {
        'name': 'statement_reconcile',
        'description': 'Reconcile one statement.',
        'parameters': {
            'type': 'object',
            'properties': {'file_id': {'type': 'string'}, 'target_account_id': {'type': 'string'}},
        },
    }
    base_client = _in_process_mcp_client_class(native_specs=[statement_spec])

    class ERPClient(base_client):
        async def connect(self, url, headers=None, auth=None, *, httpx_client_factory=None):
            self.url = url
            self.headers = headers or {}
            self.account_id = 'native'

    connection = {
        'type': 'mcp',
        'url': 'http://127.0.0.1:9/mcp',
        'path': '',
        'auth_type': 'oauth_2.1_static',
        'key': None,
        'headers': None,
        'config': {'enable': True, 'access_grants': _user_read_grant('alice')},
        'info': {'id': server_id, 'name': 'ERP'},
    }
    invocation_id = requested_id if case in {'foreign', 'shared', 'stale'} else current_id
    user_message = {
        'files': ([{'type': 'text', 'id': current_id}] if case == 'temporary' else [{'type': 'file', 'id': current_id}])
    }
    request, form_data, metadata, model = _prepare_chat_payload(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        [f'server:mcp:{server_id}'],
        hostile_metadata={'user_message': user_message},
        client_class=ERPClient,
    )
    request.app.state.oauth_client_manager = OAuthClientManager()
    await app_modules.middleware.process_chat_payload(request, form_data, _user('alice'), metadata, model)

    tool = metadata['tools'][f'{server_id}_statement_reconcile']
    assert invocation_id not in tool['spec']['description']
    with pytest.raises(ValueError, match='current-turn CSV attachment'):
        await tool['callable'](file_id=invocation_id, target_account_id='account-1')
    assert ERPClient.instances[0].calls == []


@pytest.mark.parametrize(
    ('auth_type', 'headers'),
    [
        ('none', None),
        ('oauth_2.1', None),
        ('oauth_2.1_static', {'aUtHoRiZaTiOn': 'shared-token'}),
    ],
)
@pytest.mark.asyncio
async def test_erp_mcp_connection_rejects_non_static_or_authorization_override(
    app_modules, monkeypatch, auth_type, headers
):
    server_id = 'wilopo-erp'
    connection = {
        'type': 'mcp',
        'url': 'http://127.0.0.1:9/mcp',
        'path': '',
        'auth_type': auth_type,
        'key': None,
        'headers': headers,
        'config': {'enable': True, 'access_grants': _user_read_grant('alice')},
        'info': {'id': server_id, 'name': 'ERP'},
    }
    client_class = _in_process_mcp_client_class()
    request, _form, metadata, _model = _prepare_chat_payload(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        [f'server:mcp:{server_id}'],
        client_class=client_class,
    )
    with pytest.raises(ValueError, match='ERP MCP server configuration'):
        await app_modules.tool_utils.connect_mcp_server(
            request, server_id, _user('alice'), {'__metadata__': metadata}
        )
    assert client_class.instances == []


@pytest.mark.parametrize('failure', ['missing-token', 'redirect', 'oversize-response', 'invalid-response'])
@pytest.mark.asyncio
async def test_erp_mcp_statement_reconcile_blocks_relay_failures(app_modules, monkeypatch, failure):
    from open_webui.utils.session_pool import close_session

    statement = b'No. Rekening,=,123\nNama,=,Fixture\nMata Uang,=,IDR\n'
    statement_path = Path(app_modules.erp_mcp.UPLOAD_DIR) / f'{failure}.csv'
    statement_path.parent.mkdir(parents=True, exist_ok=True)
    statement_path.write_bytes(statement)
    received_paths = []

    def consume_body(handler):
        if handler.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            while True:
                size = int(handler.rfile.readline().split(b';', 1)[0], 16)
                if size == 0:
                    handler.rfile.readline()
                    return
                handler.rfile.read(size)
                handler.rfile.read(2)
        else:
            handler.rfile.read(int(handler.headers.get('Content-Length', '0')))

    class Receiver(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_POST(self):
            received_paths.append(self.path)
            consume_body(self)
            if failure == 'redirect':
                self.send_response(302)
                self.send_header('Location', '/uploads/statements/redirected')
                self.end_headers()
                return
            body = (
                b'x' * (app_modules.erp_mcp.MAX_UPLOAD_RESPONSE_BYTES + 1)
                if failure == 'oversize-response'
                else (b'{"file_id":"not-a-uuid"}' if failure == 'invalid-response' else b'{}')
            )
            self.send_response(201)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        file_id = 'relay-failure-file'

        class FileStore:
            async def get_file_by_id_and_user_id(self, requested_id, user_id):
                if requested_id != file_id or user_id != 'alice':
                    return None
                return SimpleNamespace(
                    id=file_id,
                    filename='statement.csv',
                    path=str(statement_path),
                    meta={'size': len(statement)},
                )

        class StorageStore:
            @staticmethod
            def get_file(path):
                return path

        monkeypatch.setattr(app_modules.erp_mcp, 'Files', FileStore())
        monkeypatch.setattr(app_modules.erp_mcp, 'Storage', StorageStore())

        class OAuthClientManager:
            async def get_oauth_token(self, _user_id, _client_id):
                return None if failure == 'missing-token' else {'access_token': 'erp-user-token'}

        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(oauth_client_manager=OAuthClientManager())))
        calls = []

        async def delegate(**kwargs):
            calls.append(kwargs)
            return {'unexpected': True}

        spec, callable_ = await app_modules.erp_mcp.bind_statement_reconcile_tool(
            request,
            {
                'type': 'mcp',
                'url': f'http://127.0.0.1:{server.server_port}/mcp',
                'auth_type': 'oauth_2.1_static',
            },
            _user('alice'),
            {'files': [{'type': 'file', 'id': file_id}]},
            {
                'name': 'wilopo-erp_statement_reconcile',
                'description': 'Reconcile.',
                'parameters': {'type': 'object', 'properties': {'file_id': {'type': 'string'}}},
            },
            delegate,
            'wilopo-erp',
        )
        assert file_id in spec['description']
        with pytest.raises(ValueError):
            await callable_(file_id=file_id, target_account_id='account-1')
        assert calls == []
        assert received_paths == ([] if failure == 'missing-token' else ['/uploads/statements'])
    finally:
        await close_session()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_disabled_tool_servers_hide_catalog_and_block_native_and_composio_resolution(app_modules, monkeypatch):
    client_class = _in_process_mcp_client_class()
    requests = _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    user = _user('alice')
    connection = _connection(server_id='shared', grants=_user_read_grant('alice'))
    request, form_data, metadata, model = _prepare_chat_payload(
        monkeypatch,
        app_modules,
        [connection, _native_connection()],
        user,
        ['server:composio:shared', 'server:mcp:shared'],
        client_class=client_class,
    )
    for module in (app_modules.middleware, app_modules.tools, app_modules.tool_utils):
        monkeypatch.setattr(module, 'ENABLE_TOOL_SERVERS', False)
    assert await app_modules.tools.get_tools(request, user=user, db=None) == []
    assert await app_modules.middleware.connect_composio_server(request, 'shared', user) is None
    assert await app_modules.tool_utils.connect_mcp_server(request, 'shared', user, {}) is None
    await app_modules.middleware.process_chat_payload(request, form_data, user, metadata, model)
    assert not metadata.get('tools')
    assert client_class.instances == []
    assert requests == []


@pytest.mark.asyncio
async def test_deferred_composio_dispatch_keeps_user_scope_and_rejects_hostile_manager_arguments(
    app_modules, monkeypatch
):
    from open_webui.utils import tool_search

    async def search_config():
        return {'defer_threshold': 0, 'always_loaded': [], 'defer_builtin_tools': False}

    monkeypatch.setattr(tool_search, 'get_tool_search_config', search_config)
    client_class = _in_process_mcp_client_class()
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    connection = _connection(server_id='shared', grants=_user_read_grant('alice') + _user_read_grant('bob'))
    for user_id in ('alice', 'bob'):
        _request, form_data, metadata, _ = await _process_chat_with_servers(
            monkeypatch,
            app_modules,
            [connection],
            _user(user_id),
            ['server:composio:shared'],
            client_class=client_class,
        )
        assert {tool['function']['name'] for tool in form_data['tools']} == {'search_tools', 'call_tool'}
        app_name = _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE')
        manager_name = _composio_function_name('shared', 'COMPOSIO_MANAGE_CONNECTIONS')
        assert app_name in metadata['deferred_tools'] and manager_name in metadata['deferred_tools']

        def tool_call(name, params):
            call = {'function': {'name': name, 'arguments': json.dumps(params)}}
            assert app_modules.middleware.resolve_tool_call(call, metadata['tools']) is None
            return call['function']

        _params, found, *_ = await app_modules.middleware.execute_tool_call(
            form_data, metadata, None, tool_call('search_tools', {'query': 'GOOGLEDRIVE_FIND_FILE'})
        )
        assert app_name in {spec['name'] for spec in json.loads(found)['tools']}
        for arguments in (
            {'toolkits': ['notion']},
            {'toolkits': ['googledrive'], 'session_id': 'foreign-session'},
            {'toolkits': ['googledrive'], 'unknown': True},
        ):
            _params, result, *_ = await app_modules.middleware.execute_tool_call(
                form_data,
                metadata,
                None,
                tool_call('call_tool', {'tool_name': manager_name, 'params': json.dumps(arguments)}),
            )
            assert result.get('error')
            assert client_class.instances[-1].calls == []
        _params, result, *_ = await app_modules.middleware.execute_tool_call(
            form_data,
            metadata,
            None,
            tool_call('call_tool', {'tool_name': app_name, 'params': json.dumps({'query': 'shared document'})}),
        )
        assert result == {'files': _PRIVATE_DOCUMENTS[user_id]}
        await app_modules.middleware.disconnect_mcp_clients(metadata['mcp_clients'])


@pytest.mark.asyncio
async def test_native_mcp_retains_instructions_long_names_elicitation_and_live_system_oauth(app_modules, monkeypatch):
    names = ['tool_' + 'x' * 75 + suffix for suffix in ('first', 'second')]
    specs = [
        {'name': name, 'description': 'Native tool.', 'parameters': {'type': 'object', 'properties': {}}}
        for name in names
    ]
    base_client = _in_process_mcp_client_class(native_specs=specs)

    class NativeClient(base_client):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.instructions = 'Use the native server instructions.'

    user = _user('alice')
    connection = {**_native_connection(), 'auth_type': 'system_oauth'}
    request, form_data, metadata, model = _prepare_chat_payload(
        monkeypatch,
        app_modules,
        [connection],
        user,
        ['server:mcp:shared'],
        hostile_metadata={'session_id': 'test-browser'},
        client_class=NativeClient,
    )

    async def event_call(_event):
        return {'action': 'cancel'}

    async def get_event_call(_metadata):
        return event_call

    tokens = iter(['first-token', 'refreshed-token'])

    async def get_oauth_token(_request, current_user):
        assert current_user.id == user.id
        return {'access_token': next(tokens)}

    monkeypatch.setattr(app_modules.middleware, 'get_event_call', get_event_call)
    monkeypatch.setattr(app_modules.tool_utils, 'get_system_oauth_token', get_oauth_token)
    await app_modules.middleware.process_chat_payload(request, form_data, user, metadata, model)
    client = NativeClient.instances[0]
    assert client.event_caller is event_call
    assert any(client.instructions in message.get('content', '') for message in form_data['messages'])
    assert len(metadata['tools']) == 2
    assert all(len(name) <= 64 for name in metadata['tools'])
    results = [await tool['callable']() for tool in metadata['tools'].values()]
    assert {result['tool'] for result in results} == set(names)

    authorizations = []

    def transport(http_request):
        authorizations.append(http_request.headers['Authorization'])
        return httpx2.Response(200)

    async with httpx2.AsyncClient(transport=httpx2.MockTransport(transport), auth=client.auth) as http:
        await http.post('https://native.example/mcp', json={})
        await http.post('https://native.example/mcp', json={})
    assert authorizations == ['Bearer first-token', 'Bearer refreshed-token']
    await app_modules.middleware.disconnect_mcp_clients(metadata['mcp_clients'])


@pytest.mark.asyncio
async def test_full_config_import_preserves_upstream_mfa_authorization_guard(app_modules, config_import_client):
    Config = app_modules.configs.Config
    before = await Config.get_all()
    response = await config_import_client.post(
        '/configs/import',
        json={
            'config': {
                'auth.mfa.enable': True,
                'ui.default_models': 'must-not-change',
                'tool_server.connections': [_connection()],
            }
        },
    )
    assert response.status_code == 403
    assert await Config.get_all() == before


@pytest.mark.parametrize('composio_mode', [False, True])
@pytest.mark.asyncio
async def test_mcp_httpx2_transport_negotiates_legacy_tools_and_blocks_cross_origin_redirects(
    app_modules, monkeypatch, composio_mode
):
    from open_webui.utils.mcp import client as mcp_client

    monkeypatch.setenv('NO_PROXY', '*')
    received = []
    redirected = []
    arguments = {'query': 'account document', 'server_owned': {'enabled': True}}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            message = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if self.path in ('/401', '/403'):
                self.send_response(int(self.path[1:]))
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            if self.path == '/redirect':
                self.send_response(307)
                self.send_header('Location', f'http://localhost:{self.server.server_port}/collect')
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            if self.path == '/collect':
                redirected.append(dict(self.headers))
            received.append((message, self.headers))
            if message['method'] == 'tools/call' and message['params']['arguments'].get('fail_transport'):
                import socket

                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            method = message['method']
            if method == 'notifications/initialized':
                self.send_response(202)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            response = {'jsonrpc': '2.0', 'id': message['id']}
            if method == 'server/discover':
                response['error'] = {'code': -32601, 'message': 'Legacy server'}
            elif method == 'initialize':
                response['result'] = {
                    'protocolVersion': '2025-06-18',
                    'capabilities': {'tools': {}},
                    'serverInfo': {'name': 'fixture', 'version': '1'},
                    'instructions': 'Use account-scoped tools.',
                }
            elif method == 'tools/list':
                response['result'] = {
                    'tools': [{'name': 'echo', 'inputSchema': {'type': 'object', 'additionalProperties': True}}]
                }
            elif method == 'tools/call':
                response['result'] = {
                    'content': [{'type': 'text', 'text': json.dumps(message['params']['arguments'])}],
                    'structuredContent': message['params']['arguments'],
                }
            else:
                response['error'] = {'code': -32601, 'message': 'Unknown method'}
            body = json.dumps(response).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = mcp_client.MCPClient()
    factory = app_modules.composio.create_composio_mcp_httpx_client if composio_mode else None
    headers = {'x-api-key': 'test-project-key'} if composio_mode else None
    auth = None if composio_mode else httpx2.BasicAuth('fixture', 'password')
    url = f'http://127.0.0.1:{server.server_port}'
    try:
        await client.connect(f'{url}/mcp', headers=headers, auth=auth, httpx_client_factory=factory)
        assert client.instructions == 'Use account-scoped tools.'
        assert [spec['name'] for spec in await client.list_tool_specs()] == ['echo']
        result = await client.call_tool('echo', arguments)
        assert len(result) == 1
        assert json.loads(result[0]['text']) == arguments
        assert any(message['method'] == 'initialize' for message, _headers in received)
        for _message, request_headers in received:
            if composio_mode:
                assert request_headers['x-api-key'] == 'test-project-key'
                assert request_headers.get('Authorization') is None
            else:
                assert request_headers['Authorization'] == 'Basic Zml4dHVyZTpwYXNzd29yZA=='
                assert request_headers.get('x-api-key') is None
        with pytest.raises(Exception):
            await client.call_tool('echo', {'fail_transport': True})
        await asyncio.sleep(0)
        assert asyncio.current_task().cancelling() == 0
        await client.disconnect()
        with pytest.raises(RuntimeError, match='not connected'):
            await client.call_tool('echo', arguments)
        with pytest.raises(Exception):
            await client.connect(f'{url}/redirect', headers=headers, auth=auth, httpx_client_factory=factory)
        assert redirected == []
        for status in (401, 403):
            with pytest.raises(mcp_client.MCPConnectionHTTPError) as failure:
                await client.connect(f'{url}/{status}', headers=headers, auth=auth, httpx_client_factory=factory)
            assert failure.value.status_code == status
    finally:
        await client.disconnect()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize('factory_name', ['create_composio_httpx_client', 'create_composio_mcp_httpx_client'])
@pytest.mark.asyncio
async def test_composio_transport_rejects_untrusted_tls_despite_native_override(
    app_modules, monkeypatch, tmp_path, factory_name
):
    import ipaddress
    import ssl
    from datetime import datetime, timedelta, timezone

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    from open_webui.utils.mcp import client as mcp_client

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
        .sign(key, hashes.SHA256())
    )
    cert_path = tmp_path / 'server.pem'
    key_path = tmp_path / 'server-key.pem'
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Length', '2')
            self.end_headers()
            self.wfile.write(b'ok')

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(cert_path), str(key_path))
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv('NO_PROXY', '*')
    monkeypatch.setattr(mcp_client, 'AIOHTTP_CLIENT_SESSION_TOOL_SERVER_SSL', False)
    url = f'https://127.0.0.1:{server.server_port}/mcp'
    try:
        async with mcp_client.create_httpx_client() as native:
            assert (await native.get(url)).text == 'ok'
        async with getattr(app_modules.composio, factory_name)() as composio:
            with pytest.raises((httpx.ConnectError, httpx2.ConnectError)):
                await composio.get(url)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize('dispatch', ['native', 'legacy'])
@pytest.mark.parametrize('tool_type', ['mcp', 'local'])
@pytest.mark.asyncio
async def test_tool_dispatch_preserves_server_owned_mcp_arguments(app_modules, monkeypatch, dispatch, tool_type):
    middleware = app_modules.middleware
    received = []
    arguments = {'declared': 'value', 'server_owned': {'enabled': True}}

    async def tool(**kwargs):
        received.append(kwargs)
        return [{'type': 'text', 'text': 'ok'}]

    tools = {
        'echo': {
            'spec': {'name': 'echo', 'parameters': {'type': 'object', 'properties': {'declared': {'type': 'string'}}}},
            'callable': tool,
            'type': tool_type,
        }
    }
    body = {'model': 'fixture', 'messages': [{'role': 'user', 'content': 'Echo the arguments.'}]}
    if dispatch == 'native':
        await middleware.execute_tool_call(
            body, {'tools': tools}, None, {'name': 'echo', 'arguments': json.dumps(arguments)}
        )
    else:
        async def config_get_many(*_keys):
            return {'task.model.default': None, 'task.model.external': None, 'task.tools.prompt_template': ''}

        async def completion(*_args, **_kwargs):
            return {'choices': [{'message': {'content': json.dumps({'name': 'echo', 'parameters': arguments})}}]}

        monkeypatch.setattr(middleware.Config, 'get_many', staticmethod(config_get_many))
        monkeypatch.setattr(middleware, 'generate_chat_completion', completion)
        await middleware.chat_completion_tools_handler(
            None,
            body,
            {'__event_call__': None, '__event_emitter__': None, '__metadata__': {}},
            _user('alice'),
            {'fixture': {'owned_by': 'openai'}},
            tools,
        )
    assert received == [arguments if tool_type == 'mcp' else {'declared': 'value'}]


_DRIVE_UPLOAD_SPEC = {
    'name': 'GOOGLEDRIVE_UPLOAD_FILE',
    'description': 'Upload a new file to Google Drive.',
    'parameters': {
        'type': 'object',
        'properties': {
            'file_to_upload': {'$ref': '#/$defs/FileUploadable'},
            'folder_to_upload_to': {'type': 'string', 'description': 'Destination Drive folder ID.'},
        },
        'required': ['file_to_upload'],
        'additionalProperties': False,
        '$defs': {
            'FileUploadable': {
                'type': 'object',
                'properties': {name: {'type': 'string'} for name in ('name', 'mimetype', 's3key')},
                'required': ['name', 'mimetype', 's3key'],
                'file_uploadable': True,
            }
        },
    },
}


@pytest.fixture
def composio_attachment(app_modules, monkeypatch, tmp_path):
    module = app_modules.composio_files
    content = b'%PDF-1.7\n\x00binary attachment\xff\n'
    path = tmp_path / 'stored-file'
    path.write_bytes(content)
    file_id = 'current-upload-file'
    record = SimpleNamespace(
        id=file_id,
        user_id='alice',
        filename='report.pdf',
        path=str(path),
        meta={'size': len(content), 'content_type': 'application/pdf'},
    )
    state = SimpleNamespace(
        module=module,
        content=content,
        path=path,
        file_id=file_id,
        record=record,
        rows={file_id: record},
        readable=True,
        lookups=[],
        authorizations=[],
        storage_reads=[],
        delegated=[],
    )

    class FileStore:
        async def get_file_by_id(self, requested_id):
            state.lookups.append(requested_id)
            return state.rows.get(requested_id)

    class StorageStore:
        @staticmethod
        def get_file(stored_path):
            state.storage_reads.append(stored_path)
            return stored_path

    async def authorize(requested_id, access_type, user):
        state.authorizations.append((requested_id, access_type, user.id))
        return state.readable

    async def delegate(**kwargs):
        state.delegated.append(copy.deepcopy(kwargs))
        return {'successful': True}

    state.delegate = delegate
    monkeypatch.setattr(module, 'UPLOAD_DIR', tmp_path)
    monkeypatch.setattr(module, 'Files', FileStore())
    monkeypatch.setattr(module, 'Storage', StorageStore())
    monkeypatch.setattr(module, 'has_access_to_file', authorize)
    return state


def _bind_composio_attachment(attachment, *, user_message=None, delegate=None, spec=None):
    return attachment.module.bind_composio_upload_tool(
        'test-project-key',
        _user('alice'),
        user_message if user_message is not None else {'files': [{'type': 'file', 'id': attachment.file_id}]},
        spec if spec is not None else _DRIVE_UPLOAD_SPEC,
        delegate or attachment.delegate,
    )


@pytest.mark.parametrize('requires_approval', [False, True])
@pytest.mark.asyncio
async def test_composio_attachment_upload_honors_approval_review_and_display_labels(
    app_modules, composio_attachment, monkeypatch, requires_approval
):
    from open_webui.utils.tool_calling import add_tool_call_metadata

    async def config_get_many(*_keys):
        return {
            'chat.tool_permissions.enable': True,
            'chat.tool_permissions.auto.enable': True,
            'chat.tool_permissions.auto.engine': '',
        }

    monkeypatch.setattr(app_modules.middleware.Config, 'get_many', staticmethod(config_get_many))
    spec, callable_ = _bind_composio_attachment(composio_attachment)
    tools = {spec['name']: {'spec': spec, 'callable': callable_, 'type': 'mcp'}}
    add_tool_call_metadata(tools, labels=True, approval=True)
    staged_file = {'name': 'approved.pdf', 'mimetype': 'application/pdf', 's3key': 'already-staged-key'}
    labels = {'running': 'Uploading report', 'completed': 'Uploaded report'}
    call = {
        'name': spec['name'],
        'arguments': json.dumps(
            {
                'file_to_upload': staged_file,
                '__approval__': {'required': requires_approval},
                '__labels__': labels,
            }
        ),
    }
    metadata = {'tools': tools, 'params': {'tool_approval_mode': 'auto'}}
    await app_modules.middleware.execute_tool_call({'messages': []}, metadata, None, call)
    assert call['display_labels'] == labels
    assert call['status'] == ('failed' if requires_approval else 'completed')
    assert composio_attachment.delegated == ([] if requires_approval else [{'file_to_upload': staged_file}])
    assert composio_attachment.lookups == composio_attachment.storage_reads == []


@pytest.mark.parametrize(
    'mutation',
    [
        lambda schema: schema['properties'].pop('file_to_upload'),
        lambda schema: schema['properties'].update({'file_id': {'type': 'string'}}),
        lambda schema: schema.update({'required': []}),
        lambda schema: schema.update({'anyOf': [{'required': ['file_to_upload']}]}),
        lambda schema: schema.update({'properties': []}),
        lambda schema: schema.update({'required': 'file_to_upload'}),
    ],
)
def test_composio_attachment_upload_fails_closed_on_provider_schema_drift(composio_attachment, mutation):
    spec = copy.deepcopy(_DRIVE_UPLOAD_SPEC)
    mutation(spec['parameters'])
    with pytest.raises(ValueError):
        _bind_composio_attachment(composio_attachment, spec=spec)
    assert composio_attachment.lookups == []


@pytest.mark.parametrize(
    ('attachment_type', 'filename', 'mimetype'),
    [('file', 'report.pdf', 'application/pdf'), ('image', 'photo.png', 'image/png')],
)
@pytest.mark.parametrize(('storage_backend', 'deferred'), [('s3', False), ('azure_blob_storage', True)])
@pytest.mark.asyncio
async def test_composio_attachment_upload_preserves_bytes_folder_user_session_and_approval(
    app_modules,
    composio_attachment,
    monkeypatch,
    caplog,
    storage_backend,
    deferred,
    attachment_type,
    filename,
    mimetype,
):
    import hashlib

    from open_webui.utils import tool_search

    caplog.set_level(logging.DEBUG)
    if attachment_type == 'image':
        composio_attachment.content = b'\x89PNG\r\n\x1a\nsaved image bytes\x00\xff'
        composio_attachment.path.write_bytes(composio_attachment.content)
    composio_attachment.record.filename = filename
    composio_attachment.record.meta = {'size': len(composio_attachment.content), 'content_type': mimetype}
    if deferred:

        async def search_config():
            return {'defer_threshold': 0, 'always_loaded': [], 'defer_builtin_tools': False}

        monkeypatch.setattr(tool_search, 'get_tool_search_config', search_config)
    base_client = _in_process_mcp_client_class(app_specs=[_DRIVE_UPLOAD_SPEC, _APP_TOOL_SPECS[-1]])

    class UploadClient(base_client):
        async def call_tool(self, function_name, function_args):
            if function_name != 'GOOGLEDRIVE_UPLOAD_FILE':
                return await super().call_tool(function_name, function_args)
            self.calls.append((function_name, copy.deepcopy(function_args)))
            return {'uploaded_by': self.sessions[self.url]['account_id'], 'drive_file_id': 'drive-result'}

    connection = _connection(
        {'googledrive': {'tools': ['GOOGLEDRIVE_UPLOAD_FILE']}},
        server_id='uploads',
        grants=_user_read_grant('alice') + _user_read_grant('bob'),
    )
    sessions = _install_user_bound_sessions(monkeypatch, app_modules, UploadClient)
    chats = []
    for user_id in ('alice', 'bob'):
        _request, form, metadata, _client_class = await _process_chat_with_servers(
            monkeypatch,
            app_modules,
            [connection],
            _user(user_id),
            ['server:composio:uploads'],
            hostile_metadata={
                'user_id': 'mallory',
                'composio_user_id': 'mallory',
                'user_message': {
                    'files': [
                        {
                            'type': attachment_type,
                            'id': composio_attachment.file_id,
                            'name': 'untrusted-name.exe',
                            'content_type': 'text/html',
                            'path': '/etc/passwd',
                            'url': 'https://attacker.example/payload',
                            'size': 1,
                        }
                    ]
                },
            },
            client_class=UploadClient,
        )
        chats.append((form, metadata))
    assert [json.loads(request.content)['user_id'] for request in sessions] == ['openwebui:alice', 'openwebui:bob']
    assert composio_attachment.lookups == []
    stage_requests = []
    puts = []
    private_key = 'projects/private-project/requests/googledrive/private-staged-file'
    private_url = f'https://storage.composio.dev/{private_key}?signature=private-upload-signature'

    def handler(request):
        if request.method == 'POST':
            stage_requests.append(request)
            assert str(request.url) == 'https://backend.composio.dev/api/v3.1/files/upload/request'
            assert request.headers['x-api-key'] == 'test-project-key'
            assert 'authorization' not in request.headers and 'cookie' not in request.headers
            assert json.loads(request.content) == {
                'toolkit_slug': 'googledrive',
                'tool_slug': 'GOOGLEDRIVE_UPLOAD_FILE',
                'filename': filename,
                'mimetype': mimetype,
                'md5': hashlib.md5(composio_attachment.content, usedforsecurity=False).hexdigest(),
            }
            # The PUT must use the already-read bytes, even if storage changes after hashing.
            composio_attachment.path.write_bytes(b'changed after the upload request')
            return httpx.Response(
                200,
                json={
                    'key': private_key,
                    'new_presigned_url': private_url,
                    'metadata': {'storage_backend': storage_backend},
                },
            )
        assert request.method == 'PUT'
        puts.append(request)
        assert str(request.url) == private_url
        assert request.content == composio_attachment.content
        assert request.headers['Content-Type'] == mimetype
        assert not {'x-api-key', 'authorization', 'cookie'} & set(request.headers)
        expected_blob_type = 'BlockBlob' if storage_backend == 'azure_blob_storage' else None
        assert request.headers.get('x-ms-blob-type') == expected_blob_type
        composio_attachment.path.write_bytes(composio_attachment.content)
        return httpx.Response(201 if storage_backend == 'azure_blob_storage' else 200)

    def async_client(*args, **kwargs):
        assert kwargs['verify'] is True
        assert kwargs['follow_redirects'] is False
        kwargs['transport'] = httpx.MockTransport(handler)
        return _HTTPX_ASYNC_CLIENT(*args, **kwargs)

    monkeypatch.setattr(app_modules.composio.httpx, 'AsyncClient', async_client)
    tool_name = _composio_function_name('uploads', 'GOOGLEDRIVE_UPLOAD_FILE')

    async def invoke(form, metadata):
        arguments = {'file_to_upload': {'file_id': composio_attachment.file_id}, 'folder_to_upload_to': 'target-folder'}
        call = {'function': {'name': tool_name, 'arguments': json.dumps(arguments)}}
        if deferred:
            assert tool_name in metadata['deferred_tools']
            call['function'] = {
                'name': 'call_tool',
                'arguments': json.dumps({'tool_name': tool_name, 'params': json.dumps(arguments)}),
            }
            assert app_modules.middleware.resolve_tool_call(call, metadata['tools']) is None
        return await app_modules.middleware.execute_tool_call(form, metadata, None, call['function'])

    async def require_approval(*_args, **_kwargs):
        return True

    monkeypatch.setattr(app_modules.middleware, 'requires_tool_approval', require_approval)
    _params, denied, *_rest = await invoke(*chats[0])
    assert app_modules.middleware._is_tool_result_error(denied)
    assert stage_requests == puts == composio_attachment.lookups == []

    async def approved(*_args, **_kwargs):
        return False

    monkeypatch.setattr(app_modules.middleware, 'requires_tool_approval', approved)
    for (form, metadata), user_id, client in zip(chats, ('alice', 'bob'), UploadClient.instances):
        params, result, *_rest = await invoke(form, metadata)
        assert params == {
            'file_to_upload': {'file_id': composio_attachment.file_id},
            'folder_to_upload_to': 'target-folder',
        }
        assert result == {'uploaded_by': user_id, 'drive_file_id': 'drive-result'}
        assert client.calls == [
            (
                'GOOGLEDRIVE_UPLOAD_FILE',
                {
                    'file_to_upload': {'name': filename, 'mimetype': mimetype, 's3key': private_key},
                    'folder_to_upload_to': 'target-folder',
                },
            )
        ]
        assert private_key not in json.dumps([metadata['tools'][tool_name]['spec'], params, result])
    assert len(stage_requests) == len(puts) == 2
    assert composio_attachment.authorizations == [
        (composio_attachment.file_id, 'read', 'alice'),
        (composio_attachment.file_id, 'read', 'bob'),
    ]
    assert UploadClient.instances[0].session_id != UploadClient.instances[1].session_id
    assert private_key not in caplog.text and private_url not in caplog.text
    await app_modules.middleware.disconnect_mcp_clients(
        {f'server:composio:uploads-{index}': client for index, client in enumerate(UploadClient.instances)}
    )


@pytest.mark.parametrize(
    'case',
    [
        'historical',
        'readable-knowledge-only',
        'missing-record',
        'unreadable',
        'revoked-after-binding',
        'url',
        'uppercase-url',
        'path-id',
        'windows-path-id',
        'data',
        'text',
        'unsaved-image',
        'base64-image',
        'external-image',
        'missing-image-record',
        'unreadable-image',
        'missing-type',
        'malformed-files',
        'metadata-oversize',
        'actual-oversize',
        'empty',
        'size-mismatch',
        'invalid-size',
        'filesystem',
        'symlink',
        'missing-path',
    ],
)
@pytest.mark.asyncio
async def test_composio_attachment_upload_rejects_untrusted_files_before_staging(
    composio_attachment, monkeypatch, tmp_path, case
):
    attachment = composio_attachment
    file_id = attachment.file_id
    entry = {'type': 'file', 'id': file_id}
    reference_ids = {
        'url': 'https://attacker.example/file',
        'uppercase-url': 'HTTPS://attacker.example/file',
        'data': 'data:application/pdf;base64,Zm9v',
        'path-id': '/etc/passwd',
        'windows-path-id': 'C:\\private\\file.pdf',
        'base64-image': 'data:image/png;base64,iVBORw0KGgo=',
        'external-image': 'https://attacker.example/image.png',
    }
    if case in reference_ids:
        file_id = reference_ids[case]
        entry['id'] = file_id
        entry['type'] = 'image' if case in ('base64-image', 'external-image') else 'file'
        attachment.rows[file_id] = attachment.record
    elif case == 'text':
        entry['type'] = 'text'
    elif case == 'unsaved-image':
        entry = {'type': 'image', 'url': 'data:image/png;base64,iVBORw0KGgo='}
    elif case in ('missing-image-record', 'unreadable-image'):
        entry['type'] = 'image'
    elif case == 'missing-type':
        entry.pop('type')
    user_message = {'files': [] if case in ('historical', 'readable-knowledge-only') else [entry]}
    if case == 'malformed-files':
        user_message['files'] = 42
    _spec, callable_ = _bind_composio_attachment(attachment, user_message=user_message)
    if case in ('missing-record', 'missing-image-record'):
        attachment.rows.clear()
    elif case in ('unreadable', 'revoked-after-binding', 'unreadable-image'):
        attachment.readable = False
    elif case == 'metadata-oversize':
        attachment.record.meta['size'] = attachment.module.MAX_COMPOSIO_UPLOAD_BYTES + 1
    elif case == 'actual-oversize':
        attachment.path.write_bytes(b'x' * (attachment.module.MAX_COMPOSIO_UPLOAD_BYTES + 1))
    elif case == 'empty':
        attachment.path.write_bytes(b'')
    elif case == 'size-mismatch':
        attachment.record.meta['size'] += 1
    elif case == 'invalid-size':
        attachment.record.meta['size'] = True
    elif case == 'filesystem':
        attachment.record.path = str(Path(attachment.module.__file__).resolve())
    elif case == 'symlink':
        link = tmp_path / 'link'
        link.symlink_to(attachment.path)
        attachment.record.path = str(link)
    elif case == 'missing-path':
        attachment.record.path = str(tmp_path / 'missing-file')
    external_attempts = []

    def forbidden_client(*_args, **_kwargs):
        external_attempts.append(True)
        raise AssertionError('Rejected attachments must not be staged')

    monkeypatch.setattr(attachment.module, 'create_composio_httpx_client', forbidden_client)
    with pytest.raises(ValueError):
        await callable_(file_to_upload={'file_id': file_id}, folder_to_upload_to='target-folder')
    assert external_attempts == []
    assert attachment.delegated == []


@pytest.mark.asyncio
async def test_composio_attachment_upload_rechecks_record_and_snapshots_current_ids(composio_attachment):
    attachment = composio_attachment
    user_message = {'files': [{'type': 'file', 'id': attachment.file_id}]}
    _spec, callable_ = _bind_composio_attachment(attachment, user_message=user_message)
    user_message['files'].append({'type': 'file', 'id': 'added-after-binding'})
    attachment.rows['added-after-binding'] = attachment.record
    with pytest.raises(ValueError):
        await callable_(file_to_upload={'file_id': 'added-after-binding'})
    assert attachment.lookups == []
    replacement = copy.deepcopy(attachment.record)
    replacement.meta['size'] = attachment.module.MAX_COMPOSIO_UPLOAD_BYTES + 1
    attachment.rows[attachment.file_id] = replacement
    with pytest.raises(ValueError):
        await callable_(file_to_upload={'file_id': attachment.file_id})
    assert attachment.authorizations == [(attachment.file_id, 'read', 'alice')]
    assert attachment.storage_reads == attachment.delegated == []


@pytest.mark.asyncio
async def test_composio_attachment_upload_accepts_exact_byte_limit_and_trusted_storage_metadata(
    composio_attachment, monkeypatch
):
    attachment = composio_attachment
    content = b'x' * attachment.module.MAX_COMPOSIO_UPLOAD_BYTES
    attachment.path.write_bytes(content)
    attachment.record.meta = {'size': len(content)}
    attachment.record.filename = 'archive.unknown-extension'
    _spec, callable_ = _bind_composio_attachment(attachment)
    staged = []

    async def stage(api_key, filename, mimetype, actual_bytes):
        staged.append((api_key, filename, mimetype, actual_bytes))
        return (
            {'name': filename, 'mimetype': mimetype, 's3key': 'private-boundary-key'},
            'https://storage.composio.dev/file',
        )

    monkeypatch.setattr(attachment.module, '_stage_attachment', stage)
    assert await callable_(file_to_upload={'file_id': attachment.file_id}) == {'successful': True}
    assert staged == [('test-project-key', 'archive.unknown-extension', 'application/octet-stream', content)]


@pytest.mark.parametrize(
    'arguments',
    [
        {},
        {
            'file_id': 'current-upload-file',
            'file_to_upload': {'name': 'report.pdf', 'mimetype': 'application/pdf', 's3key': 'private'},
        },
        {'file_id': None},
        {'file_to_upload': {'file_id': None}},
        {'file_to_upload': {'file_id': 'current-upload-file', 's3key': 'private'}},
        {'file_to_upload': {'file_id': 'current-upload-file', 'name': 'untrusted.pdf'}},
        {
            'file_to_upload': {
                'file_id': 'current-upload-file',
                'name': 'report.pdf',
                'mimetype': 'application/pdf',
                's3key': 'private',
            }
        },
        {'file_to_upload': '/etc/passwd'},
        {'file_to_upload': 'https://attacker.example/file'},
        {'file_to_upload': {'name': 'report.pdf', 'mimetype': 'application/pdf'}},
        {'file_to_upload': {'name': 'report.pdf', 'mimetype': 'application/pdf', 's3key': ''}},
    ],
)
@pytest.mark.asyncio
async def test_composio_attachment_upload_rejects_ambiguous_or_unstaged_input(composio_attachment, arguments):
    _spec, callable_ = _bind_composio_attachment(composio_attachment)
    with pytest.raises(ValueError):
        await callable_(**arguments)
    assert composio_attachment.lookups == composio_attachment.storage_reads == composio_attachment.delegated == []


@pytest.mark.asyncio
async def test_composio_attachment_upload_preserves_existing_staged_payload_without_local_access(composio_attachment):
    _spec, callable_ = _bind_composio_attachment(composio_attachment, user_message={'files': []})
    staged = {'name': 'existing.pdf', 'mimetype': 'application/pdf', 's3key': 'existing-provider-key'}
    assert await callable_(file_to_upload=staged, folder_to_upload_to='existing-folder') == {'successful': True}
    assert composio_attachment.delegated == [{'file_to_upload': staged, 'folder_to_upload_to': 'existing-folder'}]
    assert composio_attachment.lookups == composio_attachment.authorizations == composio_attachment.storage_reads == []


@pytest.mark.parametrize(
    'failure',
    [
        'api-status',
        'api-redirect',
        'api-network',
        'invalid-json',
        'missing-key',
        'empty-key',
        'missing-url',
        'missing-metadata',
        'unknown-backend',
        'http-url',
        'foreign-url',
        'suffix-url',
        'userinfo-url',
        'port-url',
        'fragment-url',
        'loopback-url',
        'azure-tenant-url',
        's3-tenant-url',
        'put-status',
        'put-redirect',
        'put-network',
    ],
)
@pytest.mark.asyncio
async def test_composio_attachment_upload_blocks_staging_failures_without_leaking_credentials(
    app_modules, composio_attachment, monkeypatch, caplog, failure
):
    caplog.set_level(logging.DEBUG)
    private_key = 'projects/private-project/requests/googledrive/private-failure-key'
    private_url = f'https://storage.composio.dev/{private_key}?signature=private-failure-signature'
    payload = {'key': private_key, 'new_presigned_url': private_url, 'metadata': {'storage_backend': 's3'}}
    invalid_urls = {
        'http-url': private_url.replace('https:', 'http:'),
        'foreign-url': 'https://attacker.example/upload',
        'suffix-url': 'https://storage.composio.dev.attacker.example/upload',
        'userinfo-url': 'https://storage.composio.dev@attacker.example/upload',
        'port-url': 'https://storage.composio.dev:8443/upload',
        'fragment-url': private_url + '#fragment',
        'loopback-url': 'https://127.0.0.1/upload',
        'azure-tenant-url': 'https://attacker.blob.core.windows.net/upload',
        's3-tenant-url': 'https://attacker.s3.amazonaws.com/upload',
    }
    if failure in invalid_urls:
        payload['new_presigned_url'] = invalid_urls[failure]
    elif failure == 'missing-key':
        payload.pop('key')
    elif failure == 'empty-key':
        payload['key'] = ' '
    elif failure == 'missing-url':
        payload.pop('new_presigned_url')
    elif failure == 'missing-metadata':
        payload.pop('metadata')
    elif failure == 'unknown-backend':
        payload['metadata']['storage_backend'] = 'unsupported'
    requests = []

    def handler(request):
        requests.append(request)
        logging.getLogger('httpx').info('private upload %s %s', private_key, private_url)
        if request.method == 'POST':
            if failure == 'api-status':
                return httpx.Response(401, text=f'{private_key} {private_url} test-project-key')
            if failure == 'api-redirect':
                return httpx.Response(307, headers={'Location': 'https://attacker.example/api'})
            if failure == 'api-network':
                raise httpx.ConnectError(f'{private_key} {private_url}', request=request)
            if failure == 'invalid-json':
                return httpx.Response(200, text=f'{private_key} not-json {private_url}')
            return httpx.Response(200, json=payload)
        assert request.method == 'PUT'
        assert str(request.url) == private_url
        assert 'x-api-key' not in request.headers
        if failure == 'put-redirect':
            return httpx.Response(307, headers={'Location': 'https://attacker.example/file'})
        if failure == 'put-network':
            raise httpx.ConnectError(f'{private_key} {private_url}', request=request)
        return httpx.Response(403, text=f'{private_key} {private_url}')

    _install_composio_transport(monkeypatch, app_modules.composio, handler)
    _spec, callable_ = _bind_composio_attachment(composio_attachment)
    with pytest.raises(ValueError) as error:
        await callable_(file_to_upload={'file_id': composio_attachment.file_id}, folder_to_upload_to='target-folder')
    assert [request.method for request in requests] == (['POST', 'PUT'] if failure.startswith('put-') else ['POST'])
    assert composio_attachment.delegated == []
    visible = str(error.value) + caplog.text
    assert private_key not in visible and private_url not in visible and 'test-project-key' not in visible
    assert not any(record.exc_info for record in caplog.records)


@pytest.mark.parametrize('result_type', ['structured', 'mcp-app', 'exception'])
@pytest.mark.parametrize('escaped_key', [False, True])
@pytest.mark.asyncio
async def test_composio_attachment_upload_redacts_provider_echoes(
    composio_attachment, monkeypatch, result_type, escaped_key
):
    from open_webui.utils.mcp.apps import MCPAppResult

    private_key = 'projects/private-project/requests/googledrive/private-result-key'
    if escaped_key:
        private_key += '/quote"and\\backslash\n'
    private_url = f'https://storage.composio.dev/{private_key}?signature=private-result-signature'

    async def stage(_api_key, filename, mimetype, _content):
        return {'name': filename, 'mimetype': mimetype, 's3key': private_key}, private_url

    async def delegate(**arguments):
        if result_type == 'exception':
            raise RuntimeError(f'private failure {private_key} {private_url}')
        result = {'error': f'provider rejected {private_key} at {private_url}', 'successful': False}
        if result_type == 'mcp-app':
            return MCPAppResult(embed={'arguments': arguments, 'result': result}, context=[result])
        return [{'type': 'text', 'text': json.dumps(result)}, {'structured': result}]

    monkeypatch.setattr(composio_attachment.module, '_stage_attachment', stage)
    _spec, callable_ = _bind_composio_attachment(composio_attachment, delegate=delegate)
    if result_type == 'exception':
        with pytest.raises(ValueError) as error:
            await callable_(file_to_upload={'file_id': composio_attachment.file_id})
        visible = str(error.value)
    else:
        result = await callable_(file_to_upload={'file_id': composio_attachment.file_id})
        visible = json.dumps({'embed': result.embed, 'context': result.context} if result_type == 'mcp-app' else result)
        assert 'provider rejected' in visible and '[redacted]' in visible
    assert private_key not in visible and private_url not in visible and 'private-result-signature' not in visible
    assert json.dumps(private_key)[1:-1] not in visible


@pytest.mark.parametrize('excluded_by', ['policy', 'function-filter'])
@pytest.mark.asyncio
async def test_composio_attachment_upload_does_not_expand_policy_or_filters(
    app_modules, composio_attachment, monkeypatch, excluded_by
):
    client_class = _in_process_mcp_client_class(app_specs=[*_APP_TOOL_SPECS, _DRIVE_UPLOAD_SPEC])
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    allowed = ['GOOGLEDRIVE_FIND_FILE']
    if excluded_by == 'function-filter':
        allowed.append('GOOGLEDRIVE_UPLOAD_FILE')
    connection = _connection(
        {'googledrive': {'tools': allowed}}, server_id='policy-test', grants=_user_read_grant('alice')
    )
    connection['config']['function_name_filter_list'] = 'GOOGLEDRIVE_FIND_FILE'
    _request, _form, metadata, _client = await _process_chat_with_servers(
        monkeypatch,
        app_modules,
        [connection],
        _user('alice'),
        ['server:composio:policy-test'],
        hostile_metadata={'user_message': {'files': [{'type': 'file', 'id': composio_attachment.file_id}]}},
        client_class=client_class,
    )
    assert set(metadata['tools']) == {
        _composio_function_name('policy-test', slug)
        for slug in ('GOOGLEDRIVE_FIND_FILE', 'COMPOSIO_MANAGE_CONNECTIONS')
    }
    assert composio_attachment.lookups == composio_attachment.storage_reads == []
    assert client_class.instances[0].calls == []
    await app_modules.middleware.disconnect_mcp_clients({'server:composio:policy-test': client_class.instances[0]})


@pytest.mark.parametrize('failure', [None, 'final-answer', 'sse', 'sse-then-finish', 'plain-json', 'responses-error', 'responses-failed-then-completed'])
@pytest.mark.asyncio
async def test_stream_error_or_disabled_tool_choice_never_executes_mcp_tool(app_modules, monkeypatch, failure):
    from starlette.responses import StreamingResponse
    from open_webui.utils.tool_calling import final_answer_payload

    middleware = app_modules.middleware
    client_class = _in_process_mcp_client_class()
    _install_user_bound_sessions(monkeypatch, app_modules, client_class)
    user = _user('alice')
    request, form_data, metadata, _ = await _process_chat_with_servers(
        monkeypatch, app_modules,
        [_connection(server_id='shared', grants=_user_read_grant('alice'))],
        user, ['server:composio:shared'], client_class=client_class,
    )
    request.app.state.redis = None
    metadata['message_id'] = 'stream-error-regression'
    metadata['params']['tool_approval_mode'] = 'full'
    form_data['metadata'] = metadata
    if failure == 'final-answer':
        declarations = copy.deepcopy(form_data['tools'])
        form_data = final_answer_payload(form_data)
        assert form_data['tools'] == declarations
    name = _composio_function_name('shared', 'GOOGLEDRIVE_FIND_FILE')
    emitted = []

    async def emit(event):
        emitted.append(copy.deepcopy(event))

    async def no_result(*_args, **_kwargs):
        return None

    async def follow_up(*_args, **_kwargs):
        # The next model request is outside this regression; do not make external calls.
        return {'choices': [{'message': {'content': 'Finished.'}}]}

    monkeypatch.setattr(middleware, 'generate_chat_completion', follow_up)
    monkeypatch.setattr(middleware, 'outlet_filter_handler', no_result)
    monkeypatch.setattr(middleware, 'background_tasks_handler', no_result)
    monkeypatch.setattr(middleware, 'publish_chat_finished_event', no_result)

    error = {'message': 'Provider stopped this generation.'}
    if failure and failure.startswith('responses-'):
        item = {'type': 'function_call', 'id': 'fc-stream', 'call_id': 'call-stream', 'name': name,
                'arguments': '{"query":"document"}', 'status': 'completed'}
        events = [
            {'type': 'response.output_item.added', 'output_index': 0, 'item': item},
            {'type': 'response.output_item.done', 'output_index': 0, 'item': item},
            {'type': 'error', 'error': error} if failure == 'responses-error' else
            {'type': 'response.failed', 'response': {'status': 'failed', 'error': error}},
        ]
        if failure == 'responses-failed-then-completed':
            events.append({'type': 'response.completed', 'response': {'status': 'completed'}})
    else:
        # Both fragments together form valid JSON before the provider reports failure.
        events = [
            {'choices': [{'index': 0, 'delta': {'tool_calls': [{'index': 0, 'id': 'call-stream', 'type': 'function',
                          'function': {'name': name, 'arguments': '{"query":'}}]}, 'finish_reason': None}]},
            {'choices': [{'index': 0, 'delta': {'tool_calls': [{'index': 0,
                          'function': {'arguments': '"document"}'}}]}, 'finish_reason': None}]},
        ]
        if failure and failure != 'final-answer':
            events.append({'error': error})
        if failure in (None, 'final-answer', 'sse-then-finish'):
            events.append({'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'tool_calls'}]})

    async def chunks():
        for event in events:
            prefix = '' if failure == 'plain-json' and 'error' in event else 'data: '
            yield prefix + json.dumps(event) + '\n\n'
        yield 'data: [DONE]\n\n'

    ctx = {
        'request': request, 'form_data': form_data, 'user': user, 'model': request.app.state.MODELS['test-model'],
        'metadata': metadata, 'events': [], 'tasks': None, 'event_emitter': emit, 'event_caller': None,
    }
    try:
        await middleware.streaming_chat_response_handler(StreamingResponse(chunks(), media_type='text/event-stream'), ctx)
        calls = client_class.instances[0].calls
        output = ctx['assistant_message']['output']
        if failure:
            assert calls == []
            assert any(event.get('data', {}).get('error') for event in emitted)
            assert ctx['assistant_message']['finish_reason'] == ('tool_calls' if failure == 'final-answer' else 'failed')
            assert any(item.get('type') == 'function_call' and item.get('status') == 'failed' for item in output)
        else:
            # Positive control proves this same parsed call reaches the real bound MCP callable.
            assert calls == [('GOOGLEDRIVE_FIND_FILE', {'query': 'document'})]
            assert any(item.get('type') == 'function_call_output' and item.get('status') == 'completed' for item in output)
    finally:
        await middleware.disconnect_mcp_clients(metadata.get('mcp_clients', {}))


@pytest_asyncio.fixture
async def generated_file_client(app_modules, monkeypatch, tmp_path):
    from fastapi import FastAPI, HTTPException, Request
    from open_webui.routers import files
    from open_webui.storage import provider

    records = {}

    async def insert_file(user_id, form, db=None):
        record = files.FileModel(user_id=user_id, created_at=0, updated_at=0, **form.model_dump())
        records[record.id] = record
        return record

    async def get_file(file_id, db=None):
        return records.get(file_id)

    async def config_get(key, default=None):
        return {'rag.file.max_size': 1, 'rag.file.allowed_extensions': ['pdf']}.get(key, default)

    async def publish(*args, **kwargs):
        pass

    async def shared_read(file_id, permission, user, db=None):
        return file_id in records and permission == 'read' and user.id == 'shared-reader'

    def verified_user(request: Request):
        token = request.headers.get('authorization', '').removeprefix('Bearer ')
        if token not in ('alice', 'bob', 'shared-reader'):
            raise HTTPException(401, 'Not authenticated')
        return _user(token)

    async def no_db():
        return None

    monkeypatch.setattr(files.Files, 'insert_new_file', insert_file)
    monkeypatch.setattr(files.Files, 'get_file_by_id', get_file)
    monkeypatch.setattr(files.Config, 'get', staticmethod(config_get))
    monkeypatch.setattr(files, 'publish_event', publish)
    monkeypatch.setattr(files, 'has_access_to_file', shared_read)
    monkeypatch.setattr(provider, 'UPLOAD_DIR', tmp_path)
    monkeypatch.setattr(files, 'Storage', provider.LocalStorageProvider())
    app = FastAPI()
    app.include_router(files.router, prefix='/api/v1/files')
    app.dependency_overrides[files.get_verified_user] = verified_user
    app.dependency_overrides[files.get_async_session] = no_db
    async with _HTTPX_ASYNC_CLIENT(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        yield SimpleNamespace(client=client, records=records, directory=tmp_path)


@pytest.mark.parametrize(
    ('filename', 'content'),
    [('report.csv', b'name,total\nAlice,42\n'), ('binary.bin', b'\x00\xff\x80\n'), ('empty.txt', b'')],
)
@pytest.mark.asyncio
async def test_generated_file_download_uses_owned_storage_and_existing_access_control(generated_file_client, filename, content):
    fixture = generated_file_client
    uploaded = await fixture.client.post(
        '/api/v1/files/?process=false',
        headers={'authorization': 'Bearer alice'},
        files={'file': (filename, content, 'application/octet-stream')},
    )
    assert uploaded.status_code == 200
    data = uploaded.json()
    record = fixture.records[data['id']]
    assert record.user_id == 'alice'
    assert record.meta['size'] == len(content)
    assert record.data == {}  # No RAG processing is scheduled for generated files.
    assert Path(record.path).read_bytes() == content

    url = f'/api/v1/files/{record.id}/content?attachment=true'
    download = await fixture.client.get(url, headers={'authorization': 'Bearer alice'})
    assert download.status_code == 200
    assert download.content == content
    assert download.headers['content-disposition'] == f"attachment; filename*=UTF-8''{filename}"
    assert (await fixture.client.get(url)).status_code == 401
    assert (await fixture.client.get(url, headers={'authorization': 'Bearer bob'})).status_code == 404
    shared = await fixture.client.get(url, headers={'authorization': 'Bearer shared-reader'})
    assert shared.status_code == 200 and shared.content == content
    Path(record.path).unlink()
    assert (await fixture.client.get(url, headers={'authorization': 'Bearer alice'})).status_code == 404


@pytest.mark.asyncio
async def test_generated_file_upload_keeps_size_limits_and_processed_empty_rejection(generated_file_client):
    fixture = generated_file_client
    too_large = await fixture.client.post(
        '/api/v1/files/?process=false',
        headers={'authorization': 'Bearer alice'},
        files={'file': ('large.bin', b'x' * (1024 * 1024 + 1), 'application/octet-stream')},
    )
    assert too_large.status_code == 413
    empty_processed = await fixture.client.post(
        '/api/v1/files/?process=true',
        headers={'authorization': 'Bearer alice'},
        files={'file': ('empty.pdf', b'', 'application/pdf')},
    )
    assert empty_processed.status_code == 400
    assert fixture.records == {}
    assert list(fixture.directory.iterdir()) == []


@pytest.mark.parametrize('file_errors', [[], ['Could not publish failed.csv: upload unavailable']])
@pytest.mark.asyncio
async def test_execute_code_download_result_preserves_real_links_and_export_errors(app_modules, monkeypatch, file_errors):
    from open_webui.tools import builtin

    async def config_get(key, default=None):
        return 'pyodide' if key == 'code_interpreter.engine' else default

    files = [{'id': 'stored-output-id', 'name': 'nested/report.csv', 'url': 'https://webui.test/api/v1/files/stored-output-id/content?attachment=true'}]

    async def event_call(event):
        assert event['type'] == 'execute:python'
        assert event['data']['session_id'] == 'alice-session'
        assert event['data']['files'] == []
        return {'stdout': 'done\n', 'stderr': '\n'.join(file_errors), 'result': 0, 'files': files, 'file_errors': file_errors}

    monkeypatch.setattr(builtin.Config, 'get', staticmethod(config_get))
    result = json.loads(await builtin.execute_code(
        "print('done')",
        __request__=SimpleNamespace(),
        __event_call__=event_call,
        __metadata__={'session_id': 'alice-session'},
    ))
    assert result == {
        'status': 'error' if file_errors else 'success',
        'stdout': 'done\n',
        'stderr': '\n'.join(file_errors),
        'result': 0,
        'files': files,
        'file_errors': file_errors,
    }


@pytest.mark.parametrize('raw', [False, True])
def test_generated_file_legacy_continuation_and_replay_preserve_downloads_and_errors(app_modules, raw):
    from open_webui.utils.misc import convert_output_to_messages

    execution_output = {
        'stdout': 'done\n',
        'stderr': 'Generated file download: upload unavailable',
        'result': 0,
        'files': [{'id': 'stored-output-id', 'name': 'report.csv', 'url': 'https://webui.test/api/v1/files/stored-output-id/content?attachment=true'}],
        'file_errors': ['upload unavailable'],
    }
    messages = convert_output_to_messages([{
        'type': 'open_webui:code_interpreter',
        'code': "print('done')",
        'status': 'completed',
        'output': execution_output,
    }], raw=raw)
    content = messages[0]['content']
    result_text = content.split('<code_interpreter_output>\n', 1)[1].split('\n</code_interpreter_output>', 1)[0]
    assert json.loads(result_text) == execution_output
