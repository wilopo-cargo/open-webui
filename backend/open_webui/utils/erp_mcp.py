"""Strict current-turn CSV handoff for the ERP MCP server."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

import aiofiles
import aiohttp

from open_webui.config import UPLOAD_DIR
from open_webui.env import (
    AIOHTTP_CLIENT_SESSION_TOOL_SERVER_SSL,
    AIOHTTP_CLIENT_TIMEOUT_TOOL_SERVER,
    AIOHTTP_FILE_STREAM_CHUNK_SIZE,
)
from open_webui.models.files import Files
from open_webui.storage.provider import Storage
from open_webui.utils.session_pool import cleanup_response, get_session

MAX_STATEMENT_BYTES = 10 * 1024 * 1024
MAX_UPLOAD_RESPONSE_BYTES = 64 * 1024


class _AttachmentError(ValueError):
    """An expected, safe failure at the attachment trust boundary."""


def validate_saved_mcp_url(connection: dict) -> str:
    """Return the fixed upload endpoint for a trusted saved MCP URL."""
    raw_url = connection.get('url')
    if not isinstance(raw_url, str) or not raw_url.strip():
        raise _AttachmentError('ERP MCP attachment upload is not configured safely.')

    parsed = urlsplit(raw_url)
    try:
        port = parsed.port
    except ValueError:
        raise _AttachmentError('ERP MCP attachment upload is not configured safely.') from None
    if (
        parsed.scheme not in {'http', 'https'}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path != '/mcp'
        or port is None and ':' in parsed.netloc.rsplit(']', 1)[-1]
    ):
        raise _AttachmentError('ERP MCP attachment upload is not configured safely.')

    host = parsed.hostname
    if not host:
        raise _AttachmentError('ERP MCP attachment upload is not configured safely.')
    if parsed.scheme == 'http':
        host_lower = host.casefold().rstrip('.')
        try:
            loopback = ipaddress.ip_address(host_lower).is_loopback
        except ValueError:
            loopback = host_lower == 'localhost' or host_lower.endswith('.localhost')
        if not loopback:
            raise _AttachmentError('ERP MCP attachment upload requires HTTPS.')

    return f'{parsed.scheme}://{parsed.netloc}/uploads/statements'


async def _load_owned_file(user_id: str, file_id: str) -> dict | None:
    try:
        file = await Files.get_file_by_id_and_user_id(file_id, user_id)
        filename = getattr(file, 'filename', None)
        file_path = getattr(file, 'path', None)
        metadata = getattr(file, 'meta', None)
    except Exception:
        return None
    if not file or not isinstance(filename, str) or not filename.casefold().endswith('.csv'):
        return None
    if not isinstance(file_path, str) or not file_path.strip() or not isinstance(metadata, dict):
        return None

    stored_size = metadata.get('size')
    if isinstance(stored_size, bool) or not isinstance(stored_size, int) or stored_size <= 0:
        return None
    if stored_size > MAX_STATEMENT_BYTES:
        return None

    try:
        local_path = await asyncio.to_thread(Storage.get_file, file_path)
        path = Path(local_path)
        root = Path(UPLOAD_DIR).resolve()
        resolved = path.resolve(strict=True)
        if path.is_symlink() or not resolved.is_file() or not resolved.is_relative_to(root):
            return None
        actual_size = (await asyncio.to_thread(resolved.stat)).st_size
    except Exception:
        return None
    if actual_size <= 0 or actual_size > MAX_STATEMENT_BYTES or actual_size != stored_size:
        return None

    return {
        'id': getattr(file, 'id', file_id),
        'filename': filename,
        'path': resolved,
        'stored_size': stored_size,
        'actual_size': actual_size,
    }


async def _eligible_current_turn_files(user, user_message: dict | None) -> dict[str, dict]:
    if not isinstance(user_message, dict):
        return {}
    entries = user_message.get('files')
    if not isinstance(entries, list):
        return {}

    eligible: dict[str, dict] = {}
    for entry in entries:
        if not isinstance(entry, dict) or entry.get('type') != 'file':
            continue
        file_id = entry.get('id')
        if not isinstance(file_id, str) or not file_id or file_id in eligible:
            continue
        record = await _load_owned_file(str(user.id), file_id)
        if record:
            eligible[file_id] = record
    return eligible


async def _hash_file(record: dict) -> str:
    digest = hashlib.sha256()
    total = 0
    try:
        async with aiofiles.open(record['path'], 'rb') as source:
            while True:
                chunk = await source.read(AIOHTTP_FILE_STREAM_CHUNK_SIZE)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_STATEMENT_BYTES:
                    raise _AttachmentError('Statement CSV must be 10 MiB or smaller.')
                digest.update(chunk)
    except _AttachmentError:
        raise
    except Exception:
        raise _AttachmentError('Statement attachment is unavailable; try again.') from None
    if total != record['actual_size']:
        raise _AttachmentError('Statement attachment changed; attach the CSV again.')
    return digest.hexdigest()


async def _access_token(request, user, server_id: str) -> str:
    try:
        manager = request.app.state.oauth_client_manager
        token = await manager.get_oauth_token(user.id, f'mcp:{server_id}')
    except Exception:
        raise _AttachmentError('ERP authorization is unavailable; reconnect the ERP MCP server.') from None
    access_token = token.get('access_token') if isinstance(token, dict) else None
    if not isinstance(access_token, str) or not access_token.strip():
        raise _AttachmentError('ERP authorization is unavailable; reconnect the ERP MCP server.')
    return access_token


async def _relay_upload(request, connection: dict, user, server_id: str, record: dict, digest: str) -> str:
    upload_url = validate_saved_mcp_url(connection)
    access_token = await _access_token(request, user, server_id)
    stream_digest = hashlib.sha256()
    total = 0

    async def chunks():
        nonlocal total
        try:
            async with aiofiles.open(record['path'], 'rb') as source:
                while True:
                    chunk = await source.read(AIOHTTP_FILE_STREAM_CHUNK_SIZE)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_STATEMENT_BYTES:
                        raise _AttachmentError('Statement CSV must be 10 MiB or smaller.')
                    stream_digest.update(chunk)
                    yield chunk
        except _AttachmentError:
            raise
        except Exception:
            raise _AttachmentError('Statement attachment is unavailable; try again.') from None
        if total != record['actual_size'] or stream_digest.hexdigest() != digest:
            raise _AttachmentError('Statement attachment changed; attach the CSV again.')

    try:
        form_data = aiohttp.FormData()
        form_data.add_field('file', chunks(), filename=record['filename'], content_type='text/csv')
        timeout_value = AIOHTTP_CLIENT_TIMEOUT_TOOL_SERVER
        timeout = (
            aiohttp.ClientTimeout(total=float(timeout_value))
            if isinstance(timeout_value, (int, float)) and timeout_value > 0
            else aiohttp.ClientTimeout(total=30)
        )
        ssl = AIOHTTP_CLIENT_SESSION_TOOL_SERVER_SSL
        if ssl is False and upload_url.startswith('https://'):
            ssl = True
    except Exception:
        raise _AttachmentError('ERP statement upload is unavailable; try again.') from None

    response = None
    try:
        session = await get_session()
        response = await session.post(
            upload_url,
            data=form_data,
            headers={'Authorization': f'Bearer {access_token}'},
            allow_redirects=False,
            ssl=ssl if upload_url.startswith('https://') else None,
            timeout=timeout,
        )
        body = await response.content.read(MAX_UPLOAD_RESPONSE_BYTES + 1)
        if len(body) > MAX_UPLOAD_RESPONSE_BYTES or response.status != 201:
            raise _AttachmentError('ERP statement upload failed; try again.')
        try:
            payload = json.loads(body)
        except (TypeError, ValueError):
            raise _AttachmentError('ERP statement upload returned an invalid response.') from None
        remote_id = payload.get('file_id') if isinstance(payload, dict) else None
        if not isinstance(remote_id, str):
            raise _AttachmentError('ERP statement upload returned an invalid file ID.')
        try:
            parsed_id = UUID(remote_id)
        except ValueError:
            raise _AttachmentError('ERP statement upload returned an invalid file ID.') from None
        if str(parsed_id) != remote_id.casefold():
            raise _AttachmentError('ERP statement upload returned an invalid file ID.')
        return remote_id
    except _AttachmentError:
        raise
    except Exception:
        raise _AttachmentError('ERP statement upload is unavailable; try again.') from None
    finally:
        try:
            await cleanup_response(response)
        except Exception:
            pass


async def bind_statement_reconcile_tool(
    request,
    connection: dict,
    user,
    user_message: dict | None,
    tool_spec: dict,
    delegate,
    server_id: str,
) -> tuple[dict, object]:
    """Bind reconciliation to owned current-turn files and the generic MCP delegate."""
    eligible = await _eligible_current_turn_files(user, user_message)
    attachment_names = {file_id: record['filename'] for file_id, record in eligible.items()}
    encoded_names = json.dumps(attachment_names, ensure_ascii=False, separators=(',', ':'))
    spec = {**tool_spec}
    existing_description = spec.get('description') or ''
    spec['description'] = (
        f'{existing_description}\n\n'
        'Use exactly one verified current-turn CSV attachment ID from this JSON object; '
        f'no other file reference is accepted: {encoded_names}'
    ).strip()
    upload_cache: dict[tuple[str, str], str] = {}

    async def tool_function(**kwargs):
        file_id = kwargs.get('file_id')
        if not isinstance(file_id, str) or not file_id:
            raise _AttachmentError('Choose one verified current-turn CSV attachment with file_id.')

        current_files = await _eligible_current_turn_files(user, user_message)
        record = current_files.get(file_id)
        if record is None:
            raise _AttachmentError('Choose one verified current-turn CSV attachment with file_id.')

        digest = await _hash_file(record)
        cache_key = (str(user.id), digest)
        remote_id = upload_cache.get(cache_key)
        if remote_id is None:
            remote_id = await _relay_upload(request, connection, user, server_id, record, digest)
            upload_cache[cache_key] = remote_id

        delegated_kwargs = dict(kwargs)
        delegated_kwargs['file_id'] = remote_id
        return await delegate(**delegated_kwargs)

    return spec, tool_function
