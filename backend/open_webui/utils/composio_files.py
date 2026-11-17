"""Current-turn chat attachments for Composio's Google Drive upload action."""

import asyncio
import copy
import hashlib
import json
import mimetypes
from pathlib import Path
from urllib.parse import urlsplit

from open_webui.config import UPLOAD_DIR
from open_webui.models.files import Files
from open_webui.storage.provider import Storage
from open_webui.utils.access_control.files import has_access_to_file
from open_webui.utils.composio import composio_log_scope, create_composio_httpx_client
from open_webui.utils.mcp.apps import MCPAppResult

MAX_COMPOSIO_UPLOAD_BYTES = 5_000_000  # GOOGLEDRIVE_UPLOAD_FILE documents a 5 MB limit.


class _AttachmentError(ValueError):
    """A safe, model-visible attachment failure without storage credentials."""


def _read_attachment_bytes(stored_path: str, stored_size: int) -> bytes:
    try:
        path = Path(Storage.get_file(stored_path))
        resolved = path.resolve(strict=True)
        if path.is_symlink() or not resolved.is_file() or not resolved.is_relative_to(Path(UPLOAD_DIR).resolve()):
            raise _AttachmentError('Composio attachment is unavailable; attach the file again.')
        with resolved.open('rb') as source:
            content = source.read(MAX_COMPOSIO_UPLOAD_BYTES + 1)
    except _AttachmentError:
        raise
    except Exception:
        raise _AttachmentError('Composio attachment is unavailable; attach the file again.') from None
    if len(content) > MAX_COMPOSIO_UPLOAD_BYTES:
        raise _AttachmentError('Composio Drive attachments must be 5 MB (5,000,000 bytes) or smaller.')
    if not content or len(content) != stored_size:
        raise _AttachmentError('Composio attachment changed; attach the file again.')
    return content


async def _load_attachment(user, file_id: str) -> tuple[str, str, bytes]:
    try:
        file = await Files.get_file_by_id(file_id)
        if file is None or not (user.role == 'admin' or await has_access_to_file(file_id, 'read', user)):
            raise _AttachmentError('Choose a readable file attached to the current user message.')
        metadata = file.meta or {}
        size = metadata.get('size')
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise _AttachmentError('Composio attachment is unavailable; attach the file again.')
        if size > MAX_COMPOSIO_UPLOAD_BYTES:
            raise _AttachmentError('Composio Drive attachments must be 5 MB (5,000,000 bytes) or smaller.')
        if not isinstance(file.filename, str) or not file.filename.strip() or not isinstance(file.path, str):
            raise _AttachmentError('Composio attachment is unavailable; attach the file again.')
        filename = Path(file.filename.replace('\\', '/')).name
        mimetype = metadata.get('content_type') or mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        if (
            not filename
            or not isinstance(mimetype, str)
            or any(ord(character) < 32 or ord(character) > 126 for character in mimetype)
        ):
            raise _AttachmentError('Composio attachment metadata is unavailable; attach the file again.')
        content = await asyncio.to_thread(_read_attachment_bytes, file.path, size)
        return filename, mimetype, content
    except _AttachmentError:
        raise
    except Exception:
        raise _AttachmentError('Composio attachment is unavailable; attach the file again.') from None


def _validate_upload_url(value: object) -> str:
    # The Files API documents this Composio-controlled host. Do not allow arbitrary
    # S3/Azure/R2 tenants merely because they share a cloud storage domain.
    # https://docs.composio.dev/reference/api-reference/files/postFilesUploadRequest
    if not isinstance(value, str) or not value or any(character.isspace() for character in value):
        raise ValueError
    parsed = urlsplit(value)
    if (
        parsed.scheme != 'https'
        or parsed.hostname != 'storage.composio.dev'
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or not parsed.path.startswith('/')
        or '#' in value
        or '\\' in value
    ):
        raise ValueError
    return value


async def _stage_attachment(api_key: str, filename: str, mimetype: str, content: bytes) -> tuple[dict, str]:
    try:
        with composio_log_scope():
            async with create_composio_httpx_client(headers={'x-api-key': api_key}) as client:
                response = await client.post(
                    'https://backend.composio.dev/api/v3.1/files/upload/request',
                    json={
                        'toolkit_slug': 'googledrive',
                        'tool_slug': 'GOOGLEDRIVE_UPLOAD_FILE',
                        'filename': filename,
                        'mimetype': mimetype,
                        'md5': hashlib.md5(content, usedforsecurity=False).hexdigest(),
                    },
                )
                if response.status_code != 200:
                    raise ValueError
                payload = response.json()
            key = payload['key']
            if not isinstance(key, str) or not key.strip():
                raise ValueError
            upload_url = _validate_upload_url(payload['new_presigned_url'])
            backend = payload['metadata']['storage_backend']
            if backend not in ('s3', 'azure_blob_storage'):
                raise ValueError
            headers = {'Content-Type': mimetype}
            if backend == 'azure_blob_storage':
                headers['x-ms-blob-type'] = 'BlockBlob'
            # Separate client: never forward the project key, cookies or API auth to storage.
            async with create_composio_httpx_client() as client:
                response = await client.put(upload_url, content=content, headers=headers)
                if not 200 <= response.status_code < 300:
                    raise ValueError
        return {'name': filename, 'mimetype': mimetype, 's3key': key}, upload_url
    except Exception:
        raise _AttachmentError('Composio attachment upload failed; try again.') from None


def _redact_upload_result(value, secrets: tuple[str, ...]):
    if isinstance(value, str):
        for secret in secrets:
            value = value.replace(secret, '[redacted]')
        return value
    if isinstance(value, list):
        return [_redact_upload_result(item, secrets) for item in value]
    if isinstance(value, dict):
        return {
            _redact_upload_result(key, secrets): _redact_upload_result(item, secrets)
            for key, item in value.items()
        }
    if isinstance(value, MCPAppResult):
        return MCPAppResult(
            embed=_redact_upload_result(value.embed, secrets), context=_redact_upload_result(value.context, secrets)
        )
    return value


def _upload_schema_properties(spec: dict) -> dict:
    parameters = spec.get('parameters')
    if not isinstance(parameters, dict):
        raise ValueError('Composio is unavailable')
    properties = parameters.get('properties')
    required = parameters.get('required')
    if (
        parameters.get('type', 'object') != 'object'
        or not isinstance(properties, dict)
        or not isinstance(required, list)
        or not isinstance(properties.get('file_to_upload'), dict)
        or 'file_to_upload' not in required
        or 'file_id' in properties
        or any(key in parameters for key in ('$ref', 'oneOf', 'anyOf', 'allOf', 'if'))
    ):
        raise ValueError('Composio is unavailable')
    return properties


def bind_composio_upload_tool(api_key: str, user, user_message: dict | None, tool_spec: dict, delegate):
    """Retain staged-provider input; resolve local file IDs only inside the approved callable."""
    spec = copy.deepcopy(tool_spec)
    properties = _upload_schema_properties(spec)
    entries = user_message.get('files') if isinstance(user_message, dict) else None
    entries = entries if isinstance(entries, list) else []
    current_ids = frozenset(
        entry['id']
        for entry in entries or []
        if isinstance(entry, dict)
        and entry.get('type') in ('file', 'image')
        and isinstance(entry.get('id'), str)
        and entry['id']
        and not any(character in entry['id'] for character in '/\\:')
    )
    local_file_schema = {
        'type': 'object',
        'properties': {
            'file_id': {
                'type': 'string',
                'description': (
                    'Open WebUI file ID attached to the current user message (maximum 5 MB); not a path or URL.'
                ),
            }
        },
        'required': ['file_id'],
        'additionalProperties': False,
    }
    if current_ids:
        local_file_schema['properties']['file_id']['enum'] = sorted(current_ids)
    properties['file_to_upload'] = {'anyOf': [properties['file_to_upload'], local_file_schema]}
    spec['description'] = (
        f'{spec.get("description") or ""}\n\n'
        'For a chat attachment, supply file_to_upload: {"file_id": "<current attachment ID>"}. '
        'Alternatively supply an already staged Composio file_to_upload object with name, mimetype and s3key. '
        'Do not mix the two formats; local paths and URLs are not accepted.'
    ).strip()

    async def tool_function(**kwargs):
        if 'file_id' in kwargs or 'file_to_upload' not in kwargs:
            raise _AttachmentError(
                'Supply file_to_upload as a local file_id object or an already staged Composio file.'
            )
        arguments = dict(kwargs)
        upload_input = arguments['file_to_upload']
        if isinstance(upload_input, dict) and 'file_id' in upload_input:
            if set(upload_input) != {'file_id'}:
                raise _AttachmentError('Choose a local file_id object or a staged file object; do not mix the formats.')
            file_id = upload_input['file_id']
            if not isinstance(file_id, str) or file_id not in current_ids:
                raise _AttachmentError('Choose a readable file attached to the current user message.')
            filename, mimetype, content = await _load_attachment(user, file_id)
            staged_file, upload_url = await _stage_attachment(api_key, filename, mimetype, content)
            arguments['file_to_upload'] = staged_file
            secrets = (staged_file['s3key'], upload_url)
        else:
            staged_file = arguments['file_to_upload']
            if not isinstance(staged_file, dict) or any(
                not isinstance(staged_file.get(name), str) or not staged_file[name].strip()
                for name in ('name', 'mimetype', 's3key')
            ):
                raise _AttachmentError('file_to_upload must be an already staged Composio file object.')
            secrets = (staged_file['s3key'],)
        try:
            with composio_log_scope():
                result = await delegate(**arguments)
        except Exception:
            raise ValueError('Composio is unavailable') from None
        secrets = tuple(
            sorted(
                {variant for secret in secrets for variant in (secret, json.dumps(secret)[1:-1])},
                key=len,
                reverse=True,
            )
        )
        return _redact_upload_result(result, secrets)

    return spec, tool_function
