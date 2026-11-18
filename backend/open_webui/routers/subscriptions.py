from typing import Literal

from fastapi import APIRouter, Depends
from open_webui.utils.auth import get_verified_user
from open_webui.utils import subscriptions
from pydantic import BaseModel, ConfigDict, Field, field_validator

router = APIRouter()


class Grant(BaseModel):
    model_config = ConfigDict(extra='forbid')
    principal_type: Literal['user', 'group']
    principal_id: str = Field(min_length=1, max_length=255)
    permission: Literal['read']


class AuthorizeForm(BaseModel):
    model_config = ConfigDict(extra='forbid')
    provider: Literal['codex', 'claude']
    name: str = Field(min_length=1, max_length=200)
    scope: Literal['personal', 'shared'] = 'personal'
    access_grants: list[Grant] = Field(default_factory=list, max_length=1000)

    @field_validator('name')
    @classmethod
    def strip_name(cls, value):
        if not value.strip():
            raise ValueError('Connection name is required')
        return value.strip()


class CompleteForm(BaseModel):
    model_config = ConfigDict(extra='forbid')
    state: str = Field(min_length=1, max_length=200)
    callback: str = Field(min_length=1, max_length=16384)


class UpdateForm(BaseModel):
    model_config = ConfigDict(extra='forbid')
    name: str | None = Field(default=None, min_length=1, max_length=200)
    enabled: bool | None = None
    priority: int | None = Field(default=None, strict=True, ge=0, le=10000)
    access_grants: list[Grant] | None = Field(default=None, max_length=1000)

    @field_validator('name', 'enabled', 'priority', 'access_grants')
    @classmethod
    def reject_null(cls, value):
        if value is None:
            raise ValueError('Updated fields cannot be null')
        if isinstance(value, str):
            if not value.strip():
                raise ValueError('Connection name is required')
            return value.strip()
        return value


@router.get('/')
async def list_connections(user=Depends(get_verified_user)):
    return await subscriptions.list_connections(user)


@router.post('/authorize')
async def authorize(form: AuthorizeForm, user=Depends(get_verified_user)):
    return await subscriptions.authorize(user, form.provider, form.name, form.scope, [grant.model_dump() for grant in form.access_grants])


@router.post('/complete')
async def complete(form: CompleteForm, user=Depends(get_verified_user)):
    return await subscriptions.complete(user, form.state, form.callback)


@router.patch('/{connection_id}')
async def update_connection(connection_id: str, form: UpdateForm, user=Depends(get_verified_user)):
    return await subscriptions.update_connection(user, connection_id, form.model_dump(exclude_unset=True))


@router.post('/{connection_id}/models')
async def refresh_models(connection_id: str, user=Depends(get_verified_user)):
    return await subscriptions.refresh_models(user, connection_id)


@router.delete('/{connection_id}')
async def delete_connection(connection_id: str, user=Depends(get_verified_user)):
    return await subscriptions.delete_connection(user, connection_id)
