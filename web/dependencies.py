from __future__ import annotations

from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from sqlmodel import Session, select

from database.models import Tenant, User
from database.session import get_session
from services.settings_service import SettingsService


def get_current_user(
    request: Request,
    session: Session = Depends(get_session),
) -> Optional[User]:
    user_id = request.session.get("user_id")
    if not user_id:
        return None

    user = session.get(User, user_id)
    if user and not user.tenant_id:
        tenant = session.exec(select(Tenant).order_by(Tenant.id)).first()
        if tenant:
            user.tenant_id = tenant.id
            session.add(user)
            session.commit()
            session.refresh(user)
    return user


def require_auth(user: Optional[User] = Depends(get_current_user)) -> User:
    if not user:
        raise HTTPException(
            status_code=status.HTTP_302_FOUND,
            headers={"Location": "/login"},
        )
    return user


def get_tenant(user: User = Depends(require_auth)) -> int:
    if not user.tenant_id:
        raise HTTPException(status_code=403, detail="No tenant associated")
    return user.tenant_id


def get_settings(
    session: Session = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    tenant_id = user.tenant_id if user else None
    return SettingsService.get_or_create_settings(session, tenant_id=tenant_id)
