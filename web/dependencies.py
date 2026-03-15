from __future__ import annotations

from typing import Optional
import os

from fastapi import Depends, HTTPException, Request, status
from sqlmodel import Session, select

from database.models import Tenant, User
from database.session import get_session
from services.settings_service import SettingsService


def _resolve_tenant_from_host(host: str, session: Session) -> Optional[int]:
    """
    If BASE_DOMAIN is set (e.g. "tudominio.com"), resolve tenant by subdomain.
    Example: acme.tudominio.com -> tenant with subdomain "acme".
    """
    base_domain = os.getenv("BASE_DOMAIN")
    if not base_domain:
        return None

    hostname = (host or "").split(":")[0].lower()
    base_domain = base_domain.lower()
    if hostname == base_domain:
        return None
    if not hostname.endswith("." + base_domain):
        return None

    subdomain = hostname.replace("." + base_domain, "")
    tenant = session.exec(select(Tenant).where(Tenant.subdomain == subdomain)).first()
    return tenant.id if tenant else None


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


def get_tenant(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
) -> int:
    # Resolve tenant by host if configured
    host_tenant_id = _resolve_tenant_from_host(request.headers.get("host"), session)
    if host_tenant_id and user.tenant_id and user.tenant_id != host_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch for this domain")
    if host_tenant_id:
        return host_tenant_id
    if not user.tenant_id:
        raise HTTPException(status_code=403, detail="No tenant associated")
    return user.tenant_id


def require_superadmin(user: User = Depends(require_auth)) -> User:
    if user.role != "admin" or user.tenant_id != 1:
        raise HTTPException(status_code=403, detail="Superadmin required")
    return user


def get_settings(
    request: Request,
    session: Session = Depends(get_session),
    user: Optional[User] = Depends(get_current_user),
):
    tenant_id = user.tenant_id if user else None
    if tenant_id is None:
        host_tenant = _resolve_tenant_from_host(request.headers.get("host"), session)
        tenant_id = host_tenant if host_tenant else None
    return SettingsService.get_or_create_settings(session, tenant_id=tenant_id)
