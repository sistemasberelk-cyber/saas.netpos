from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session, select
from database.session import get_session
from database.models import User, RefreshToken
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
from typing import Optional

router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str

class RefreshRequest(BaseModel):
    refresh_token: str

class RefreshResponse(BaseModel):
    access_token: str

class LogoutRequest(BaseModel):
    refresh_token: str

@router.post("/auth/login", response_model=TokenResponse)
def login(req: LoginRequest, session: Session = Depends(get_session)):
    user = session.exec(select(User).where(User.username == req.username)).first()
    if not user or not user.is_active or user.is_deleted:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas"
        )
    from services.auth_service import AuthService
    if not AuthService.verify_password(req.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas"
        )
    
    from services.jwt_service import create_access_token, create_refresh_token, hash_refresh_token
    access = create_access_token(user.id, user.tenant_id, user.role)
    raw_refresh = create_refresh_token()
    refresh_hash = hash_refresh_token(raw_refresh)
    
    expires = datetime.now(timezone.utc) + timedelta(days=7)
    
    db_token = RefreshToken(
        user_id=user.id,
        tenant_id=user.tenant_id,
        token_hash=refresh_hash,
        expires_at=expires
    )
    session.add(db_token)
    session.commit()
    
    return TokenResponse(access_token=access, refresh_token=raw_refresh)

@router.post("/auth/refresh", response_model=RefreshResponse)
def refresh(req: RefreshRequest, session: Session = Depends(get_session)):
    from services.jwt_service import hash_refresh_token, create_access_token
    r_hash = hash_refresh_token(req.refresh_token)
    db_token = session.exec(select(RefreshToken).where(RefreshToken.token_hash == r_hash)).first()
    if not db_token or db_token.revoked_at is not None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de refresco inválido o revocado"
        )
    
    expires = db_token.expires_at
    if expires.tzinfo is None:
        now = datetime.now()
    else:
        now = datetime.now(timezone.utc)
        
    if expires < now:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de refresco expirado"
        )
    
    user = session.get(User, db_token.user_id)
    if not user or not user.is_active or user.is_deleted:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario inactivo o no encontrado"
        )
    
    access = create_access_token(user.id, user.tenant_id, user.role)
    return RefreshResponse(access_token=access)

@router.post("/auth/logout")
def logout(req: LogoutRequest, session: Session = Depends(get_session)):
    from services.jwt_service import hash_refresh_token
    r_hash = hash_refresh_token(req.refresh_token)
    db_token = session.exec(select(RefreshToken).where(RefreshToken.token_hash == r_hash)).first()
    if db_token:
        db_token.revoked_at = datetime.now(timezone.utc) if db_token.expires_at.tzinfo is not None else datetime.now()
        session.add(db_token)
        session.commit()
    return {"message": "Sesión cerrada correctamente"}
