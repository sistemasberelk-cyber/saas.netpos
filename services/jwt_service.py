import jwt
from datetime import datetime, timedelta, timezone
import hashlib
import os
import secrets

SECRET_KEY = os.environ.get("SECRET_KEY", "testsecretkey123")
ALGORITHM = "HS256"

def create_access_token(user_id: int, tenant_id: int, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    payload = {
        "sub": str(user_id),
        "tenant_id": tenant_id,
        "role": role,
        "exp": expire
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token() -> str:
    return secrets.token_hex(32)

def hash_refresh_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()

def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise ValueError("Token expired")
    except jwt.InvalidTokenError:
        raise ValueError("Invalid token")
