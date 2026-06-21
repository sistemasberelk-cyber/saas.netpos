import os
os.environ["SECRET_KEY"] = "testsecretkey123"
os.environ["NEXPOS_FERNET_KEY"] = "I9StON-hofzi783VWEhFYFM1DCXGJc08SBE1olJhDqI="

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from main import app
from database.session import get_session
from database.models import User, Tenant
from services.auth_service import AuthService

from sqlalchemy.pool import StaticPool

@pytest.fixture
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

@pytest.fixture
def client(session):
    app.dependency_overrides[get_session] = lambda: session
    yield TestClient(app)
    app.dependency_overrides.clear()

def test_login_success(client, session):
    tenant = Tenant(name="Test Tenant", subdomain="test")
    session.add(tenant)
    session.commit()
    
    hashed = AuthService.get_password_hash("password123")
    user = User(username="testuser", password_hash=hashed, role="admin", tenant_id=tenant.id)
    session.add(user)
    session.commit()
    
    response = client.post("/api/v1/auth/login", json={"username": "testuser", "password": "password123"})
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data

def test_login_invalid_credentials(client, session):
    response = client.post("/api/v1/auth/login", json={"username": "wrong", "password": "wrong"})
    assert response.status_code == 401

def test_refresh_token_success(client, session):
    tenant = Tenant(name="Test Tenant", subdomain="test")
    session.add(tenant)
    session.commit()
    
    hashed = AuthService.get_password_hash("password123")
    user = User(username="testuser", password_hash=hashed, role="admin", tenant_id=tenant.id)
    session.add(user)
    session.commit()
    
    response = client.post("/api/v1/auth/login", json={"username": "testuser", "password": "password123"})
    assert response.status_code == 200
    refresh_token = response.json()["refresh_token"]
    
    response_refresh = client.post("/api/v1/auth/refresh", json={"refresh_token": refresh_token})
    assert response_refresh.status_code == 200
    assert "access_token" in response_refresh.json()

def test_logout_revokes_refresh_token(client, session):
    tenant = Tenant(name="Test Tenant", subdomain="test")
    session.add(tenant)
    session.commit()
    
    hashed = AuthService.get_password_hash("password123")
    user = User(username="testuser", password_hash=hashed, role="admin", tenant_id=tenant.id)
    session.add(user)
    session.commit()
    
    response = client.post("/api/v1/auth/login", json={"username": "testuser", "password": "password123"})
    refresh_token = response.json()["refresh_token"]
    
    response_logout = client.post("/api/v1/auth/logout", json={"refresh_token": refresh_token})
    assert response_logout.status_code == 200
    
    response_refresh = client.post("/api/v1/auth/refresh", json={"refresh_token": refresh_token})
    assert response_refresh.status_code == 401
