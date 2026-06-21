import os
os.environ["SECRET_KEY"] = "testsecretkey123"
os.environ["NEXPOS_FERNET_KEY"] = "I9StON-hofzi783VWEhFYFM1DCXGJc08SBE1olJhDqI="

import pytest
import json
from unittest.mock import MagicMock, AsyncMock
import httpx
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine, select
from main import app
from database.session import get_session
from database.models import User, Tenant, UIConfig, AICredential, encrypt_api_key
from services.jwt_service import create_access_token
from services.gemini_service import GeminiService
from web.scheduler import run_daily_theme_generation
from sqlalchemy.pool import StaticPool

# Mocking response structures for Gemini API
MOCK_UI_RESPONSE = {
    "candidates": [
        {
            "content": {
                "parts": [
                    {
                        "text": json.dumps({
                            "layout": {
                                "modules": ["Header", "ProductSearchBox", "CatalogGrid"],
                                "grid_cols": 12,
                                "layout_structure": {
                                    "Header": {"row": 1, "col_span": 12}
                                }
                            },
                            "theme": {
                                "primary_color": "#112233",
                                "secondary_color": "#445566",
                                "mode": "dark",
                                "background_gradient": "linear-gradient(#112233, #000)",
                                "border_radius": "8px",
                                "font_family": "Roboto"
                            }
                        })
                    }
                ]
            }
        }
    ]
}

MOCK_DAILY_THEME_RESPONSE = {
    "candidates": [
        {
            "content": {
                "parts": [
                    {
                        "text": json.dumps({
                            "theme": {
                                "primary_color": "#FFA500",
                                "secondary_color": "#008000",
                                "mode": "light",
                                "background_gradient": "linear-gradient(to right, orange, green)",
                                "border_radius": "15px",
                                "font_family": "Outfit"
                            }
                        })
                    }
                ]
            }
        }
    ]
}

@pytest.fixture
def anyio_backend():
    return 'asyncio'

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

@pytest.mark.anyio
async def test_generate_ui_endpoint(client, session, monkeypatch):
    tenant = Tenant(name="Tenant 1", subdomain="t1")
    session.add(tenant)
    session.commit()

    admin = User(username="admin", password_hash="hash", role="admin", tenant_id=tenant.id)
    session.add(admin)
    session.commit()

    # Configure Gemini API credential for Tenant
    cred = AICredential(
        tenant_id=tenant.id,
        provider="gemini",
        api_key_enc=encrypt_api_key("fake-gemini-key")
    )
    session.add(cred)
    session.commit()

    # Mock Gemini HTTP Call
    async def mock_post(*args, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json = lambda: MOCK_UI_RESPONSE
        return mock_resp

    monkeypatch.setattr(httpx.AsyncClient, "post", mock_post)

    token = create_access_token(admin.id, tenant.id, admin.role)
    headers = {"Authorization": f"Bearer {token}"}

    response = client.post(
        "/api/v1/ui/generate",
        json={"page_name": "pos", "prompt": "quiero colores de invierno y buscador de productos arriba"},
        headers=headers
    )

    assert response.status_code == 200
    data = response.json()
    assert data["layout"]["modules"] == ["Header", "ProductSearchBox", "CatalogGrid"]
    assert data["theme"]["primary_color"] == "#112233"

    # Verify db was updated
    config = session.exec(select(UIConfig).where(UIConfig.tenant_id == tenant.id, UIConfig.page_name == "pos")).first()
    assert config is not None
    assert "CatalogGrid" in config.layout_json

@pytest.mark.anyio
async def test_generate_ui_seller_forbidden(client, session):
    tenant = Tenant(name="Tenant 1", subdomain="t1")
    session.add(tenant)
    session.commit()

    seller = User(username="seller", password_hash="hash", role="vendedor", tenant_id=tenant.id)
    session.add(seller)
    session.commit()

    token = create_access_token(seller.id, tenant.id, seller.role)
    headers = {"Authorization": f"Bearer {token}"}

    response = client.post(
        "/api/v1/ui/generate",
        json={"page_name": "pos", "prompt": "make it look nice"},
        headers=headers
    )
    assert response.status_code == 403  # Forbidden

@pytest.mark.anyio
async def test_force_daily_theme_endpoint(client, session, monkeypatch):
    tenant = Tenant(name="Tenant 1", subdomain="t1")
    session.add(tenant)
    session.commit()

    admin = User(username="admin", password_hash="hash", role="admin", tenant_id=tenant.id)
    session.add(admin)
    session.commit()

    # Configure Gemini API credential for Tenant
    cred = AICredential(
        tenant_id=tenant.id,
        provider="gemini",
        api_key_enc=encrypt_api_key("fake-gemini-key")
    )
    session.add(cred)
    session.commit()

    # Mock Gemini HTTP Call
    async def mock_post(*args, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json = lambda: MOCK_DAILY_THEME_RESPONSE
        return mock_resp

    monkeypatch.setattr(httpx.AsyncClient, "post", mock_post)

    token = create_access_token(admin.id, tenant.id, admin.role)
    headers = {"Authorization": f"Bearer {token}"}

    response = client.post(
        "/api/v1/ui/cron/daily-theme?page_name=pos",
        headers=headers
    )

    assert response.status_code == 200
    data = response.json()
    assert "Tema diario generado con éxito" in data["message"]
    assert data["theme"]["primary_color"] == "#FFA500"

    # Verify db
    config = session.exec(select(UIConfig).where(UIConfig.tenant_id == tenant.id, UIConfig.page_name == "pos")).first()
    assert config is not None
    assert "#FFA500" in config.theme_json

@pytest.mark.anyio
async def test_scheduler_daily_theme_task(session, monkeypatch):
    tenant = Tenant(name="Test Tenant", subdomain="test")
    session.add(tenant)
    session.commit()

    # Configure Gemini API credential for Tenant
    cred = AICredential(
        tenant_id=tenant.id,
        provider="gemini",
        api_key_enc=encrypt_api_key("fake-gemini-key")
    )
    session.add(cred)
    session.commit()

    # Mock Gemini HTTP Call
    async def mock_post(*args, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json = lambda: MOCK_DAILY_THEME_RESPONSE
        return mock_resp

    monkeypatch.setattr(httpx.AsyncClient, "post", mock_post)

    # Patch global engine in web.scheduler with our test in-memory engine
    from web import scheduler
    monkeypatch.setattr(scheduler, "engine", session.bind)

    # Execute scheduler task
    await run_daily_theme_generation()

    # Verify db record was created
    config = session.exec(select(UIConfig).where(UIConfig.tenant_id == tenant.id, UIConfig.page_name == "pos")).first()
    assert config is not None
    assert "#FFA500" in config.theme_json
