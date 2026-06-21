import os
os.environ["SECRET_KEY"] = "testsecretkey123"
os.environ["NEXPOS_FERNET_KEY"] = "I9StON-hofzi783VWEhFYFM1DCXGJc08SBE1olJhDqI="

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from main import app
from database.session import get_session
from database.models import User, Tenant, Product
from services.jwt_service import create_access_token

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

def test_get_products_requires_auth(client):
    response = client.get("/api/v1/products")
    assert response.status_code == 422

def test_get_products_authorized(client, session):
    tenant = Tenant(name="T1", subdomain="t1")
    session.add(tenant)
    session.commit()
    
    user = User(username="u1", password_hash="hash", role="admin", tenant_id=tenant.id)
    session.add(user)
    session.commit()
    
    product = Product(tenant_id=tenant.id, name="Product 1", barcode="111", price=10.0)
    session.add(product)
    session.commit()
    
    token = create_access_token(user.id, tenant.id, user.role)
    headers = {"Authorization": f"Bearer {token}"}
    
    response = client.get("/api/v1/products", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["name"] == "Product 1"

def test_cross_tenant_product_isolation(client, session):
    tenant1 = Tenant(name="T1", subdomain="t1")
    tenant2 = Tenant(name="T2", subdomain="t2")
    session.add(tenant1)
    session.add(tenant2)
    session.commit()
    
    user1 = User(username="u1", password_hash="hash", role="admin", tenant_id=tenant1.id)
    session.add(user1)
    session.commit()
    
    p2 = Product(tenant_id=tenant2.id, name="Product Tenant 2", barcode="222", price=20.0)
    session.add(p2)
    session.commit()
    
    token = create_access_token(user1.id, tenant1.id, user1.role)
    headers = {"Authorization": f"Bearer {token}"}
    
    response_list = client.get("/api/v1/products", headers=headers)
    assert response_list.status_code == 200
    assert len(response_list.json()["items"]) == 0
    
    response_get = client.get(f"/api/v1/products/{p2.id}", headers=headers)
    assert response_get.status_code == 404
