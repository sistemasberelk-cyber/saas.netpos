"""tests/test_price_logic.py — Tests para la lógica de precios y bultos"""
import pytest
from sqlmodel import Session, SQLModel, create_engine
from database.models import Product, Sale, Tenant
from services.stock_service import StockService

@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

def test_price_bulk_logic(session):
    # Setup
    tenant = Tenant(name="Test Tenant")
    session.add(tenant)
    session.commit()

    from database.models import Location, Bin
    location = Location(tenant_id=tenant.id, name="Depósito Central", code="DEP1")
    session.add(location)
    session.commit()

    bin_ = Bin(tenant_id=tenant.id, location_id=location.id, name="SIN-UBICACION")
    session.add(bin_)
    session.commit()
    
    product = Product(
        tenant_id=tenant.id,
        name="Test Product",
        barcode="123",
        price=100.0,
        price_bulk=80.0,
        cant_bulto=10
    )
    session.add(product)
    session.commit()
    
    service = StockService(static_dir="static/barcodes")
    service.add_stock(session, product.id, tenant.id, 100, "ingreso", "Stock inicial")
    
    # Caso 1: Venta menor al bulto (debe cobrar precio unitario 100)
    sale1 = service.process_sale(session, user_id=1, tenant_id=tenant.id, items_data=[{"product_id": product.id, "quantity": 5}])
    assert sale1.items[0].unit_price == 100.0
    assert sale1.total_amount == 500.0
    
    # Caso 2: Venta igual o mayor al bulto (debe cobrar precio bulto 80)
    sale2 = service.process_sale(session, user_id=1, tenant_id=tenant.id, items_data=[{"product_id": product.id, "quantity": 10}])
    assert sale2.items[0].unit_price == 80.0
    assert sale2.total_amount == 800.0
    
    # Caso 3: Producto sin precio bulto (debe cobrar unitario)
    product2 = Product(tenant_id=tenant.id, name="P2", barcode="456", price=50.0)
    session.add(product2)
    session.commit()
    service.add_stock(session, product2.id, tenant.id, 10, "ingreso", "Stock inicial")
    
    sale3 = service.process_sale(session, user_id=1, tenant_id=tenant.id, items_data=[{"product_id": product2.id, "quantity": 5}])
    assert sale3.items[0].unit_price == 50.0
