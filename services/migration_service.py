from __future__ import annotations

from sqlmodel import Session, select

from database.models import Product, Tenant
from database.session import create_db_and_tables


def run_schema_migrations(session: Session) -> list[str]:
    create_db_and_tables()
    results = ["Core tables checked"]

    tenant = session.exec(select(Tenant).order_by(Tenant.id)).first()
    tenant_id = tenant.id if tenant else 1

    new_products_data = [
        {"item_number": "7111", "name": "Gomon Pin Negro", "price": 7500.0, "numeracion": "35-40", "cant_bulto": 12, "category": "Verano", "stock_quantity": 100},
        {"item_number": "7110", "name": "Articulo 7110", "price": 13000.0, "numeracion": "35-40", "cant_bulto": 12, "category": "Verano", "stock_quantity": 100},
        {"item_number": "7098", "name": "Gomon NO Pin", "price": 6000.0, "numeracion": "35-40", "cant_bulto": 12, "category": "Verano", "stock_quantity": 100},
        {"item_number": "7083", "name": "1/2 Alto", "price": 8500.0, "numeracion": "35-40", "cant_bulto": 12, "category": "Verano", "stock_quantity": 100},
        {"item_number": "7091", "name": "Articulo 7091", "price": 7200.0, "numeracion": "35/6-39/0", "cant_bulto": 12, "category": "Verano", "stock_quantity": 100},
    ]

    products_added = 0
    for p_data in new_products_data:
        existing = session.exec(
            select(Product).where(
                Product.item_number == p_data["item_number"],
                Product.tenant_id == tenant_id,
            )
        ).first()
        if existing:
            continue

        barcode_val = p_data["item_number"] if len(p_data["item_number"]) >= 4 else p_data["item_number"].zfill(8)
        session.add(Product(tenant_id=tenant_id, barcode=barcode_val, **p_data))
        products_added += 1

    if products_added:
        session.commit()
        results.append(f"Seeded {products_added} migration products for tenant {tenant_id}")
    else:
        results.append("No migration seed changes needed")

    return results
