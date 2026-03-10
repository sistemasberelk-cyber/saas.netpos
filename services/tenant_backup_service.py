from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import HTTPException
from sqlalchemy import delete
from sqlmodel import Session, select

from database.models import Client, Payment, Product, Sale, SaleItem, Settings, User


def _serialize_datetime(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def export_tenant_snapshot(session: Session, tenant_id: int) -> dict[str, Any]:
    sale_ids = session.exec(select(Sale.id).where(Sale.tenant_id == tenant_id)).all()
    sale_items = []
    if sale_ids:
        sale_items = [i.model_dump() for i in session.exec(select(SaleItem).where(SaleItem.sale_id.in_(sale_ids))).all()]

    sales = []
    for sale in session.exec(select(Sale).where(Sale.tenant_id == tenant_id)).all():
        row = sale.model_dump()
        row["timestamp"] = _serialize_datetime(sale.timestamp)
        sales.append(row)

    payments = []
    for payment in session.exec(select(Payment).where(Payment.tenant_id == tenant_id)).all():
        row = payment.model_dump()
        row["date"] = _serialize_datetime(payment.date)
        payments.append(row)

    return {
        "version": "2.1",
        "generated_at": datetime.utcnow().isoformat(),
        "tenant_id": tenant_id,
        "products": [p.model_dump() for p in session.exec(select(Product).where(Product.tenant_id == tenant_id)).all()],
        "clients": [c.model_dump() for c in session.exec(select(Client).where(Client.tenant_id == tenant_id)).all()],
        "users": [u.model_dump() for u in session.exec(select(User).where(User.tenant_id == tenant_id)).all()],
        "settings": [s.model_dump() for s in session.exec(select(Settings).where(Settings.tenant_id == tenant_id)).all()],
        "sales": sales,
        "sale_items": sale_items,
        "payments": payments,
    }


def restore_tenant_snapshot(session: Session, tenant_id: int, data: dict[str, Any]) -> dict[str, Any]:
    required_keys = {"products", "clients"}
    if not required_keys.issubset(data.keys()):
        raise HTTPException(400, detail="Invalid backup format: missing 'products' or 'clients'")

    valid_columns = {
        Product: {c.name for c in Product.__table__.columns},
        Client: {c.name for c in Client.__table__.columns},
        User: {c.name for c in User.__table__.columns},
        Settings: {c.name for c in Settings.__table__.columns},
        Sale: {c.name for c in Sale.__table__.columns},
        SaleItem: {c.name for c in SaleItem.__table__.columns},
        Payment: {c.name for c in Payment.__table__.columns},
    }

    def _safe_fields(model_class, row: dict, *, force_tenant: bool = False) -> dict[str, Any]:
        filtered = {k: v for k, v in row.items() if k in valid_columns[model_class]}
        if force_tenant and "tenant_id" in valid_columns[model_class]:
            filtered["tenant_id"] = tenant_id
        return filtered

    sale_ids = session.exec(select(Sale.id).where(Sale.tenant_id == tenant_id)).all()
    if sale_ids:
        session.exec(delete(SaleItem).where(SaleItem.sale_id.in_(sale_ids)))
    session.exec(delete(Sale).where(Sale.tenant_id == tenant_id))
    session.exec(delete(Payment).where(Payment.tenant_id == tenant_id))
    session.exec(delete(Product).where(Product.tenant_id == tenant_id))
    session.exec(delete(Client).where(Client.tenant_id == tenant_id))
    session.exec(delete(User).where(User.tenant_id == tenant_id))
    session.exec(delete(Settings).where(Settings.tenant_id == tenant_id))
    session.commit()

    for row in data.get("products", []):
        session.add(Product(**_safe_fields(Product, row, force_tenant=True)))
    for row in data.get("clients", []):
        session.add(Client(**_safe_fields(Client, row, force_tenant=True)))
    for row in data.get("users", []):
        session.add(User(**_safe_fields(User, row, force_tenant=True)))
    for row in data.get("settings", []):
        session.add(Settings(**_safe_fields(Settings, row, force_tenant=True)))

    session.flush()

    for row in data.get("sales", []):
        payload = _safe_fields(Sale, row, force_tenant=True)
        if isinstance(payload.get("timestamp"), str):
            payload["timestamp"] = datetime.fromisoformat(payload["timestamp"])
        session.add(Sale(**payload))

    session.flush()

    for row in data.get("sale_items", []):
        session.add(SaleItem(**_safe_fields(SaleItem, row)))
    for row in data.get("payments", []):
        payload = _safe_fields(Payment, row, force_tenant=True)
        if isinstance(payload.get("date"), str):
            payload["date"] = datetime.fromisoformat(payload["date"])
        session.add(Payment(**payload))

    session.commit()
    return {"status": "success", "message": "Tenant restored successfully"}
