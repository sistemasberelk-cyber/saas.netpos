from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlmodel import Session, select

from database.models import Product, Sale, SaleItem, Settings, User
from database.session import get_session
from web.dependencies import get_settings, get_tenant, require_auth

router = APIRouter()


def _templates():
    from fastapi.templating import Jinja2Templates
    return Jinja2Templates(directory="templates")


def _find_product(session: Session, tenant_id: int, search_term: str):
    product = session.exec(select(Product).where(Product.barcode == search_term, Product.tenant_id == tenant_id)).first()
    if not product:
        product = session.exec(select(Product).where(Product.item_number == search_term, Product.tenant_id == tenant_id)).first()
    if not product and len(search_term) >= 4:
        prefixes = [search_term[:i] for i in range(3, min(len(search_term), 6))]
        candidates = session.exec(select(Product).where(Product.item_number.in_(prefixes), Product.tenant_id == tenant_id)).all()
        for candidate in sorted(candidates, key=lambda x: len(x.item_number or ""), reverse=True):
            if candidate.item_number and search_term.startswith(candidate.item_number):
                product = candidate
                break
    return product


@router.get("/picking", response_class=HTMLResponse)
def picking_page(request: Request, user: User = Depends(require_auth), settings: Settings = Depends(get_settings)):
    return _templates().TemplateResponse("picking.html", {"request": request, "user": user, "settings": settings, "active_page": "picking"})


@router.post("/api/picking/entry")
def picking_entry(
    barcode: str = Form(...),
    qty: int = Form(1),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    product = _find_product(session, tenant_id, barcode.strip())
    if not product:
        raise HTTPException(404, f"Producto no encontrado: {barcode.strip()}")
    product.stock_quantity += qty
    session.add(product)
    session.commit()
    session.refresh(product)
    return {"status": "ok", "product": {"name": product.name, "new_stock": product.stock_quantity}}


class PickingItem(BaseModel):
    barcode: str
    qty: int


class PickingExitRequest(BaseModel):
    items: List[PickingItem]


@router.post("/api/picking/exit")
def picking_exit(
    data: PickingExitRequest,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    products_map = {}
    total_amount = 0.0
    for item in data.items:
        prod = _find_product(session, tenant_id, item.barcode.strip())
        if not prod:
            raise HTTPException(404, f"Producto no encontrado: {item.barcode}")
        products_map[item.barcode] = prod
        total_amount += prod.price * item.qty

    new_sale = Sale(tenant_id=tenant_id, client_id=None, user_id=user.id, total_amount=total_amount)
    session.add(new_sale)
    session.commit()
    session.refresh(new_sale)

    for item in data.items:
        prod = products_map[item.barcode]
        session.add(
            SaleItem(
                sale_id=new_sale.id,
                product_id=prod.id,
                product_name=prod.name,
                quantity=item.qty,
                unit_price=prod.price,
                total=prod.price * item.qty,
            )
        )
        prod.stock_quantity -= item.qty
        session.add(prod)

    session.commit()
    return {"status": "ok", "sale_id": new_sale.id, "print_url": f"/sales/{new_sale.id}/remito"}
