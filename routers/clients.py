"""routers/clients.py — Clientes + Cuenta Corriente"""
from __future__ import annotations
from datetime import date, datetime, timezone
from typing import Optional
import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, func, select
from database.models import Client, Payment, Sale, Settings, User
from database.session import get_session
from services.settings_service import SettingsService
from web.compat_templates import CompatTemplates
from web.dependencies import get_settings, get_tenant, require_auth
from web.pagination import paginate

router = APIRouter(tags=["Clients"])

def _templates():
    return CompatTemplates(directory="templates")

@router.get("/clients", response_class=HTMLResponse)
def get_clients_page(
    request: Request, 
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=100),
    user: User = Depends(require_auth), 
    settings: Settings = Depends(get_settings), 
    tenant_id: int = Depends(get_tenant), 
    session: Session = Depends(get_session)
):
    query = select(Client).where(Client.tenant_id == tenant_id, Client.is_deleted == False).order_by(Client.name)
    clients_page = paginate(session, query, page=page, size=size)
    
    balances = {c.id: sum(p.amount for p in c.payments) - sum(s.total_amount for s in c.sales) for c in clients_page.items}
    
    return _templates().TemplateResponse("clients.html", {
        "request": request, 
        "active_page": "clients", 
        "settings": settings, 
        "user": user, 
        "clients": clients_page.items,
        "total": clients_page.total,
        "current_page": clients_page.page,
        "total_pages": clients_page.pages,
        "balances": balances
    })

@router.get("/clients/{id}/account", response_class=HTMLResponse)
def get_client_account(id: int, request: Request, user: User = Depends(require_auth), settings: Settings = Depends(get_settings), tenant_id: int = Depends(get_tenant), session: Session = Depends(get_session)):
    client = session.get(Client, id)
    if not client or client.tenant_id != tenant_id:
        raise HTTPException(404, "Client not found")
    sales = session.exec(select(Sale).where(Sale.client_id == id, Sale.tenant_id == tenant_id)).all()
    payments_list = session.exec(select(Payment).where(Payment.client_id == id, Payment.tenant_id == tenant_id)).all()
    total_debt = sum((s.total_amount or 0.0) for s in sales)
    total_paid = sum((p.amount or 0.0) for p in payments_list)
    balance = float(total_debt - total_paid)
    movements = []
    def _sort_date(dt_value):
        if dt_value is None: return datetime.min.replace(tzinfo=timezone.utc)
        if isinstance(dt_value, date) and not isinstance(dt_value, datetime): dt_value = datetime.combine(dt_value, datetime.min.time())
        if getattr(dt_value, "tzinfo", None) is None: return dt_value.replace(tzinfo=timezone.utc)
        return dt_value
    for s in sales:
        for item in s.items:
            movements.append({"date": s.timestamp, "type": "Mercaderia", "quantity": item.quantity, "description": item.product_name, "price": item.unit_price, "debit": item.total, "credit": 0.0})
        if (s.amount_cash or 0.0) > 0:
            movements.append({"date": s.timestamp, "type": "Efectivo", "quantity": 0, "description": f"Pago efectivo s/Venta #{s.id}", "price": 0.0, "debit": 0.0, "credit": s.amount_cash})
        if (s.amount_transfer or 0.0) > 0:
            movements.append({"date": s.timestamp, "type": "Transf", "quantity": 0, "description": f"Pago transf s/Venta #{s.id}", "price": 0.0, "debit": 0.0, "credit": s.amount_transfer})
        if (s.amount_cash or 0.0) == 0 and (s.amount_transfer or 0.0) == 0 and (s.amount_paid or 0.0) > 0:
            movements.append({"date": s.timestamp, "type": s.payment_method.title() if s.payment_method else "Pago", "quantity": 0, "description": f"Pago s/Venta #{s.id}", "price": 0.0, "debit": 0.0, "credit": s.amount_paid})
    for p in payments_list:
        movements.append({"date": p.date, "type": "Abono", "quantity": 0, "description": p.note or "Abono a cuenta corriente", "price": 0.0, "debit": 0.0, "credit": p.amount})
    movements.sort(key=lambda x: _sort_date(x.get("date")))
    current_balance = 0.0
    for m in movements:
        current_balance += m["debit"] or 0.0
        current_balance -= m["credit"] or 0.0
        m["running_balance"] = current_balance
    return _templates().TemplateResponse("client_account.html", {"request": request, "active_page": "clients", "settings": settings, "user": user, "client": client, "balance": round(balance, 2), "movements": movements})

@router.post("/api/clients/{id}/pay")
def register_payment(id: int, amount: float = Form(...), note: Optional[str] = Form(None), session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    client = session.get(Client, id)
    if not client or client.tenant_id != tenant_id: raise HTTPException(404, "Client not found")
    session.add(Payment(tenant_id=tenant_id, client_id=id, amount=amount, note=note))
    session.commit()
    return RedirectResponse(f"/clients/{id}/account", status_code=303)

@router.get("/api/clients")
def get_clients_api(session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    return session.exec(select(Client).where(Client.tenant_id == tenant_id)).all()

@router.post("/api/clients")
def create_client_api(name: str = Form(...), phone: Optional[str] = Form(None), email: Optional[str] = Form(None), address: Optional[str] = Form(None), credit_limit: Optional[float] = Form(None), razon_social: Optional[str] = Form(None), cuit: Optional[str] = Form(None), iva_category: Optional[str] = Form(None), transport_name: Optional[str] = Form(None), transport_address: Optional[str] = Form(None), session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    client = Client(tenant_id=tenant_id, name=name, phone=phone, email=email, address=address, credit_limit=credit_limit, razon_social=razon_social, cuit=cuit, iva_category=iva_category, transport_name=transport_name, transport_address=transport_address)
    session.add(client)
    session.commit()
    return client

@router.put("/api/clients/{id}")
def update_client_api(id: int, name: str = Form(...), phone: Optional[str] = Form(None), email: Optional[str] = Form(None), address: Optional[str] = Form(None), credit_limit: Optional[float] = Form(None), razon_social: Optional[str] = Form(None), cuit: Optional[str] = Form(None), iva_category: Optional[str] = Form(None), transport_name: Optional[str] = Form(None), transport_address: Optional[str] = Form(None), session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    client = session.get(Client, id)
    if not client or client.tenant_id != tenant_id: raise HTTPException(404, "Not found")
    client.name, client.phone, client.email, client.address = name, phone, email, address
    client.credit_limit, client.razon_social, client.cuit = credit_limit, razon_social, cuit
    client.iva_category, client.transport_name, client.transport_address = iva_category, transport_name, transport_address
    session.add(client)
    session.commit()
    return client

@router.delete("/api/clients/{id}")
def delete_client_api(id: int, session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    client = session.get(Client, id)
    if not client or client.tenant_id != tenant_id: raise HTTPException(404, "Not found")
    client.is_deleted = True
    session.add(client)
    session.commit()
    return {"ok": True}

@router.post("/api/import/clients")
async def import_clients(file: UploadFile = File(...), session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    SettingsService.ensure_admin(user)
    import io
    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents))
    added, errors = 0, []
    for index, row in df.iterrows():
        try:
            name = str(row.get("Name", "")).strip()
            if not name or pd.isna(name): continue
            get_val = lambda col, d=None: str(row.get(col)).strip() if not pd.isna(row.get(col)) else d
            existing = session.exec(select(Client).where(Client.name == name, Client.tenant_id == tenant_id)).first()
            cl = row.get("CreditLimit")
            cl = None if pd.isna(cl) else float(cl)
            if existing:
                for attr, col in [("phone","Phone"),("email","Email"),("address","Address"),("razon_social","RazonSocial"),("cuit","CUIT"),("iva_category","IVACategory"),("transport_name","TransportName"),("transport_address","TransportAddress")]:
                    v = get_val(col)
                    if v: setattr(existing, attr, v)
                if cl is not None: existing.credit_limit = cl
                session.add(existing)
            else:
                session.add(Client(tenant_id=tenant_id, name=name, phone=get_val("Phone"), email=get_val("Email"), address=get_val("Address"), razon_social=get_val("RazonSocial"), cuit=get_val("CUIT"), iva_category=get_val("IVACategory"), credit_limit=cl, transport_name=get_val("TransportName"), transport_address=get_val("TransportAddress")))
                added += 1
        except Exception as e:
            errors.append(f"Row {index}: {e!s}")
    session.commit()
    return {"added": added, "errors": errors}
