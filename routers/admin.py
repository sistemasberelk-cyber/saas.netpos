from __future__ import annotations

import gzip
import json
import os
import uuid
from io import BytesIO
from typing import Optional
from datetime import datetime, date, timedelta
from sqlalchemy import case

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response, StreamingResponse
from sqlmodel import Session, delete, select

from database.models import Client, Product, Sale, Settings, User, Tenant, SaleItem, CashMovement, Supplier, Payment, Purchase, AICredential
from database.session import get_session
from services.auth_service import AuthService
from services.database_backup_service import create_backup_file, get_local_backup_path, list_local_backups
from services.migration_service import run_schema_migrations
from services.settings_service import SettingsService
from services.tenant_backup_service import export_tenant_snapshot, restore_tenant_snapshot
from services.purchase_service import PurchaseService
from web.dependencies import get_settings, get_tenant, require_auth, require_superadmin
from sqlmodel import func, col
import requests

router = APIRouter()

SUPPORT_CONSTITUTION = """
Eres el asistente oficial de soporte del sistema NexPos Cloud.
Reglas:
- Responde en español, tono breve, claro y amable.
- No inventes datos: usa solo la información suministrada por el backend (KPIs y contexto).
- Si falta dato o rango de fechas, solicita un filtro concreto.
- No reveles credenciales ni keys. No pidas API keys al usuario final.
- Respeta el rol del usuario: cashier no ve costos ni gestión de usuarios; admin sí.
- Si la pregunta no es del negocio/soporte del sistema, indícalo y ofrece ayuda sobre reportes, ventas, stock, caja, usuarios o configuración.
Formato: máximo 120 palabras, sin HTML.
"""

def _templates():
    from fastapi.templating import Jinja2Templates
    return Jinja2Templates(directory="templates")


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, user: User = Depends(require_auth), settings: Settings = Depends(get_settings)):
    SettingsService.ensure_admin(user)
    return _templates().TemplateResponse("settings.html", {"request": request, "user": user, "settings": settings, "active_page": "settings"})


@router.get("/admin")
def admin_page(user: User = Depends(require_auth)):
    SettingsService.ensure_admin(user)
    return RedirectResponse(url="/settings", status_code=307)


@router.get("/api/settings")
def read_settings(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    return SettingsService.get_or_create_settings(session, tenant_id=tenant_id)


@router.post("/api/settings")
async def update_settings(
    request: Request,
    company_name: Optional[str] = Form(None),
    printer_name: Optional[str] = Form(None),
    label_width_mm: Optional[int] = Form(None),
    label_height_mm: Optional[int] = Form(None),
    logo_file: Optional[UploadFile] = File(None),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    form_data = await request.form()
    SettingsService.validate_supported_fields(form_data.keys())
    settings = SettingsService.get_or_create_settings(session, tenant_id=tenant_id)
    SettingsService.apply_updates(
        session=session,
        settings=settings,
        company_name=company_name,
        printer_name=printer_name,
        label_width_mm=label_width_mm,
        label_height_mm=label_height_mm,
        logo_file=logo_file,
    )
    return {"status": "success"}


@router.get("/migrate-schema")
def migrate_schema(session: Session = Depends(get_session), user: User = Depends(require_auth)):
    SettingsService.ensure_admin(user)
    return {"status": "success", "results": run_schema_migrations(session)}


@router.get("/api/backup")
def download_backup(
    user: User = Depends(require_auth),
    session: Session = Depends(get_session),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    data = export_tenant_snapshot(session, tenant_id)
    json_str = json.dumps(data, indent=2, default=str)
    return Response(
        content=json_str,
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="tenant_backup.json"'},
    )


@router.get("/api/users")
def get_users(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    return session.exec(select(User).where(User.tenant_id == tenant_id)).all()


@router.post("/api/users")
def create_user(
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form(...),
    full_name: Optional[str] = Form(None),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    hashed = AuthService.get_password_hash(password)
    new_user = User(
        username=username.strip(),
        password_hash=hashed,
        role=role,
        full_name=full_name,
        tenant_id=tenant_id,
    )
    session.add(new_user)
    try:
        session.commit()
        session.refresh(new_user)
    except Exception:
        session.rollback()
        raise HTTPException(400, "Username already exists")
    return new_user


@router.delete("/api/users/{id}")
def delete_user(
    id: int,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    if user.id == id:
        raise HTTPException(400, "Cannot delete yourself")
    target = session.get(User, id)
    if not target or target.tenant_id != tenant_id:
        raise HTTPException(404, "User not found")
    session.delete(target)
    session.commit()
    return {"ok": True}

# --- Password management ---

def _validate_password_strength(pwd: str):
    if len(pwd) < 12:
        raise HTTPException(400, "La clave debe tener al menos 12 caracteres.")
    if pwd.lower() == pwd or pwd.upper() == pwd:
        raise HTTPException(400, "Usa mayúsculas y minúsculas.")
    if not any(c.isdigit() for c in pwd):
        raise HTTPException(400, "Incluye al menos un número.")
    if not any(c in "!@#$%^&*()-_=+[]{};:,<.>/?\\|" for c in pwd):
        raise HTTPException(400, "Incluye al menos un símbolo.")


@router.post("/api/users/change-password")
def change_own_password(
    current_password: str = Form(...),
    new_password: str = Form(...),
    user: User = Depends(require_auth),
    session: Session = Depends(get_session),
):
    if not AuthService.verify_password(current_password, user.password_hash):
        raise HTTPException(400, "La clave actual es incorrecta")
    _validate_password_strength(new_password)
    user.password_hash = AuthService.get_password_hash(new_password)
    session.add(user)
    session.commit()
    return {"status": "ok"}


@router.post("/api/users/{id}/reset-password")
def admin_reset_password(
    id: int,
    new_password: str = Form(...),
    user: User = Depends(require_auth),
    session: Session = Depends(get_session),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    target = session.get(User, id)
    if not target or target.tenant_id != tenant_id:
        raise HTTPException(404, "Usuario no encontrado")
    _validate_password_strength(new_password)
    target.password_hash = AuthService.get_password_hash(new_password)
    session.add(target)
    session.commit()
    return {"status": "ok"}


@router.get("/api/ai/key")
def get_ai_key(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    cred = session.exec(select(AICredential).where(AICredential.tenant_id == tenant_id)).first()
    return {"exists": bool(cred), "provider": cred.provider if cred else None}


@router.post("/api/ai/key")
def set_ai_key(
    api_key: str = Form(...),
    provider: str = Form("gemini"),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    cred = session.exec(select(AICredential).where(AICredential.tenant_id == tenant_id)).first()
    if cred:
        cred.api_key = api_key.strip()
        cred.provider = provider
    else:
        cred = AICredential(tenant_id=tenant_id, api_key=api_key.strip(), provider=provider)
    session.add(cred)
    session.commit()
    return {"status": "ok"}


# --- Reports & Metrics ---

def _parse_date(date_str: Optional[str]) -> Optional[date]:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str).date()
    except Exception:
        return None


@router.get("/reports", response_class=HTMLResponse)
def reports_page(
    request: Request,
    user: User = Depends(require_auth),
    settings: Settings = Depends(get_settings),
):
    SettingsService.ensure_admin(user)
    return _templates().TemplateResponse(
        "reports.html",
        {"request": request, "user": user, "settings": settings, "active_page": "reports"},
    )


@router.get("/api/reports/summary")
def reports_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    export: Optional[str] = None,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)

    end_dt = _parse_date(end_date) or date.today()
    start_dt = _parse_date(start_date) or (end_dt - timedelta(days=29))
    start_ts = datetime.combine(start_dt, datetime.min.time())
    end_ts = datetime.combine(end_dt + timedelta(days=1), datetime.min.time())

    sales_rows = session.exec(
        select(
            func.date(Sale.timestamp).label("day"),
            func.sum(Sale.total_amount).label("total"),
        )
        .where(Sale.tenant_id == tenant_id, Sale.timestamp >= start_ts, Sale.timestamp < end_ts)
        .group_by(func.date(Sale.timestamp))
        .order_by(func.date(Sale.timestamp))
    ).all()
    sales_by_day = [{"day": str(r.day), "total": float(r.total or 0)} for r in sales_rows]

    top_rows = session.exec(
        select(
            SaleItem.product_name.label("product_name"),
            func.sum(SaleItem.quantity).label("units"),
            func.sum(SaleItem.total).label("amount"),
        )
        .join(Sale, Sale.id == SaleItem.sale_id)
        .where(Sale.tenant_id == tenant_id, Sale.timestamp >= start_ts, Sale.timestamp < end_ts)
        .group_by(SaleItem.product_name)
        .order_by(func.sum(SaleItem.total).desc())
        .limit(10)
    ).all()
    top_products = [
        {
            "product_name": r.product_name,
            "units": int(r.units or 0),
            "amount": float(r.amount or 0),
        }
        for r in top_rows
    ]

    cash_rows = session.exec(
        select(
            func.date(CashMovement.timestamp).label("day"),
            func.sum(case((CashMovement.movement_type == "in", CashMovement.amount), else_=0)).label("ingresos"),
            func.sum(case((CashMovement.movement_type == "out", CashMovement.amount), else_=0)).label("egresos"),
        )
        .where(CashMovement.tenant_id == tenant_id, CashMovement.timestamp >= start_ts, CashMovement.timestamp < end_ts)
        .group_by(func.date(CashMovement.timestamp))
        .order_by(func.date(CashMovement.timestamp))
    ).all()
    cash_by_day = []
    for r in cash_rows:
        ingresos = float(r.ingresos or 0)
        egresos = float(abs(r.egresos or 0))
        cash_by_day.append(
            {"day": str(r.day), "ingresos": ingresos, "egresos": egresos, "balance": ingresos - egresos}
        )

    # Client balances
    client_sales = session.exec(
        select(Sale.client_id, func.sum(Sale.total_amount).label("total"))
        .where(Sale.tenant_id == tenant_id)
        .group_by(Sale.client_id)
    ).all()
    client_sales_map = {row.client_id: float(row.total or 0) for row in client_sales if row.client_id}
    client_payments = session.exec(
        select(Payment.client_id, func.sum(Payment.amount).label("total"))
        .where(Payment.tenant_id == tenant_id)
        .group_by(Payment.client_id)
    ).all()
    client_pay_map = {row.client_id: float(row.total or 0) for row in client_payments if row.client_id}
    clients = session.exec(select(Client).where(Client.tenant_id == tenant_id)).all()
    client_balances = []
    for client in clients:
        sales_total = client_sales_map.get(client.id, 0.0)
        paid_total = client_pay_map.get(client.id, 0.0)
        balance = float(sales_total - paid_total)
        client_balances.append({"name": client.name, "balance": balance})

    suppliers = session.exec(select(Supplier).where(Supplier.tenant_id == tenant_id)).all()
    supplier_balances = [
        {"name": s.name, "balance": PurchaseService.get_supplier_balance(session, tenant_id, s.id)}
        for s in suppliers
    ]

    if export and export.lower() == "xlsx":
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame(sales_by_day).to_excel(writer, index=False, sheet_name="ventas_diarias")
            pd.DataFrame(top_products).to_excel(writer, index=False, sheet_name="top_productos")
            pd.DataFrame(cash_by_day).to_excel(writer, index=False, sheet_name="caja")
            pd.DataFrame(client_balances).to_excel(writer, index=False, sheet_name="clientes")
            pd.DataFrame(supplier_balances).to_excel(writer, index=False, sheet_name="proveedores")
        output.seek(0)
        filename = f"reporte_{start_dt.isoformat()}_{end_dt.isoformat()}.xlsx"
        return StreamingResponse(
            output,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return {
        "range": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
        "sales_by_day": sales_by_day,
        "top_products": top_products,
        "cash_by_day": cash_by_day,
        "client_balances": client_balances,
        "supplier_balances": supplier_balances,
    }


@router.post("/api/ai/chat")
def ai_chat(
    payload: dict,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    question = (payload.get("question") or "").strip()
    if not question:
        raise HTTPException(400, "Pregunta vacía")

    cred = session.exec(select(AICredential).where(AICredential.tenant_id == tenant_id)).first()
    if not cred or not cred.api_key:
        raise HTTPException(400, "Configura tu API key de Gemini en Configuración > IA")

    # Contexto breve: ventas últimas 7 días, top 3 productos, caja hoy
    today = date.today()
    start_dt = today - timedelta(days=6)
    summary = reports_summary(
        start_date=start_dt.isoformat(),
        end_date=today.isoformat(),
        export=None,
        session=session,
        user=user,
        tenant_id=tenant_id,
    )

    system_prompt = (
        SUPPORT_CONSTITUTION
    )
    context = {
        "empresa": session.exec(select(Settings).where(Settings.tenant_id == tenant_id)).first().company_name,
        "usuario": user.username,
        "rol": user.role,
        "kpis": summary,
    }

    try:
        res = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={cred.api_key}",
            json={
                "contents": [
                    {"role": "user", "parts": [{"text": system_prompt}]},
                    {"role": "user", "parts": [{"text": f"Contexto: {json.dumps(context)}"}]},
                    {"role": "user", "parts": [{"text": question}]},
                ]
            },
            timeout=8,
        )
        res.raise_for_status()
        data = res.json()
        text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        return {"answer": text.strip()}
    except Exception as exc:
        raise HTTPException(500, f"No se pudo obtener respuesta: {exc}")


# --- Tenants (Superadmin only) ---

@router.get("/tenants", response_class=HTMLResponse)
def list_tenants(
    request: Request,
    user: User = Depends(require_superadmin),
    session: Session = Depends(get_session),
):
    tenants = session.exec(select(Tenant)).all()
    users = session.exec(select(User)).all()
    user_counts = {}
    for u in users:
        user_counts[u.tenant_id] = user_counts.get(u.tenant_id, 0) + 1
    settings = SettingsService.get_or_create_settings(session, tenant_id=user.tenant_id)
    return _templates().TemplateResponse(
        "tenants.html",
        {
            "request": request,
            "user": user,
            "tenants": tenants,
            "user_counts": user_counts,
            "settings": settings,
            "active_page": "tenants",
        },
    )


@router.post("/api/tenants")
def create_tenant(
    name: str = Form(...),
    subdomain: Optional[str] = Form(None),
    admin_username: str = Form(...),
    admin_password: str = Form(...),
    admin_full_name: Optional[str] = Form(None),
    session: Session = Depends(get_session),
    user: User = Depends(require_superadmin),
):
    sub = (subdomain or "").strip().lower() or None
    if sub:
        existing = session.exec(select(Tenant).where(Tenant.subdomain == sub)).first()
        if existing:
            raise HTTPException(400, "Subdomain already in use")

    tenant = Tenant(name=name.strip(), subdomain=sub)
    session.add(tenant)
    session.commit()
    session.refresh(tenant)

    settings = Settings(tenant_id=tenant.id, company_name=name.strip(), logo_url="/static/images/logo.png")
    session.add(settings)

    hashed = AuthService.get_password_hash(admin_password)
    new_admin = User(
        username=admin_username.strip(),
        password_hash=hashed,
        role="admin",
        full_name=admin_full_name or f"Admin {name}",
        tenant_id=tenant.id,
    )
    session.add(new_admin)
    try:
        session.commit()
    except Exception:
        session.rollback()
        session.delete(tenant)
        session.commit()
        raise HTTPException(400, "Failed to create tenant/admin (username or subdomain conflict)")

    return {"status": "success", "tenant_id": tenant.id}


@router.get("/api/admin/backup")
def create_system_backup(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    return export_tenant_snapshot(session, tenant_id)


@router.post("/api/admin/backups/create")
def create_database_backup_file(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    return create_backup_file(session, tenant_id=tenant_id)


@router.get("/api/admin/backups/list")
def list_database_backup_files(
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    return {"backups": list_local_backups(tenant_id=tenant_id)}


@router.get("/api/admin/backups/download/{filename}")
def download_database_backup_file(
    filename: str,
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    path = get_local_backup_path(filename, tenant_id=tenant_id)
    return FileResponse(path=path, media_type="application/gzip", filename=path.name)


@router.post("/api/admin/restore")
async def restore_system_backup(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    try:
        raw_content = await file.read()
        content = gzip.decompress(raw_content) if file.filename and file.filename.endswith(".gz") else raw_content
        data = json.loads(content)
        return restore_tenant_snapshot(session, tenant_id, data)
    except HTTPException:
        raise
    except Exception as exc:
        session.rollback()
        return {"error": f"Restore failed: {exc}"}


@router.get("/api/admin/reset-inventory-from-excel")
def reset_inventory_from_excel(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    file_path = "productos.xlsx"
    if not os.path.exists(file_path):
        return {"error": "File 'productos.xlsx' not found on server root"}

    try:
        session.exec(delete(Product).where(Product.tenant_id == tenant_id))
        df = pd.read_excel(file_path)
        added = 0
        errors = []

        def get_int(val, default=0):
            if pd.isna(val):
                return default
            try:
                return int(float(val))
            except Exception:
                return default

        def get_float(val, default=0.0):
            if pd.isna(val):
                return default
            try:
                return float(val)
            except Exception:
                return default

        for index, row in df.iterrows():
            try:
                name = str(row.get("Name", "")).strip()
                if not name or name.lower() == "nan" or pd.isna(name):
                    continue

                barcode = str(row.get("Barcode", "")).strip()
                if pd.isna(barcode) or barcode.lower() == "nan":
                    barcode = None

                should_generate = False
                if not barcode:
                    should_generate = True
                    barcode = f"TMP-{uuid.uuid4().hex[:8]}"

                price = get_float(row.get("Price"), 0.0)
                price_bulk = get_float(row.get("PriceBulk"), None) if not pd.isna(row.get("PriceBulk")) else None
                if price_bulk is None and price is not None:
                    price_bulk = price * 12

                prod = Product(
                    tenant_id=tenant_id,
                    name=name,
                    price=price,
                    stock_quantity=get_int(row.get("Stock"), 0),
                    barcode=barcode,
                    category=None if pd.isna(row.get("Category")) else str(row.get("Category")).strip(),
                    description=None if pd.isna(row.get("Description")) else str(row.get("Description")).strip(),
                    numeracion=None if pd.isna(row.get("Numeracion")) else str(row.get("Numeracion")).strip(),
                    cant_bulto=get_int(row.get("CantBulto"), None) if not pd.isna(row.get("CantBulto")) else None,
                    item_number=None if pd.isna(row.get("ItemNumber")) else str(row.get("ItemNumber")).strip(),
                    price_retail=get_float(row.get("PriceRetail"), None) if not pd.isna(row.get("PriceRetail")) else None,
                    price_bulk=price_bulk,
                )
                session.add(prod)
                if should_generate:
                    session.flush()
                    prod.barcode = str(prod.id).zfill(8)
                    session.add(prod)
                added += 1
            except Exception as exc:
                errors.append(f"Row {index}: {exc}")

        session.commit()
        return {"status": "success", "message": f"Inventory Reset. Added {added} products.", "errors": errors}
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


@router.get("/api/admin/reset-clients-from-excel")
def reset_clients_from_excel(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    SettingsService.ensure_admin(user)
    file_path = "clientes.xlsx"
    if not os.path.exists(file_path):
        return {"error": "File 'clientes.xlsx' not found on server root"}

    try:
        xls = pd.ExcelFile(file_path)
        added = 0
        updated = 0
        errors = []

        for sheet_name in xls.sheet_names:
            try:
                client_name = sheet_name.strip()
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                initial_debt = 0.0
                for debt_col in ("Restan", "Saldo"):
                    if debt_col in df.columns:
                        last_val = df[debt_col].iloc[-1]
                        initial_debt = float(last_val) if not pd.isna(last_val) else 0.0
                        break

                existing = session.exec(
                    select(Client).where(Client.name == client_name, Client.tenant_id == tenant_id)
                ).first()
                if existing:
                    updated += 1
                    client_id = existing.id
                else:
                    new_client = Client(name=client_name, tenant_id=tenant_id)
                    session.add(new_client)
                    session.commit()
                    session.refresh(new_client)
                    added += 1
                    client_id = new_client.id

                if initial_debt > 0:
                    has_sales = session.exec(
                        select(Sale).where(Sale.client_id == client_id, Sale.tenant_id == tenant_id)
                    ).first()
                    if not has_sales:
                        session.add(
                            Sale(
                                tenant_id=tenant_id,
                                client_id=client_id,
                                user_id=user.id,
                                total_amount=initial_debt,
                                amount_paid=0,
                                payment_status="pending",
                                payment_method="account",
                            )
                        )
                        session.commit()
            except Exception as exc:
                errors.append(f"Sheet {sheet_name}: {exc}")

        return {"status": "success", "added": added, "updated": updated, "sheets_processed": len(xls.sheet_names), "errors": errors}
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


@router.get("/api/products/export")
def export_products_api(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    products = session.exec(select(Product).where(Product.tenant_id == tenant_id)).all()
    data = [{
        "ID": p.id,
        "Name": p.name,
        "Category": p.category,
        "ItemNumber": p.item_number,
        "Barcode": p.barcode,
        "Price": p.price,
        "Stock": p.stock_quantity,
        "Description": p.description,
        "Numeracion": p.numeracion,
        "CantBulto": p.cant_bulto,
        "PriceBulk": p.price_bulk,
        "PriceRetail": p.price_retail,
    } for p in products]
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(data).to_excel(writer, index=False)
    output.seek(0)
    return StreamingResponse(
        output,
        headers={"Content-Disposition": 'attachment; filename="productos_export.xlsx"'},
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/api/clients/export")
def export_clients_api(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant),
):
    clients = session.exec(select(Client).where(Client.tenant_id == tenant_id)).all()
    data = [{
        "ID": c.id,
        "Name": c.name,
        "RazonSocial": c.razon_social,
        "CUIT": c.cuit,
        "Phone": c.phone,
        "Email": c.email,
        "Address": c.address,
        "IVACategory": c.iva_category,
        "CreditLimit": c.credit_limit,
        "TransportName": c.transport_name,
        "TransportAddress": c.transport_address,
    } for c in clients]
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(data).to_excel(writer, index=False)
    output.seek(0)
    return StreamingResponse(
        output,
        headers={"Content-Disposition": 'attachment; filename="clientes_export.xlsx"'},
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
