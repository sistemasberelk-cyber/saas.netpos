"""routers/sales.py — Ventas + Cierre de Caja"""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import HTMLResponse
from sqlmodel import Session, select
from database.models import CashMovement, Product, Sale, Settings, User
from database.session import get_session
from services.stock_service import StockService
from web.compat_templates import CompatTemplates
from web.dependencies import get_settings, get_tenant, require_auth

router = APIRouter(tags=["Sales"])
stock_service = StockService(static_dir="static/barcodes")

def _templates():
    return CompatTemplates(directory="templates")

from services.sale_service import SaleService
from services.cash_service import CashService

from web.pagination import paginate

@router.get("/sales", response_class=HTMLResponse)
def get_sales_page(
    request: Request, 
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=100),
    user: User = Depends(require_auth), 
    settings: Settings = Depends(get_settings), 
    tenant_id: int = Depends(get_tenant), 
    session: Session = Depends(get_session)
):
    query = select(Sale).where(Sale.tenant_id == tenant_id, Sale.is_closed == False).order_by(Sale.timestamp.desc())
    sales_page = paginate(session, query, page=page, size=size)
    
    low_stock_products = [p for p in session.exec(select(Product).where(Product.tenant_id == tenant_id, Product.is_deleted == False)).all() if stock_service.get_total_stock(session, p.id, tenant_id) < p.min_stock_level]
    
    return _templates().TemplateResponse("sales.html", {
        "request": request, 
        "active_page": "sales", 
        "settings": settings, 
        "user": user, 
        "sales": sales_page.items,
        "total": sales_page.total,
        "current_page": sales_page.page,
        "total_pages": sales_page.pages,
        "low_stock_products": low_stock_products, 
        "daily_reports": SaleService.build_daily_reports(session, tenant_id)
    })

@router.post("/sales/backup", response_class=HTMLResponse)
def trigger_backup(request: Request, user: User = Depends(require_auth), settings: Settings = Depends(get_settings), tenant_id: int = Depends(get_tenant), session: Session = Depends(get_session)):
    from services.backup_service import perform_backup
    from services.database_backup_service import create_backup_file
    try:
        create_backup_file(session, tenant_id=tenant_id)
    except Exception as e:
        print(f"ERROR: Failed JSON backup: {e}")
    
    # Perform external backup
    result = perform_backup(session, tenant_id=tenant_id)
    
    # Execute closing logic using service
    CashService.perform_cierre(session, tenant_id, user.id)
    
    sales = session.exec(select(Sale).where(Sale.tenant_id == tenant_id, Sale.is_closed == False).order_by(Sale.timestamp.desc())).all()
    low_stock_products = [p for p in session.exec(select(Product).where(Product.tenant_id == tenant_id, Product.is_deleted == False)).all() if stock_service.get_total_stock(session, p.id, tenant_id) < p.min_stock_level]
    status_msg = "success" if result["status"] == "success" else "error"
    msg_text = "✅ Backup exitoso y caja cerrada!" if result["status"] == "success" else f"❌ Error: {result['message']}"
    return _templates().TemplateResponse("sales.html", {"request": request, "active_page": "sales", "settings": settings, "user": user, "sales": sales, "low_stock_products": low_stock_products, "daily_reports": SaleService.build_daily_reports(session, tenant_id), "backup_status": status_msg, "backup_message": msg_text})

@router.post("/api/sales")
def create_sale_api(sale_data: dict, session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    try:
        return stock_service.process_sale(session, user_id=user.id, tenant_id=tenant_id, items_data=sale_data["items"], client_id=sale_data.get("client_id"), amount_paid=sale_data.get("amount_paid"), payment_method=sale_data.get("payment_method", "cash"), split_cash=sale_data.get("split_cash"), split_transfer=sale_data.get("split_transfer"))
    except ValueError as e:
        raise HTTPException(400, str(e))

@router.get("/sales/{id}/remito", response_class=HTMLResponse)
def get_sale_remito(id: int, request: Request, user: User = Depends(require_auth), settings: Settings = Depends(get_settings), tenant_id: int = Depends(get_tenant), session: Session = Depends(get_session)):
    sale = session.get(Sale, id)
    if not sale or sale.tenant_id != tenant_id: raise HTTPException(404, "Sale not found")
    return _templates().TemplateResponse("remito.html", {"request": request, "sale": sale, "settings": settings})
