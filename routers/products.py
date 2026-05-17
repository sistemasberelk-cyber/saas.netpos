"""routers/products.py — Productos, Etiquetas, Importación"""
from __future__ import annotations
import io, os, re, shutil, uuid, json, logging
from typing import List, Optional
from io import BytesIO
import pandas as pd
import barcode
from barcode import Code128
from barcode.writer import ImageWriter
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from sqlmodel import Session, select, col
from database.models import Product, Settings, User
from database.session import get_session
from services.stock_service import StockService
from services.settings_service import SettingsService
from web.compat_templates import CompatTemplates
from web.dependencies import get_settings, get_tenant, require_auth

router = APIRouter(tags=["Products"])
logger = logging.getLogger(__name__)
stock_service = StockService(static_dir="static/barcodes")

def _templates():
    return CompatTemplates(directory="templates")

from web.pagination import paginate

# ---------- Pages ----------
@router.get("/products", response_class=HTMLResponse)
@router.head("/products")
def get_products_page(
    request: Request, 
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=100),
    user: User = Depends(require_auth), 
    settings: Settings = Depends(get_settings), 
    tenant_id: int = Depends(get_tenant), 
    session: Session = Depends(get_session)
):
    query = select(Product).where(Product.tenant_id == tenant_id, Product.is_deleted == False).order_by(Product.name)
    products_page = paginate(session, query, page=page, size=size)
    
    low_stock_products = []
    for p in session.exec(select(Product).where(Product.tenant_id == tenant_id, Product.is_deleted == False)).all():
        stock = stock_service.get_total_stock(session, p.id, tenant_id)
        if stock < p.min_stock_level:
            p.stock_quantity = stock
            low_stock_products.append(p)
    
    for p in products_page.items:
        p.stock_quantity = stock_service.get_total_stock(session, p.id, tenant_id)
    
    return _templates().TemplateResponse("products.html", {
        "request": request, 
        "active_page": "products", 
        "settings": settings, 
        "user": user, 
        "products": products_page.items,
        "total": products_page.total,
        "current_page": products_page.page,
        "total_pages": products_page.pages,
        "low_stock_products": low_stock_products
    })

@router.get("/products/labels", response_class=HTMLResponse)
def get_labels_page(request: Request, user: User = Depends(require_auth), settings: Settings = Depends(get_settings), tenant_id: int = Depends(get_tenant), session: Session = Depends(get_session)):
    products = session.exec(select(Product).where(Product.tenant_id == tenant_id)).all()
    return _templates().TemplateResponse("print_labels_selection.html", {"request": request, "active_page": "products", "settings": settings, "user": user, "products": products})

# ---------- CRUD API ----------
@router.get("/api/products")
def get_products_api(session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    products = session.exec(select(Product).where(Product.tenant_id == tenant_id)).all()
    res = []
    for p in products:
        d = p.dict()
        d["stock_quantity"] = stock_service.get_total_stock(session, p.id, tenant_id)
        res.append(d)
    return res

@router.post("/api/products")
def create_product_api(name: str = Form(...), price: float = Form(...), stock: int = Form(...), description: Optional[str] = Form(None), barcode_val: Optional[str] = Form(None, alias="barcode"), category: Optional[str] = Form(None), item_number: Optional[str] = Form(None), cant_bulto: Optional[int] = Form(None), numeracion: Optional[str] = Form(None), price_bulk: Optional[float] = Form(None), price_retail: Optional[float] = Form(None), image: Optional[UploadFile] = File(None), session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    product = Product(tenant_id=tenant_id, name=name, price=price, description=description, barcode=barcode_val or "", category=category, item_number=item_number, cant_bulto=cant_bulto, numeracion=numeracion, price_bulk=price_bulk, price_retail=price_retail)
    if image and image.filename:
        ext = image.filename.split(".")[-1]
        filename = f"{uuid.uuid4()}.{ext}"
        file_location = f"static/product_images/{filename}"
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(image.file, buffer)
        product.image_url = f"/{file_location}"
    session.add(product)
    session.commit()
    session.refresh(product)
    if not product.barcode:
        product.barcode = stock_service.generate_barcode(product.id)
        session.add(product)
        session.commit()
    if stock > 0:
        stock_service.add_stock(session, product.id, tenant_id, stock, "Ingreso inicial", None, user.id)
    return product

@router.put("/api/products/{id}")
def update_product_api(id: int, name: str = Form(...), price: float = Form(...), stock: int = Form(...), description: Optional[str] = Form(None), barcode_val: Optional[str] = Form(None, alias="barcode"), category: Optional[str] = Form(None), item_number: Optional[str] = Form(None), cant_bulto: Optional[int] = Form(None), numeracion: Optional[str] = Form(None), price_bulk: Optional[float] = Form(None), price_retail: Optional[float] = Form(None), image: Optional[UploadFile] = File(None), session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    product = session.get(Product, id)
    if not product or product.tenant_id != tenant_id: raise HTTPException(404, "Not found")
    product.name, product.price, product.description = name, price, description
    product.category, product.item_number, product.cant_bulto, product.numeracion = category, item_number, cant_bulto, numeracion
    product.price_bulk, product.price_retail = price_bulk, price_retail
    if barcode_val: product.barcode = barcode_val
    if image and image.filename:
        ext = image.filename.split(".")[-1]
        filename = f"{uuid.uuid4()}.{ext}"
        file_location = f"static/product_images/{filename}"
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(image.file, buffer)
        product.image_url = f"/{file_location}"
    session.add(product)
    session.commit()
    return product

@router.delete("/api/products/{id}")
def delete_product_api(id: int, session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    product = session.get(Product, id)
    if not product or product.tenant_id != tenant_id: raise HTTPException(404, "Not found")
    product.is_deleted = True
    session.add(product)
    session.commit()
    return {"ok": True}

# ---------- Bulk Price ----------
class BulkPriceUpdate(BaseModel):
    update_type: str
    percentage: float
    product_ids: Optional[List[int]] = None

@router.post("/api/products/bulk-update-price")
def bulk_update_price(data: BulkPriceUpdate, session: Session = Depends(get_session), user: User = Depends(require_auth), tenant_id: int = Depends(get_tenant)):
    SettingsService.ensure_admin(user)
    if data.update_type == "all":
        products = session.exec(select(Product).where(Product.tenant_id == tenant_id)).all()
    elif data.update_type == "list" and data.product_ids:
        products = session.exec(select(Product).where(Product.id.in_(data.product_ids), Product.tenant_id == tenant_id)).all()
    else:
        raise HTTPException(400, "Tipo inválido o sin productos")
    multiplier = 1 + (data.percentage / 100.0)
    count = 0
    for p in products:
        if p.price is not None:
            p.price = round(p.price * multiplier, 2)
            session.add(p)
            count += 1
    session.commit()
    return {"status": "success", "updated_count": count}

# ---------- Import / Export ----------
@router.post("/api/products/import")
async def import_products_excel(file: UploadFile = File(...), session: Session = Depends(get_session), tenant_id: int = Depends(get_tenant), user: User = Depends(require_auth)):
    SettingsService.ensure_admin(user)
    contents = await file.read()
    ext = file.filename.split(".")[-1].lower()
    df = pd.read_csv(io.BytesIO(contents)) if ext == "csv" else pd.read_excel(io.BytesIO(contents))
    df.columns = [c.lower().strip() for c in df.columns]
    processed, updated = 0, 0
    for _, row in df.iterrows():
        p_name = row.get("nombre") or row.get("name")
        if not p_name: continue
        p_barcode = str(row.get("codigo") or row.get("barcode") or "").strip()
        existing = None
        if p_barcode and p_barcode != "nan":
            existing = session.exec(select(Product).where(Product.barcode == p_barcode, Product.tenant_id == tenant_id)).first()
        if not existing:
            existing = session.exec(select(Product).where(Product.name == p_name, Product.tenant_id == tenant_id)).first()
        if existing:
            existing.price = float(row.get("precio") or row.get("price") or 0)
            existing.cost_price = float(row.get("costo") or row.get("cost") or 0)
            # existing.stock_quantity no se actualiza directo en WMS
            session.add(existing)
            updated += 1
        else:
            session.add(Product(tenant_id=tenant_id, name=str(p_name), price=float(row.get("precio") or row.get("price") or 0), cost_price=float(row.get("costo") or row.get("cost") or 0), barcode=p_barcode if p_barcode != "nan" else None))
            processed += 1
    session.commit()
    return {"status": "success", "created": processed, "updated": updated}

@router.get("/api/templates/download/{type}")
def download_import_template(type: str, user: User = Depends(require_auth)):
    SettingsService.ensure_admin(user)
    if type == "products":
        df = pd.DataFrame({"Name": ["Ej. Coca Cola"], "Price": [1500.0], "Stock": [100], "Barcode": ["7791234"], "Category": ["Bebidas"], "Description": [""], "CantBulto": [6], "Numeracion": [""], "ItemNumber": ["1001"], "PriceRetail": [1400.0], "PriceBulk": [1200.0]})
        filename = "template_productos.xlsx"
    elif type == "clients":
        df = pd.DataFrame({"Name": ["Juan Perez"], "Phone": ["1122334455"], "Email": ["juan@mail.com"], "Address": ["Calle 123"], "RazonSocial": [""], "CUIT": ["20-11223344-5"], "IVACategory": ["Resp. Inscripto"], "CreditLimit": [50000.0], "TransportName": [""], "TransportAddress": [""]})
        filename = "template_clientes.xlsx"
    else:
        raise HTTPException(400, "Invalid template type")
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return StreamingResponse(output, headers={'Content-Disposition': f'attachment; filename="{filename}"'}, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# ---------- Label Printing ----------
from services.label_service import LabelService

@router.post("/print/labels/generate")
def print_labels_v2(request: Request, selected_items: str = Form(...), layout_type: str = Form(...), hide_price: Optional[str] = Form(None), settings: Settings = Depends(get_settings), session: Session = Depends(get_session), tenant_id: int = Depends(get_tenant)):
    allowed = {"exhibition", "list", "100x50", "90x60", "100x60", "100x65", "55x44"}
    if layout_type not in allowed: raise HTTPException(400, "Invalid layout type")
    should_hide = hide_price and str(hide_price).lower() in ["true", "on", "1", "yes"]
    if len(selected_items) > 20_000: raise HTTPException(400, "Payload too large")
    try: item_ids = json.loads(selected_items)
    except Exception: raise HTTPException(400, "Invalid JSON")
    if not isinstance(item_ids, list): raise HTTPException(400, "Must be array")
    validated = list(dict.fromkeys(int(i) for i in item_ids if int(i) > 0))[:500]
    if not validated: raise HTTPException(400, "No products selected")
    
    labels_data = LabelService.prepare_labels_data(session, tenant_id, validated)
    
    if not labels_data: raise HTTPException(422, "No valid labels")
    tpl_map = {"exhibition": "print_layout_exhibition.html", "100x50": "labels_100x50.html", "100x60": "labels_100x60.html", "100x65": "labels_100x65.html", "90x60": "labels_90x60.html"}
    if layout_type in tpl_map:
        return _templates().TemplateResponse(tpl_map[layout_type], {"request": request, "labels": labels_data, "hide_price": should_hide})
    elif layout_type == "list":
        rows = "".join(f"<tr><td>{l['item_number'] or ''}</td><td>{l['name']}</td><td>{'$'+str(l['price']) if not should_hide else '-'}</td></tr>" for l in labels_data)
        return HTMLResponse(f"<html><body style='font-family:sans-serif'><h2>Lista de Precios</h2><table border=1 cellspacing=0 cellpadding=5 style='width:100%'><tr><th>Art #</th><th>Producto</th><th>Precio</th></tr>{rows}</table><script>window.print()</script></body></html>")
    else:
        return _templates().TemplateResponse("print_layout.html", {"request": request, "labels": labels_data, "w": 55 if layout_type == "55x44" else settings.label_width_mm, "h": 44 if layout_type == "55x44" else settings.label_height_mm, "hide_price": should_hide})
