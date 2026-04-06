"""
WMS Router — Depósitos y Ubicaciones
=====================================
Endpoints MVP para gestión de depósitos físicos, ubicaciones (bins)
y movimientos de stock entre posiciones.
"""

from fastapi import APIRouter, Depends, HTTPException, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select, func
from sqlalchemy import text
from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel

from database.session import get_session
from database.models import (
    Location, Bin, BinStock, StockMovement, Product, User, Settings, Tenant
)
from web.dependencies import require_auth, get_settings, get_tenant

router = APIRouter(prefix="/wms", tags=["WMS"])
templates = Jinja2Templates(directory="templates")


# ============================================================
# API: DEPÓSITOS (Locations)
# ============================================================

@router.get("/api/locations")
def list_locations(
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """Lista todos los depósitos activos del tenant."""
    locations = session.exec(
        select(Location)
        .where(Location.tenant_id == tenant_id, Location.is_active == True)
        .order_by(Location.name)
    ).all()
    return locations


@router.post("/api/locations")
def create_location(
    name: str = Form(...),
    code: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """Crea un nuevo depósito."""
    # Validar código único si se provee
    if code:
        existing = session.exec(
            select(Location).where(Location.tenant_id == tenant_id, Location.code == code)
        ).first()
        if existing:
            raise HTTPException(400, f"Ya existe un depósito con el código '{code}'")

    location = Location(
        tenant_id=tenant_id,
        name=name,
        code=code,
        address=address,
        description=description,
    )
    session.add(location)
    session.commit()
    session.refresh(location)
    return location


@router.put("/api/locations/{location_id}")
def update_location(
    location_id: int,
    name: str = Form(...),
    code: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    is_active: bool = Form(True),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    loc = session.get(Location, location_id)
    if not loc or loc.tenant_id != tenant_id:
        raise HTTPException(404, "Depósito no encontrado")
    loc.name = name
    loc.code = code
    loc.address = address
    loc.description = description
    loc.is_active = is_active
    session.add(loc)
    session.commit()
    return loc


@router.delete("/api/locations/{location_id}")
def delete_location(
    location_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    loc = session.get(Location, location_id)
    if not loc or loc.tenant_id != tenant_id:
        raise HTTPException(404, "Depósito no encontrado")
    # Soft delete
    loc.is_active = False
    session.add(loc)
    session.commit()
    return {"ok": True}


# ============================================================
# API: UBICACIONES / BINS
# ============================================================

@router.get("/api/locations/{location_id}/bins")
def list_bins(
    location_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """Lista todas las ubicaciones de un depósito."""
    loc = session.get(Location, location_id)
    if not loc or loc.tenant_id != tenant_id:
        raise HTTPException(404, "Depósito no encontrado")

    bins = session.exec(
        select(Bin)
        .where(Bin.location_id == location_id, Bin.tenant_id == tenant_id, Bin.is_active == True)
        .order_by(Bin.name)
    ).all()
    return bins


@router.post("/api/locations/{location_id}/bins")
def create_bin(
    location_id: int,
    name: str = Form(...),
    aisle: Optional[str] = Form(None),
    shelf: Optional[str] = Form(None),
    position: Optional[str] = Form(None),
    max_capacity: Optional[int] = Form(None),
    description: Optional[str] = Form(None),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """Crea una nueva ubicación dentro de un depósito."""
    loc = session.get(Location, location_id)
    if not loc or loc.tenant_id != tenant_id:
        raise HTTPException(404, "Depósito no encontrado")

    # Nombre único dentro del depósito por tenant
    existing = session.exec(
        select(Bin).where(
            Bin.tenant_id == tenant_id,
            Bin.location_id == location_id,
            Bin.name == name
        )
    ).first()
    if existing:
        raise HTTPException(400, f"Ya existe una ubicación '{name}' en este depósito")

    bin_ = Bin(
        tenant_id=tenant_id,
        location_id=location_id,
        name=name,
        aisle=aisle,
        shelf=shelf,
        position=position,
        max_capacity=max_capacity,
        description=description,
    )
    session.add(bin_)
    session.commit()
    session.refresh(bin_)
    return bin_


@router.delete("/api/bins/{bin_id}")
def delete_bin(
    bin_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    bin_ = session.get(Bin, bin_id)
    if not bin_ or bin_.tenant_id != tenant_id:
        raise HTTPException(404, "Ubicación no encontrada")
    bin_.is_active = False
    session.add(bin_)
    session.commit()
    return {"ok": True}


# ============================================================
# API: STOCK POR UBICACIÓN
# ============================================================

@router.get("/api/bins/{bin_id}/stock")
def get_bin_stock(
    bin_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """Stock detallado de una ubicación específica."""
    bin_ = session.get(Bin, bin_id)
    if not bin_ or bin_.tenant_id != tenant_id:
        raise HTTPException(404, "Ubicación no encontrada")

    entries = session.exec(
        select(BinStock, Product)
        .join(Product, BinStock.product_id == Product.id)
        .where(BinStock.bin_id == bin_id, BinStock.tenant_id == tenant_id)
    ).all()

    return [
        {
            "bin_stock_id": bs.id,
            "product_id": p.id,
            "product_name": p.name,
            "barcode": p.barcode,
            "item_number": p.item_number,
            "quantity": bs.quantity,
            "updated_at": bs.updated_at,
        }
        for bs, p in entries
    ]


class StockAdjustRequest(BaseModel):
    product_id: int
    quantity: int          # Cantidad FINAL deseada (no delta)
    reason: Optional[str] = "ajuste"
    notes: Optional[str] = None


@router.post("/api/bins/{bin_id}/stock/adjust")
def adjust_bin_stock(
    bin_id: int,
    body: StockAdjustRequest,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """
    Ajuste manual de stock en una ubicación.
    Sincroniza product.stock_quantity en la misma transacción.
    """
    bin_ = session.get(Bin, bin_id)
    if not bin_ or bin_.tenant_id != tenant_id:
        raise HTTPException(404, "Ubicación no encontrada")

    if body.quantity < 0:
        raise HTTPException(400, "La cantidad no puede ser negativa")

    # Verificar capacidad máxima
    if bin_.max_capacity is not None and body.quantity > bin_.max_capacity:
        raise HTTPException(400, f"Excede la capacidad máxima de esta ubicación ({bin_.max_capacity})")

    product = session.get(Product, body.product_id)
    if not product or product.tenant_id != tenant_id:
        raise HTTPException(404, "Producto no encontrado")

    # Buscar o crear fila en BinStock
    bin_stock = session.exec(
        select(BinStock).where(
            BinStock.bin_id == bin_id,
            BinStock.product_id == body.product_id
        )
    ).first()

    old_qty = bin_stock.quantity if bin_stock else 0
    delta = body.quantity - old_qty

    if bin_stock:
        bin_stock.quantity = body.quantity
        bin_stock.updated_at = datetime.now(timezone.utc)
    else:
        bin_stock = BinStock(
            tenant_id=tenant_id,
            bin_id=bin_id,
            product_id=body.product_id,
            quantity=body.quantity
        )
    session.add(bin_stock)

    # Registrar movimiento de auditoría (solo si hay cambio real)
    if delta != 0:
        movement = StockMovement(
            tenant_id=tenant_id,
            product_id=body.product_id,
            from_bin_id=None if delta > 0 else bin_id,
            to_bin_id=bin_id if delta > 0 else None,
            quantity=abs(delta),
            reason=body.reason,
            notes=body.notes,
            user_id=user.id
        )
        session.add(movement)

        # Sincronizar stock global del producto
        product.stock_quantity = max(0, product.stock_quantity + delta)
        session.add(product)

    session.commit()
    return {"ok": True, "bin_id": bin_id, "product_id": body.product_id, "new_quantity": body.quantity, "delta": delta}


class TransferRequest(BaseModel):
    product_id: int
    from_bin_id: int
    to_bin_id: int
    quantity: int
    notes: Optional[str] = None
    request_id: Optional[str] = None   # Para idempotencia


@router.post("/api/bins/transfer")
def transfer_stock(
    body: TransferRequest,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """
    Transfiere stock entre dos ubicaciones (mismo o distinto depósito).
    Usa lock pesimista para evitar condiciones de carrera.
    """
    if body.quantity <= 0:
        raise HTTPException(400, "La cantidad a transferir debe ser mayor a 0")
    if body.from_bin_id == body.to_bin_id:
        raise HTTPException(400, "Origen y destino no pueden ser iguales")

    # Idempotencia: si ya se procesó este request_id, devolver OK
    if body.request_id:
        existing = session.exec(
            select(StockMovement).where(StockMovement.request_id == body.request_id)
        ).first()
        if existing:
            return {"ok": True, "idempotent": True, "movement_id": existing.id}

    # Validar bins pertenecen al tenant
    from_bin = session.get(Bin, body.from_bin_id)
    to_bin = session.get(Bin, body.to_bin_id)

    if not from_bin or from_bin.tenant_id != tenant_id:
        raise HTTPException(404, "Ubicación origen no encontrada")
    if not to_bin or to_bin.tenant_id != tenant_id:
        raise HTTPException(404, "Ubicación destino no encontrada")

    # Lock pesimista sobre las filas de BinStock (SELECT FOR UPDATE)
    from_stock = session.exec(
        select(BinStock).where(
            BinStock.bin_id == body.from_bin_id,
            BinStock.product_id == body.product_id
        ).with_for_update()
    ).first()

    if not from_stock or from_stock.quantity < body.quantity:
        available = from_stock.quantity if from_stock else 0
        raise HTTPException(400, f"Stock insuficiente en origen. Disponible: {available}")

    # Verificar capacidad destino
    to_stock = session.exec(
        select(BinStock).where(
            BinStock.bin_id == body.to_bin_id,
            BinStock.product_id == body.product_id
        ).with_for_update()
    ).first()

    to_current = to_stock.quantity if to_stock else 0
    if to_bin.max_capacity is not None and (to_current + body.quantity) > to_bin.max_capacity:
        raise HTTPException(400, f"Excede la capacidad máxima del destino ({to_bin.max_capacity})")

    # Ejecutar la transferencia atómica
    from_stock.quantity -= body.quantity
    from_stock.updated_at = datetime.now(timezone.utc)
    session.add(from_stock)

    if to_stock:
        to_stock.quantity += body.quantity
        to_stock.updated_at = datetime.now(timezone.utc)
        session.add(to_stock)
    else:
        to_stock = BinStock(
            tenant_id=tenant_id,
            bin_id=body.to_bin_id,
            product_id=body.product_id,
            quantity=body.quantity
        )
        session.add(to_stock)

    # Registrar movimiento
    movement = StockMovement(
        tenant_id=tenant_id,
        product_id=body.product_id,
        from_bin_id=body.from_bin_id,
        to_bin_id=body.to_bin_id,
        quantity=body.quantity,
        reason="transferencia",
        notes=body.notes,
        request_id=body.request_id,
        user_id=user.id
    )
    session.add(movement)
    session.commit()

    return {
        "ok": True,
        "movement_id": movement.id,
        "from_bin": body.from_bin_id,
        "to_bin": body.to_bin_id,
        "quantity": body.quantity
    }


# ============================================================
# API: REPORTES
# ============================================================

@router.get("/api/products/{product_id}/locations")
def get_product_locations(
    product_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """¿Dónde está este producto en el depósito?"""
    product = session.get(Product, product_id)
    if not product or product.tenant_id != tenant_id:
        raise HTTPException(404, "Producto no encontrado")

    entries = session.exec(
        select(BinStock, Bin, Location)
        .join(Bin, BinStock.bin_id == Bin.id)
        .join(Location, Bin.location_id == Location.id)
        .where(BinStock.product_id == product_id, BinStock.tenant_id == tenant_id, BinStock.quantity > 0)
    ).all()

    return [
        {
            "location_name": loc.name,
            "location_code": loc.code,
            "bin_name": b.name,
            "bin_id": b.id,
            "quantity": bs.quantity,
        }
        for bs, b, loc in entries
    ]


@router.get("/api/stock-map")
def get_stock_map(
    location_id: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    session: Session = Depends(get_session),
    user: User = Depends(require_auth),
    tenant_id: int = Depends(get_tenant)
):
    """Mapa completo de stock por ubicación. Paginado para evitar payloads enormes."""
    query = (
        select(BinStock, Bin, Location, Product)
        .join(Bin, BinStock.bin_id == Bin.id)
        .join(Location, Bin.location_id == Location.id)
        .join(Product, BinStock.product_id == Product.id)
        .where(BinStock.tenant_id == tenant_id, BinStock.quantity > 0)
    )

    if location_id:
        query = query.where(Bin.location_id == location_id)

    query = query.order_by(Location.name, Bin.name, Product.name)

    # Paginación
    total = session.exec(
        select(func.count()).select_from(query.subquery())
    ).one()

    offset = (page - 1) * page_size
    results = session.exec(query.offset(offset).limit(page_size)).all()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": [
            {
                "location_id": loc.id,
                "location_name": loc.name,
                "bin_id": b.id,
                "bin_name": b.name,
                "product_id": p.id,
                "product_name": p.name,
                "barcode": p.barcode,
                "item_number": p.item_number,
                "quantity": bs.quantity,
                "max_capacity": b.max_capacity,
            }
            for bs, b, loc, p in results
        ]
    }


# ============================================================
# UI: PÁGINAS HTML
# ============================================================

@router.get("/depositos", response_class=HTMLResponse)
def wms_page(
    request: Request,
    user: User = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    tenant_id: int = Depends(get_tenant),
    session: Session = Depends(get_session)
):
    """Página principal de gestión de depósitos."""
    locations = session.exec(
        select(Location)
        .where(Location.tenant_id == tenant_id)
        .order_by(Location.name)
    ).all()

    # Para cada depósito: contar bins y stock total
    locations_data = []
    for loc in locations:
        bin_count = session.exec(
            select(func.count(Bin.id)).where(Bin.location_id == loc.id, Bin.is_active == True)
        ).one()
        total_stock = session.exec(
            select(func.sum(BinStock.quantity))
            .join(Bin, BinStock.bin_id == Bin.id)
            .where(Bin.location_id == loc.id, BinStock.tenant_id == tenant_id)
        ).one() or 0
        locations_data.append({
            "location": loc,
            "bin_count": bin_count,
            "total_stock": total_stock
        })

    return templates.TemplateResponse("wms_depositos.html", {
        "request": request,
        "active_page": "wms",
        "settings": settings,
        "user": user,
        "locations_data": locations_data,
    })


@router.get("/depositos/{location_id}", response_class=HTMLResponse)
def wms_location_detail(
    location_id: int,
    request: Request,
    user: User = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    tenant_id: int = Depends(get_tenant),
    session: Session = Depends(get_session)
):
    """Detalle de un depósito: todas sus ubicaciones y stock."""
    loc = session.get(Location, location_id)
    if not loc or loc.tenant_id != tenant_id:
        raise HTTPException(404, "Depósito no encontrado")

    bins = session.exec(
        select(Bin)
        .where(Bin.location_id == location_id, Bin.tenant_id == tenant_id)
        .order_by(Bin.name)
    ).all()

    # Stock por bin
    bins_data = []
    for b in bins:
        stock_entries = session.exec(
            select(BinStock, Product)
            .join(Product, BinStock.product_id == Product.id)
            .where(BinStock.bin_id == b.id)
        ).all()
        bins_data.append({
            "bin": b,
            "stock": [{"product": p, "quantity": bs.quantity} for bs, p in stock_entries],
            "total_units": sum(bs.quantity for bs, _ in stock_entries)
        })

    return templates.TemplateResponse("wms_location_detail.html", {
        "request": request,
        "active_page": "wms",
        "settings": settings,
        "user": user,
        "location": loc,
        "bins_data": bins_data,
    })
