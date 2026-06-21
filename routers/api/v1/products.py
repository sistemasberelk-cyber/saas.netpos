from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlmodel import Session, select, func
from database.session import get_session
from database.models import Product, User
from web.dependencies import get_current_user_jwt
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

router = APIRouter()

class ProductCreate(BaseModel):
    name: str
    description: Optional[str] = None
    barcode: str
    price: float = 0.0
    price_bulk: Optional[float] = None
    price_retail: Optional[float] = None
    cost_price: float = 0.0
    min_stock_level: int = 5
    category: Optional[str] = None
    item_number: Optional[str] = None
    image_url: Optional[str] = None
    cant_bulto: Optional[int] = None
    numeracion: Optional[str] = None
    curve_quantity: int = 1

class ProductResponse(BaseModel):
    id: int
    tenant_id: int
    name: str
    description: Optional[str]
    barcode: str
    price: float
    price_bulk: Optional[float]
    price_retail: Optional[float]
    cost_price: float
    min_stock_level: int
    category: Optional[str]
    item_number: Optional[str]
    image_url: Optional[str]
    cant_bulto: Optional[int]
    numeracion: Optional[str]
    curve_quantity: int
    stock_quantity: int
    is_deleted: bool

class PaginatedProducts(BaseModel):
    items: List[ProductResponse]
    total: int
    page: int
    pages: int

@router.get("/products", response_model=PaginatedProducts)
def get_products(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    search: Optional[str] = Query(None),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    query = select(Product).where(Product.tenant_id == user.tenant_id, Product.is_deleted == False)
    if search:
        query = query.where(
            Product.name.like(f"%{search}%") | 
            Product.barcode.like(f"%{search}%") | 
            Product.item_number.like(f"%{search}%")
        )
    
    total = session.exec(select(func.count()).select_from(query.subquery())).one()
    offset = (page - 1) * limit
    items = session.exec(query.order_by(Product.name).offset(offset).limit(limit)).all()
    
    pages = (total + limit - 1) // limit
    return {
        "items": items,
        "total": total,
        "page": page,
        "pages": pages
    }

@router.get("/products/{id}", response_model=ProductResponse)
def get_product(
    id: int,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    product = session.get(Product, id)
    if not product or product.tenant_id != user.tenant_id or product.is_deleted:
        raise HTTPException(404, "Producto no encontrado")
    return product

@router.post("/products", response_model=ProductResponse)
def create_product(
    data: ProductCreate,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    if data.barcode:
        existing = session.exec(
            select(Product).where(
                Product.tenant_id == user.tenant_id, 
                Product.barcode == data.barcode,
                Product.is_deleted == False
            )
        ).first()
        if existing:
            raise HTTPException(400, "El código de barra ya está en uso")
            
    product = Product(tenant_id=user.tenant_id, **data.model_dump())
    session.add(product)
    session.commit()
    session.refresh(product)
    return product

@router.put("/products/{id}", response_model=ProductResponse)
def update_product(
    id: int,
    data: ProductCreate,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    product = session.get(Product, id)
    if not product or product.tenant_id != user.tenant_id or product.is_deleted:
        raise HTTPException(404, "Producto no encontrado")
        
    if data.barcode and data.barcode != product.barcode:
        existing = session.exec(
            select(Product).where(
                Product.tenant_id == user.tenant_id, 
                Product.barcode == data.barcode,
                Product.is_deleted == False
            )
        ).first()
        if existing:
            raise HTTPException(400, "El código de barra ya está en uso")
            
    for k, v in data.model_dump().items():
        setattr(product, k, v)
        
    session.add(product)
    session.commit()
    session.refresh(product)
    return product

@router.delete("/products/{id}")
def delete_product(
    id: int,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    product = session.get(Product, id)
    if not product or product.tenant_id != user.tenant_id or product.is_deleted:
        raise HTTPException(404, "Producto no encontrado")
        
    product.is_deleted = True
    product.deleted_at = datetime.now()
    session.add(product)
    session.commit()
    return {"message": "Producto eliminado correctamente"}
