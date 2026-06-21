from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlmodel import Session, select, func
from database.session import get_session
from database.models import Sale, User
from web.dependencies import get_current_user_jwt
from services.stock_service import StockService
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

router = APIRouter()
stock_service = StockService()

class SaleItemRequest(BaseModel):
    product_id: int
    quantity: int
    price_type: Optional[str] = None

class SaleRequest(BaseModel):
    items: List[SaleItemRequest]
    client_id: Optional[int] = None
    amount_paid: Optional[float] = None
    payment_method: str = "cash"
    split_cash: Optional[float] = None
    split_transfer: Optional[float] = None

class SaleItemResponse(BaseModel):
    id: Optional[int]
    product_id: Optional[int]
    product_name: str
    quantity: int
    unit_price: float
    total: float
    cost_price_at_sale: float

class PaymentAllocationResponse(BaseModel):
    id: Optional[int]
    method: str
    amount: float

class SaleResponse(BaseModel):
    id: int
    tenant_id: int
    timestamp: datetime
    total_amount: float
    amount_paid: float
    payment_status: str
    payment_method: str
    user_id: int
    client_id: Optional[int]
    items: List[SaleItemResponse]
    payment_allocations: List[PaymentAllocationResponse]

class PaginatedSales(BaseModel):
    items: List[SaleResponse]
    total: int
    page: int
    pages: int

@router.get("/sales", response_model=PaginatedSales)
def get_sales(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    query = select(Sale).where(Sale.tenant_id == user.tenant_id)
    total = session.exec(select(func.count()).select_from(query.subquery())).one()
    offset = (page - 1) * limit
    items = session.exec(query.order_by(Sale.timestamp.desc()).offset(offset).limit(limit)).all()
    pages = (total + limit - 1) // limit
    return {
        "items": items,
        "total": total,
        "page": page,
        "pages": pages
    }

@router.get("/sales/{id}", response_model=SaleResponse)
def get_sale(
    id: int,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    sale = session.get(Sale, id)
    if not sale or sale.tenant_id != user.tenant_id:
        raise HTTPException(404, "Venta no encontrada")
    return sale

@router.post("/sales", response_model=SaleResponse)
def create_sale(
    data: SaleRequest,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    items_data = [item.model_dump() for item in data.items]
    try:
        sale = stock_service.process_sale(
            session=session,
            user_id=user.id,
            tenant_id=user.tenant_id,
            items_data=items_data,
            payment_method=data.payment_method,
            client_id=data.client_id,
            amount_paid=data.amount_paid,
            split_cash=data.split_cash,
            split_transfer=data.split_transfer
        )
        return sale
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
