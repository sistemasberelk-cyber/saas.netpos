"""NexPos Cloud SaaS — Main Application (Refactored)"""
from fastapi import FastAPI, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, select, func
from contextlib import asynccontextmanager
from datetime import datetime, date
import os
import logging

from database.session import create_db_and_tables, get_session, engine
from database.models import Product, Sale, Settings, User, Tenant
from database.seed_data import seed_products
from services.auth_service import AuthService
from web.dependencies import get_current_user, get_settings, get_tenant, require_auth
from web.compat_templates import CompatTemplates

# Routers
from routers.auth import router as auth_router
from routers.admin import router as admin_router
from routers.products import router as products_router
from routers.sales import router as sales_router
from routers.clients import router as clients_router
from routers.suppliers import router as suppliers_router
from routers.cash import router as cash_router
from routers.reports import router as reports_router
from routers.picking import router as picking_router
from routers.wms import router as wms_router

# API V1 Routers
from routers.api.v1.auth import router as auth_v1_router
from routers.api.v1.products import router as products_v1_router
from routers.api.v1.sales import router as sales_v1_router
from routers.api.v1.ui_config import router as ui_config_v1_router

from web.logging_config import setup_logging
setup_logging()
logger = logging.getLogger(__name__)

from sqlalchemy import text

def ensure_schema_compatibility(session: Session):
    """
    DEPRECATED: This block is deprecated. All future schema changes must
    go through Alembic migrations (alembic revision --autogenerate).
    Do not add new columns or modify constraints here.
    """
    stmts = [
        "ALTER TABLE bin ADD COLUMN IF NOT EXISTS max_capacity INTEGER",
        "ALTER TABLE bin ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE bin ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE binstock ADD COLUMN IF NOT EXISTS tenant_id INTEGER",
        "ALTER TABLE binstock ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP",
        "ALTER TABLE stockmovement ADD COLUMN IF NOT EXISTS tenant_id INTEGER",
        "ALTER TABLE stockmovement ADD COLUMN IF NOT EXISTS request_id VARCHAR",
        "ALTER TABLE stockmovement ADD COLUMN IF NOT EXISTS user_id INTEGER",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS reference_id INTEGER",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS reference_type VARCHAR",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS user_id INTEGER",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS sale_id INTEGER",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS purchase_id INTEGER",
        "ALTER TABLE payment ADD COLUMN IF NOT EXISTS receivable_id INTEGER",
        "ALTER TABLE payment ADD COLUMN IF NOT EXISTS method VARCHAR DEFAULT 'cash'",
        # "ALTER TABLE sale ADD COLUMN IF NOT EXISTS amount_cash FLOAT DEFAULT 0.0",
        # "ALTER TABLE sale ADD COLUMN IF NOT EXISTS amount_transfer FLOAT DEFAULT 0.0",
        # "ALTER TABLE sale ADD COLUMN IF NOT EXISTS payment_method VARCHAR DEFAULT 'cash'",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS price_bulk FLOAT",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS price_retail FLOAT",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS cant_bulto INTEGER",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS numeracion VARCHAR",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS curve_quantity INTEGER DEFAULT 1",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS item_number VARCHAR",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS image_url VARCHAR",
        "ALTER TABLE supplier ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE supplier ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE purchase ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE purchase ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE location ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE location ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS credit_limit FLOAT",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS razon_social VARCHAR",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS cuit VARCHAR",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS iva_category VARCHAR",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS transport_name VARCHAR",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS transport_address VARCHAR",
        "ALTER TABLE settings ADD COLUMN IF NOT EXISTS ui_theme VARCHAR DEFAULT 'default'",
        "ALTER TABLE aicredential ADD COLUMN IF NOT EXISTS api_key_enc VARCHAR",
        "ALTER TABLE businessconfig ADD COLUMN IF NOT EXISTS openai_api_key_enc VARCHAR",
        "ALTER TABLE businessconfig ADD COLUMN IF NOT EXISTS deepseek_api_key_enc VARCHAR",
        "ALTER TABLE businessconfig ADD COLUMN IF NOT EXISTS elevenlabs_api_key_enc VARCHAR",
    ]
    for stmt in stmts:
        try:
            session.exec(text(stmt))
            session.commit()
        except Exception as e:
            session.rollback()
            err_msg = str(e).lower()
            is_duplicate = "duplicate" in err_msg or "already exists" in err_msg
            if not is_duplicate:
                logger.warning(f"Schema compatibility check warning for stmt '{stmt}': {e}")

templates = CompatTemplates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Run migrations on startup
    try:
        from alembic import command
        from alembic.config import Config
        alembic_cfg = Config("alembic.ini")
        # Ensure url is set from env
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            alembic_cfg.set_main_option("sqlalchemy.url", db_url)
        command.upgrade(alembic_cfg, "head")
        logger.info("Alembic migrations completed successfully.")
    except Exception as e:
        logger.error(f"Alembic migration failed: {e}")
        # Fallback to simple table creation if alembic fails
        create_db_and_tables()

    with Session(engine) as session:
        ensure_schema_compatibility(session)
        try:
            AuthService.create_default_user_and_settings(session)
        except Exception:
            session.rollback()
            raise
        if os.getenv("SEED_ON_START") == "1":
            seed_products(session)

    # Start background daily theme scheduler
    from web.scheduler import start_theme_scheduler
    start_theme_scheduler()

    yield


app = FastAPI(title="NexPos Cloud", lifespan=lifespan)

# CORS
def _get_cors_origins() -> list[str]:
    raw = os.getenv("CORS_ORIGINS", "http://localhost,http://127.0.0.1")
    return [o.strip() for o in raw.split(",") if o.strip()] or ["http://localhost"]

app.add_middleware(CORSMiddleware, allow_origins=_get_cors_origins(), allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

from starlette.middleware.sessions import SessionMiddleware
SESSION_SECRET = os.getenv("SECRET_KEY")
if not SESSION_SECRET:
    raise RuntimeError("SECRET_KEY env var is required.")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax")

app.mount("/static", StaticFiles(directory="static"), name="static")

# Register all routers
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(products_router)
app.include_router(sales_router)
app.include_router(clients_router)
app.include_router(suppliers_router)
app.include_router(cash_router)
app.include_router(reports_router)
app.include_router(picking_router)
app.include_router(wms_router)

# Register API V1 Routers
app.include_router(auth_v1_router, prefix="/api/v1", tags=["Auth V1"])
app.include_router(products_v1_router, prefix="/api/v1", tags=["Products V1"])
app.include_router(sales_v1_router, prefix="/api/v1", tags=["Sales V1"])
app.include_router(ui_config_v1_router, prefix="/api/v1", tags=["UI Config V1"])


@app.get("/health")
@app.head("/health")
def health_check():
    return {"status": "ok"}


@app.get("/fix-db")
def fix_db(session: Session = Depends(get_session), user: User = Depends(require_auth)):
    if user.role != "superadmin":
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Forbidden: Requires superadmin role.")
    results = []
    stmts = [
        "ALTER TABLE bin ADD COLUMN IF NOT EXISTS max_capacity INTEGER",
        "ALTER TABLE bin ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE bin ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE binstock ADD COLUMN IF NOT EXISTS tenant_id INTEGER",
        "ALTER TABLE binstock ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP",
        "ALTER TABLE stockmovement ADD COLUMN IF NOT EXISTS tenant_id INTEGER",
        "ALTER TABLE stockmovement ADD COLUMN IF NOT EXISTS request_id VARCHAR",
        "ALTER TABLE stockmovement ADD COLUMN IF NOT EXISTS user_id INTEGER",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS reference_id INTEGER",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS reference_type VARCHAR",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS user_id INTEGER",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS sale_id INTEGER",
        "ALTER TABLE cashmovement ADD COLUMN IF NOT EXISTS purchase_id INTEGER",
        "ALTER TABLE payment ADD COLUMN IF NOT EXISTS receivable_id INTEGER",
        "ALTER TABLE payment ADD COLUMN IF NOT EXISTS method VARCHAR DEFAULT 'cash'",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS price_bulk FLOAT",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS price_retail FLOAT",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS cant_bulto INTEGER",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS numeracion VARCHAR",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS curve_quantity INTEGER DEFAULT 1",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS item_number VARCHAR",
        "ALTER TABLE product ADD COLUMN IF NOT EXISTS image_url VARCHAR",
        "ALTER TABLE supplier ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE supplier ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE \"user\" ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE purchase ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE purchase ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE location ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE location ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS credit_limit FLOAT",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS razon_social VARCHAR",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS cuit VARCHAR",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS iva_category VARCHAR",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS transport_name VARCHAR",
        "ALTER TABLE client ADD COLUMN IF NOT EXISTS transport_address VARCHAR",
        "ALTER TABLE settings ADD COLUMN IF NOT EXISTS ui_theme VARCHAR DEFAULT 'default'",
        "ALTER TABLE aicredential ADD COLUMN IF NOT EXISTS api_key_enc VARCHAR",
        "ALTER TABLE businessconfig ADD COLUMN IF NOT EXISTS openai_api_key_enc VARCHAR",
        "ALTER TABLE businessconfig ADD COLUMN IF NOT EXISTS deepseek_api_key_enc VARCHAR",
        "ALTER TABLE businessconfig ADD COLUMN IF NOT EXISTS elevenlabs_api_key_enc VARCHAR",
    ]
    for stmt in stmts:
        try:
            session.exec(text(stmt))
            session.commit()
            results.append({"stmt": stmt, "status": "success"})
        except Exception as e:
            session.rollback()
            results.append({"stmt": stmt, "status": "error", "message": str(e)})
            
    # Try Alembic manually
    try:
        from alembic import command
        from alembic.config import Config
        alembic_cfg = Config("alembic.ini")
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            alembic_cfg.set_main_option("sqlalchemy.url", db_url)
        command.upgrade(alembic_cfg, "head")
        results.append({"stmt": "alembic upgrade head", "status": "success"})
    except Exception as e:
        results.append({"stmt": "alembic upgrade head", "status": "error", "message": str(e)})
        
    return {"results": results}


@app.get("/", response_class=HTMLResponse)
@app.head("/")
def get_dashboard(request: Request, user: User = Depends(require_auth), settings: Settings = Depends(get_settings), tenant_id: int = Depends(get_tenant), session: Session = Depends(get_session)):
    if user.role == "superadmin":
        return RedirectResponse("/tenants", status_code=302)
    total_products = session.exec(select(func.count(Product.id)).where(Product.tenant_id == tenant_id)).one()
    from database.models import BinStock
    from sqlalchemy.sql.functions import coalesce

    subquery = (
        select(Product.id)
        .join(BinStock, BinStock.product_id == Product.id, isouter=True)
        .where(Product.tenant_id == tenant_id, Product.is_deleted == False)
        .group_by(Product.id, Product.min_stock_level)
        .having(coalesce(func.sum(BinStock.quantity), 0) < Product.min_stock_level)
        .subquery()
    )
    low_stock = session.exec(select(func.count()).select_from(subquery)).one()
            
    recent_sales = session.exec(select(Sale).where(Sale.tenant_id == tenant_id, Sale.is_closed == False).order_by(Sale.timestamp.desc()).limit(5)).all()
    today_start = datetime.combine(date.today(), datetime.min.time())
    today_sales_total = session.exec(select(func.sum(Sale.total_amount)).where(Sale.tenant_id == tenant_id, Sale.timestamp >= today_start, Sale.is_closed == False)).one() or 0.0
    return templates.TemplateResponse("dashboard.html", {"request": request, "active_page": "home", "settings": settings, "user": user, "total_products": total_products, "low_stock": low_stock, "recent_sales": recent_sales, "today_sales_total": today_sales_total})


@app.get("/pos", response_class=HTMLResponse)
def get_pos(request: Request, user: User = Depends(require_auth), settings: Settings = Depends(get_settings)):
    return templates.TemplateResponse("pos.html", {"request": request, "active_page": "pos", "settings": settings, "user": user})
