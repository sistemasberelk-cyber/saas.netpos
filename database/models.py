from typing import Optional, List
from datetime import datetime, timezone
from sqlmodel import Field, SQLModel, Relationship

# --- Tenant Model (Multi-Tenancy) ---
class Tenant(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    subdomain: Optional[str] = Field(default=None, unique=True, index=True) # For SaaS URL routing
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Relations
    users: List["User"] = Relationship(back_populates="tenant")
    settings: List["Settings"] = Relationship(back_populates="tenant")

# --- Settings Model ---
class Settings(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tenant_id: Optional[int] = Field(default=None, foreign_key="tenant.id")
    tenant: Optional[Tenant] = Relationship(back_populates="settings")
    
    company_name: str = Field(default="Berel K")
    logo_url: str = Field(default="/static/images/berelk_logo.png")
    tax_rate: Optional[float] = Field(default=0.0)
    printer_name: Optional[str] = Field(default=None)
    label_width_mm: int = Field(default=60)
    label_height_mm: int = Field(default=40)

# --- Tax Model ---
class Tax(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    rate: float # 0.21 for 21%
    is_active: bool = Field(default=True)

# --- Client Model ---
class Client(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tenant_id: Optional[int] = Field(default=None, foreign_key="tenant.id")
    
    name: str = Field(index=True)
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    notes: Optional[str] = None
    credit_limit: Optional[float] = Field(default=None)
    
    # New Fields
    razon_social: Optional[str] = None
    cuit: Optional[str] = None
    iva_category: Optional[str] = None # Resp Inscripto, Monotributo, etc
    transport_name: Optional[str] = None
    transport_address: Optional[str] = None
    
    sales: List["Sale"] = Relationship(back_populates="client")
    payments: List["Payment"] = Relationship(back_populates="client")

# --- User Model ---
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tenant_id: Optional[int] = Field(default=None, foreign_key="tenant.id")
    tenant: Optional[Tenant] = Relationship(back_populates="users")
    
    username: str = Field(index=True, unique=True)
    password_hash: str  # We will store bcrypt hash, not plain text
    full_name: Optional[str] = None
    role: str = Field(default="admin")  # admin, cashier
    is_active: bool = Field(default=True)
    
    sales: List["Sale"] = Relationship(back_populates="user")

# --- Product Model ---
class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tenant_id: Optional[int] = Field(default=None, foreign_key="tenant.id")
    
    name: str
    description: Optional[str] = None
    barcode: str = Field(unique=True, index=True) 
    price: float = Field(default=0.0) # Base Price (Unitario / Lista)
    price_bulk: Optional[float] = Field(default=None) # Precio por Bulto
    price_retail: Optional[float] = Field(default=None) # Precio Mayorista (User Defined)

    cost_price: float = Field(default=0.0) # For profit calculation
    stock_quantity: int = Field(default=0)
    min_stock_level: int = Field(default=5) # Alert level
    category: Optional[str] = None
    item_number: Optional[str] = Field(default=None, index=True) # Código de Articulo
    image_url: Optional[str] = None
    
    # New Fields
    cant_bulto: Optional[int] = Field(default=None) # Quantity per package/bulk
    numeracion: Optional[str] = None # Size/Numbering
    
    curve_quantity: int = Field(default=1) # Quantity in the curve/pack

# --- Sale Models (Header & Detail) ---
class Sale(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tenant_id: Optional[int] = Field(default=None, foreign_key="tenant.id")
    
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_amount: float = Field(default=0.0)
    payment_method: str = Field(default="cash") # cash, card, transfer
    amount_paid: float = Field(default=0.0)
    payment_status: str = Field(default="paid") # paid, partial, pending
    is_closed: bool = Field(default=False) # True if processed in Cierre de Caja
    
    # Foreign Keys
    user_id: Optional[int] = Field(default=None, foreign_key="user.id")
    user: Optional[User] = Relationship(back_populates="sales")
    
    client_id: Optional[int] = Field(default=None, foreign_key="client.id")
    client: Optional["Client"] = Relationship(back_populates="sales")
    
    items: List["SaleItem"] = Relationship(back_populates="sale")

class SaleItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    sale_id: Optional[int] = Field(default=None, foreign_key="sale.id")
    product_id: Optional[int] = Field(default=None, foreign_key="product.id")
    product: Optional["Product"] = Relationship(sa_relationship_kwargs={"lazy": "joined"})
    
    product_name: str # Snapshot in case product name changes
    quantity: int
    unit_price: float
    total: float
    
    sale: Optional[Sale] = Relationship(back_populates="items")

# --- Payment Model (Current Account) ---
class Payment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tenant_id: Optional[int] = Field(default=None, foreign_key="tenant.id")
    client_id: int = Field(foreign_key="client.id")
    amount: float
    date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    note: Optional[str] = None
    
    # Relationship
    client: Optional[Client] = Relationship(back_populates="payments")

# --- Business Config Model (For AI Services) ---
class BusinessConfig(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    business_name: str
    tier: str = Field(default="standard") # standard, premium
    
    # LLM Keys
    openai_api_key: Optional[str] = None
    deepseek_api_key: Optional[str] = None
    
    # Voice Keys
    elevenlabs_api_key: Optional[str] = None
    
    # Prompts
    system_prompt: Optional[str] = "Eres un asistente de ventas útil."
    voice_id: Optional[str] = None
    
    is_active: bool = Field(default=True)
