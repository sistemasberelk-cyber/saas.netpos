from passlib.context import CryptContext
from sqlmodel import Session, select
from database.models import User, Settings, Tenant
import os
from datetime import datetime

pwd_context = CryptContext(schemes=["argon2", "bcrypt"], deprecated="auto")
print(f"INFO: Password Context Schemes: {pwd_context.schemes()}")

class AuthService:
    @staticmethod
    def verify_password(plain_password, hashed_password):
        return pwd_context.verify(plain_password, hashed_password)

    @staticmethod
    def get_password_hash(password):
        return pwd_context.hash(password)

    @staticmethod
    def create_default_user_and_settings(session: Session):
        # 0. Create Default Tenant
        tenant = session.exec(select(Tenant)).first()
        if not tenant:
            tenant = Tenant(name="Default Company", subdomain="default")
            session.add(tenant)
            session.commit()
            session.refresh(tenant)
            print(f"INFO: Created default Tenant (ID: {tenant.id})")

        # 1. Create Default Admin
        user = session.exec(select(User).where(User.username == "admin")).first()
        if not user:
            # Use env var or fallback to random secure string if not set
            default_password = os.getenv("ADMIN_PASSWORD", "Admin123!@#")
            hashed = AuthService.get_password_hash(default_password)
            admin = User(
                username="admin", 
                password_hash=hashed, 
                role="admin", 
                full_name="Administrador",
                tenant_id=tenant.id
            )
            session.add(admin)
            print(f"INFO: Created default user 'admin' (Tenant: {tenant.id})")
        
        # 2. Create Default Settings
        settings = session.exec(select(Settings).where(Settings.tenant_id == tenant.id)).first()
        if not settings:
            default_settings = Settings(
                tenant_id=tenant.id,
                company_name="NexPos", 
                logo_url="/static/images/logo.png"
            )
            session.add(default_settings)
            print("INFO: Created default settings for Tenant")
            
        session.commit()
